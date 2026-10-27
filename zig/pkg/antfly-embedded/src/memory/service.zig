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

//! Native, transport-independent memory policy. Persistence is supplied by the
//! owning Antfly database. Every mutation fences the space control row and
//! atomically commits records, revision history, operation receipt and work.
const std = @import("std");
const screening = @import("secret_screening.zig");
pub const types = @import("types.zig");
pub const generation_policy = @import("generation.zig");
pub const operations = @import("operations.zig");
const t = types;
const prov = t.provenance;
const A = std.mem.Allocator;
const eql = std.mem.eql;
const curation = @import("antfly_resolver").curation;
const identity_records = @import("../storage/db/resolver_authority.zig");
const recall_filters = @import("recall_filters.zig");
const recall_windows = @import("recall_windows.zig");
const graph_recall = @import("graph_recall.zig");
const lexical = @import("lexical.zig");
const max_dependency_depth: u8 = 32;
const max_support_work = 65536;
const resource_check_interval_ms: i64 = 30000;
const max_explanation_snapshot_candidates = 4096;
// Increment when the private inference snapshot gains a required input class.
const judgment_input_version: u8 = 2;
const assertion_reference_fields = .{ "supersedes", "motivates", "rejected_in_favor_of" };
const ReasonBindings = struct { assertion: ?[]const u8 = null, disposition: ?[]const u8 = null };
const DecisionInput = struct { id: []const u8, revision: u64, reasons: ReasonBindings, correction_evidence: []const t.EvidenceRef = &.{} };
const DecisionProvenance = struct { input_version: u8 = 0, principal: []const u8, recorded_at: i64, config_generation: u64, assertion_id: []const u8, evidence_ids: []const []const u8, inputs: []const DecisionInput, context: ?[]const t.DerivedDependency = null, incoming_reason: ?[]const u8 = null };
// Causal inference inputs are conjunctive retention/authorization dependencies,
// independent of the alternative factual support selected by a producer.
const GenerationProvenance = struct {
    operation_id: []const u8,
    config_generation: u64,
    evidence_ids: []const []const u8,
    // Frozen, selected support closure of external judgment context. Reads do
    // not follow mutable assertion parents or substitute alternative support.
    context: []const t.DerivedDependency = &.{},
    views: []const t.EntityViewInput = &.{},
};
const JudgmentArtifact = struct { input_version: u8 = 0, operation: prov.Resource, inputs: GenerationProvenance, reason: []const u8, output_assertion_id: ?[]const u8 = null };
const ExplainedRecord = struct { resource: prov.Resource, record: std.json.Value, links: []const prov.Link };
const StoredOutcome = struct { outcome: t.Outcome, owner: []const u8, recorded_at: i64, revision: u64 = 0 };
const DependencyWork = struct { kind: []const u8, id: []const u8, revision: u64, cursor: ?[]const u8 = null };
const EntityPolicy = struct { state: curation.State = .{}, redirects: []const curation.Redirect = &.{} };
// Free-text audit content has its own retention lifecycle. Identity origins
// retain only attribution/revision, so forgetting can scrub content without
// discarding a surviving suppression, distinction or redirect decision.
const EntityCurationProvenance = struct {
    owner: []const u8,
    recorded_at: i64,
    revision: u64,
    entity_ids: []const []const u8,
    reason: []const u8,
    evidence: []const t.EvidenceRef = &.{},
    judgment: ?prov.Resource = null,
};

const ProposalBinding = t.EntityProposalInputs;
const ProposalAttempt = struct { proposal: curation.Proposal, inputs: ProposalBinding, previous: ?prov.Resource = null };
const EpisodeCatalog = struct { thread_id: []const u8, members: u64 = 0 };
const EpisodeListPosition = struct {
    after: ?[]const u8 = null,
    pending: ?[]const u8 = null,
};

pub const Row = struct { key: []const u8, json: []const u8, version: u64 = 0, digest: ?[32]u8 = null };
pub const Change = struct { key: []const u8, json: ?[]const u8 };
pub const Page = struct { rows: []const Row, next: ?[]const u8 = null };
/// Service-owned causal input fence. Multi-call generation must complete this
/// boundary after each provider call, after reacquiring any embedded lease and
/// before exposing either the inputs or derived output to another provider.
pub const InferenceBoundary = struct {
    ptr: *anyopaque,
    complete_fn: *const fn (*anyopaque) anyerror!void,
    pub fn complete(self: InferenceBoundary) !void {
        try self.complete_fn(self.ptr);
    }
};
pub const EmbeddingRole = enum { document, query };
pub const Neighbor = struct { key: []const u8, similarity: f64 };
pub const NeighborRequest = struct { index: []const u8, vector: []const f32, limit: u16 };
pub const RecallSearchRequest = struct {
    text_indexes: []const []const u8,
    vector_indexes: []const []const u8 = &.{},
    query: []const u8,
    filters: recall_filters.Filters = .{},
    embedding: ?[]const f32 = null,
    offset: u32 = 0,
    limit: u16 = 128,
};
pub const RecallSearchPage = struct { keys: []const []const u8, more: bool };
pub const Backend = struct {
    evaluate: ?*const fn (*anyopaque, A, t.Config, t.Model, t.evaluation.JudgeInput, InferenceBoundary) anyerror!t.evaluation.JudgeOutput = null,
    random_bytes: ?*const fn (*anyopaque, []u8) anyerror!void = null,
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, []const u8) anyerror!?Row,
    // Bounded data-only hydration; never used for control CAS tokens.
    get_many: ?*const fn (*anyopaque, A, []const []const u8) anyerror![]const ?Row = null,
    scan: *const fn (*anyopaque, A, []const u8, ?[]const u8, u32) anyerror!Page,
    commit: *const fn (*anyopaque, A, ?Row, []const Change) anyerror!void,
    // Model integration must use Antfly's admitted provider/runtime. No model
    // client or credentials are owned by this policy module.
    resource: ?*const fn (*anyopaque, A, prov.Resource) anyerror!prov.Snapshot = null,
    generate: ?*const fn (*anyopaque, A, t.Config, []const t.StoredEvidence, InferenceBoundary) anyerror!t.Generated = null,
    decide: ?*const fn (*anyopaque, A, t.Config, t.Assertion, []const t.StoredAssertion) anyerror!t.ReconciliationDecision = null,
    embed: ?*const fn (*anyopaque, A, t.Model, []const u8) anyerror![]const f32 = null,
    embed_batch: ?*const fn (*anyopaque, A, t.Model, []const []const u8, EmbeddingRole) anyerror![]const []const f32 = null,
    embed_document: ?*const fn (*anyopaque, A, t.Model, []const u8) anyerror![]const f32 = null,
    drop_index: ?*const fn (*anyopaque, A, []const u8) anyerror!void = null,
    ensure_text_index: ?*const fn (*anyopaque, A, []const u8) anyerror!void = null,
    search_assertions: ?*const fn (*anyopaque, A, RecallSearchRequest) anyerror!RecallSearchPage = null,
    ensure_vector_index: ?*const fn (*anyopaque, A, []const u8, u32) anyerror!void = null,
    neighbors: ?*const fn (*anyopaque, A, NeighborRequest) anyerror![]const Neighbor = null,
    validate_models: ?*const fn (*anyopaque, t.Config) anyerror!void = null,
    /// Admission-only resolution. Returned immutable models are persisted with
    /// the configuration; execution always validates the pinned revisions.
    qualify_models: ?*const fn (*anyopaque, A, t.Config) anyerror!t.Models = null,
};
pub const Context = struct {
    branch: []const u8 = "",
    table_id: u64 = 0,
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    space: []const u8 = "",
    execution_principal: []const u8 = "",
    execution_signature: []const u8 = "",
    durable_execution_required: bool = false,
    principal: []const u8,
    admin: bool = false,
    curator: bool = false,
    now_ms: i64,
    clock_ms_fn: ?*const fn () i64 = null,
    grant_ptr: ?*anyopaque = null,
    require_grant_fn: ?*const fn (*anyopaque, t.RequiredGrant) anyerror!void = null,
    final_check_ptr: ?*anyopaque = null,
    final_check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    // Only background owners may supply a fresh, bounded publication window
    // after a provider timeout. Foreground deadlines are never renewed.
    checkpoint_ptr: ?*anyopaque = null,
    checkpoint_fn: ?*const fn (*anyopaque) anyerror!void = null,
    check_ptr: ?*anyopaque = null,
    check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    pub fn currentTime(self: Context) i64 {
        return if (self.clock_ms_fn) |clock| clock() else self.now_ms;
    }
    pub fn requireGrant(self: Context, grant: t.RequiredGrant) !void {
        if (grant == .admin and !self.admin) return error.Forbidden;
        if (grant == .curator and !self.admin and !self.curator) return error.Forbidden;
        if (self.require_grant_fn) |call| try call(self.grant_ptr.?, grant);
    }
    pub fn finish(self: Context) !void {
        try self.check();
        if (self.final_check_fn) |call| try call(self.final_check_ptr.?);
    }
    fn beginCheckpoint(self: Context) !void {
        if (self.checkpoint_fn) |call| try call(self.checkpoint_ptr.?);
        try self.finish();
    }
    pub fn check(self: Context) !void {
        if (self.principal.len == 0) return error.Unauthorized;
        if (self.check_fn) |call| try call(self.check_ptr.?);
    }
};
pub const Action = operations.Action;
pub const mutates = operations.mutates;
// Provider invocation/embedding budgets and request budgets use distinct native
// timeout errors. Workers finalize both through the same bounded recovery path.
pub fn isProcessingTimeout(err: anyerror) bool {
    return err == error.Timeout or err == error.DeadlineExceeded;
}
/// Transport admission uses this before provisioning a catalogue resource.
/// Raw credentials cannot become permanent space/branch/resource identifiers.
pub fn screenIdentity(a: A, value: []const u8) !void {
    _ = try screening.text(a, value, .{}, true);
}

pub const Service = struct {
    backend: Backend,
    context: Context,

    const Execution = struct { expected_revision: ?u64 = null, processing_failure: ?anyerror = null, defer_timeouts: bool = false };
    pub fn execute(self: Service, alloc: A, action: Action, id: []const u8, body: []const u8, idempotency_key: []const u8) ![]u8 {
        return self.executeInner(alloc, action, id, body, idempotency_key, .{});
    }
    /// Background execution and timeout recovery share one revision and receipt.
    /// A stale worker cannot charge a failure to newer work or replace a result
    /// whose commit succeeded despite a transport timeout.
    pub fn advanceAtRevision(self: Service, alloc: A, id: []const u8, token: []const u8, revision: u64) ![]u8 {
        return self.executeInner(alloc, .advance, id, "{}", token, .{ .expected_revision = revision, .defer_timeouts = true });
    }
    pub fn recordAttemptFailure(self: Service, alloc: A, id: []const u8, token: []const u8, revision: u64, failure: anyerror) ![]u8 {
        if (id.len == 0) return error.InvalidMemoryRequest;
        return self.executeInner(alloc, .advance, id, "{}", token, .{ .expected_revision = revision, .processing_failure = failure });
    }
    fn executeInner(self: Service, alloc: A, action: Action, id: []const u8, body: []const u8, idempotency_key: []const u8, execution: Execution) ![]u8 {
        try self.context.check();
        if (body.len > 4 * 1024 * 1024) return error.MemoryInputTooLarge;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        if (action == .create) try screenIdentity(a, self.context.space);
        var run: Run = .{ .service = self, .a = a, .explanation_snapshot_scratch = .init(a), .lexical_allocator = alloc, .processing_failure = execution.processing_failure, .defer_timeouts = execution.defer_timeouts };
        defer run.explanation_snapshot_scratch.deinit();
        defer if (run.lexical_query) |*query| query.deinit();
        const state_row = try self.backend.get(self.backend.ptr, a, "control");
        run.state_row = state_row;
        if (action == .create and state_row == null) {
            if (!self.context.admin) return error.Forbidden;
            var config = try parse(t.Config, a, body);
            config.generation = 1;
            config.embedding_generation = 1;
            run.state = .{ .table_id = self.context.table_id, .database = self.context.database, .namespace = self.context.namespace, .space = self.context.space, .execution_principal = self.context.execution_principal, .execution_signature = self.context.execution_signature, .execution_owner = self.context.principal, .config = config, .owner = self.context.principal, .created_at = self.context.now_ms };
        } else {
            const row = state_row orelse return error.MemorySpaceNotFound;
            run.state = try parse(t.State, a, row.json);
            if (run.state.identity_format > 3) return error.MemoryCorruptState;
            if (run.state.table_id != 0 and self.context.table_id != 0 and run.state.table_id != self.context.table_id) return error.MemorySpaceBindingChanged;
            if (run.state.space.len > 0 and self.context.space.len > 0 and (!eql(u8, run.state.space, self.context.space) or !eql(u8, run.state.database, self.context.database) or !eql(u8, run.state.namespace, self.context.namespace))) return error.MemorySpaceBindingChanged;
        }
        try run.prepareLineage(action, id);
        var fingerprint: []const u8 = "";
        var hash_version: u8 = 0;
        const receipt_identity = if (run.service.context.branch.len == 0) try hash(a, .{ self.context.principal, idempotency_key }) else try hash(a, .{ self.context.principal, run.service.context.branch, idempotency_key });
        const receipt_key = if (idempotency_key.len > 0) try key(a, "operation", receipt_identity) else "";
        const required_grant = if (mutates(action)) try run.requiredGrant(action, id, body) else if (actionGrant(action) == .curator) t.RequiredGrant.curator else if (actionGrant(action) == .admin) t.RequiredGrant.admin else null;
        if (required_grant) |grant| try self.context.requireGrant(grant);
        if (mutates(action)) {
            if (idempotency_key.len == 0 or idempotency_key.len > 256) return error.MemoryIdempotencyKeyRequired;
            if (try run.read(t.Operation, receipt_key)) |receipt| {
                fingerprint = try inputFingerprint(a, .{ .action = action, .id = id, .body = body, .principal = self.context.principal }, run.state.input_digest_key, receipt.request_hash_version);
                if (!eql(u8, receipt.request_hash, fingerprint)) return error.MemoryIdempotencyConflict;
                try self.context.requireGrant(operationGrant(receipt));
                try self.context.finish();
                return try std.json.Stringify.valueAlloc(alloc, publicOperation(receipt), .{});
            }
            _ = try screening.text(a, idempotency_key, run.state.config.intake_screening, true);
            _ = try screening.text(a, id, run.state.config.intake_screening, true);
            _ = try screening.text(a, run.service.context.branch, run.state.config.intake_screening, true);
            if (execution.expected_revision) |revision| if (run.state.revision != revision) return error.MemoryRevisionConflict;
            if (run.state.pending_config != null and action != .advance and action != .forget and action != .stage_embeddings) return error.MemoryConfigurationTransition;
            if (action == .create) {
                if (state_row != null) return error.MemorySpaceExists;
                run.state.config = try self.validate(a, run.state.config, run.state.input_digest_key.len == 64);
            }
            if (run.state.revision == std.math.maxInt(u64)) return error.MemoryCorruptState;
            run.state.revision += 1;
        }
        // A current ingestion budget cannot invalidate a committed receipt or
        // prevent the control operations needed to recover/configure a space.
        if (operations.usesInputBudget(action) and body.len > run.state.config.max_input_bytes) return error.MemoryInputTooLarge;
        if (mutates(action)) {
            if (run.state.input_digest_key.len == 0) {
                if (self.backend.random_bytes) |random| {
                    var secret: [32]u8 = undefined;
                    try random(self.backend.ptr, &secret);
                    run.state.input_digest_key = try a.dupe(u8, &std.fmt.bytesToHex(secret, .lower));
                } else if (run.state.config.intake_screening.mode == .redact) return error.MemoryCapabilityUnavailable;
            }
            if (run.state.input_digest_key.len != 0 and run.state.input_digest_key.len != 64) return error.MemoryCorruptState;
            hash_version = if (run.state.input_digest_key.len == 64) 1 else 0;
            fingerprint = try inputFingerprint(a, .{ .action = action, .id = id, .body = body, .principal = self.context.principal }, run.state.input_digest_key, hash_version);
        }
        var admitted_body = body;
        var intake_report: screening.Report = .{};
        if (screenedAction(action)) {
            const admitted = try screenBody(a, action, body, run.state.config.intake_screening);
            admitted_body = admitted.value;
            intake_report = admitted.report;
        }
        run.intake_report = intake_report;
        var op: t.Operation = .{
            .required_grant = required_grant,
            .execution_principal = self.context.execution_principal,
            .execution_signature = self.context.execution_signature,
            .id = if (receipt_key.len > 0) receipt_identity else "",
            .branch = run.service.context.branch,
            .action = @tagName(action),
            .request_hash = fingerprint,
            .request_hash_version = hash_version,
            .intake_screening = intake_report,
            .owner = self.context.principal,
            .revision = run.state.revision,
            .config_generation = run.state.config.generation,
            .created_at = self.context.now_ms,
        };
        var output: ?[]const u8 = null;
        switch (action) {
            .put_evaluation_dataset => try run.putEvaluationDataset(try parse(t.evaluation.DatasetRequest, a, admitted_body)),
            .evaluation_dataset => output = try run.encode(try run.evaluationDataset(id)),
            .evaluation_cases => output = try run.evaluationCases(id, try parse(operations.Page, a, admitted_body)),
            .start_evaluation => try run.startEvaluation(try parse(t.evaluation.Start, a, admitted_body), &op),
            .evaluation_run => output = try run.evaluationStatus(id),
            .evaluation_results => output = try run.evaluationResults(id, try parse(operations.EvaluationResultRequest, a, admitted_body)),
            .evaluation_tasks => output = try run.evaluationTasks(id, try parse(operations.Page, a, admitted_body)),
            .evaluation_verdict => try run.evaluationVerdict(id, try parse(t.evaluation.Submit, a, admitted_body), &op),
            .lineage => output = try run.encode(run.lineage_head orelse return error.MemoryCapabilityUnavailable),
            .fork_lineage => try run.forkLineage(try parse(t.LineageFork, a, admitted_body)),
            .checkpoint_lineage => try run.checkpointLineage(try parse(t.LineageCheckpointRequest, a, admitted_body)),
            .revert_lineage => try run.revertLineage(try parse(t.LineageRevert, a, admitted_body)),
            .supersession_candidates => output = try run.supersessionCandidates(id, try parse(t.SupersessionCandidateRequest, a, admitted_body)),
            .propose_supersession => try run.proposeSupersession(try parse(t.SupersessionProposalRequest, a, admitted_body), &op),
            .supersession_proposals => output = try run.supersessionProposals(try parse(operations.Page, a, admitted_body)),
            .supersession_proposal => output = try run.encode(try run.supersessionProposalDetail(id)),
            .review_supersession => try run.reviewSupersession(id, try parse(t.SupersessionVerdict, a, admitted_body), &op),
            .changes => output = try run.spaceChanges(try parse(t.ChangeRequest, a, admitted_body)),
            .create => {
                try run.put(try configKey(a, run.state.config.generation), run.state.config);
                try run.put("entity-candidate-index", Run.CandidateIndex{ .complete = true });
                if (self.backend.search_assertions != null) try run.put("assertion-recall-index", t.RecallIndex{ .generation = run.state.config.generation, .backfill_complete = true, .complete = true });
            },
            .explain => output = try run.explain(id, try parse(prov.ExplainRequest, a, admitted_body)),
            .configuration => {
                const generation = std.fmt.parseInt(u64, id, 10) catch return error.InvalidMemoryRequest;
                const config = (try run.read(t.Config, try configKey(a, generation))) orelse return error.MemoryNotFound;
                output = try run.encode(config);
            },
            .inspect => output = try run.encode(.{ .state = .{ .table_id = run.state.table_id, .database = run.state.database, .namespace = run.state.namespace, .space = run.state.space, .config = run.state.config, .pending_config = run.state.pending_config, .revision = run.state.revision, .owner = run.state.owner, .created_at = run.state.created_at }, .capabilities = .{ .native = true, .model_free = true, .generator_available = self.backend.generate != null, .embedder_available = self.backend.embed != null } }),
            .configure => {
                try run.requireAdmin();
                try run.put(try run.semanticPath("entity-semantic-index"), t.SemanticIndex{});
                try run.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
                const request = try parse(operations.Configure, a, admitted_body);
                if (request.expected_revision != run.state.revision - 1) return error.MemoryRevisionConflict;
                var config = request.config;
                if (run.state.config.features.lineage and !config.features.lineage) return error.InvalidMemoryConfig;
                // Admission revisions are durable and never reused, including failed
                // builds. Serving generations are not allocation counters.
                config.generation = run.state.revision;
                config = try self.validate(a, config, run.state.input_digest_key.len == 64);
                if (self.context.execution_signature.len > 0) {
                    run.state.execution_principal = self.context.execution_principal;
                    run.state.execution_signature = self.context.execution_signature;
                    run.state.execution_owner = self.context.principal;
                }
                if (config.retention_ms != run.state.config.retention_ms) run.state.next_expiry_at = self.context.now_ms;
                const embedding_changed = (config.features.semantic_recall or config.semantic_discovery.enabled) and (!(run.state.config.features.semantic_recall or run.state.config.semantic_discovery.enabled) or !valueEqual(?t.Model, run.state.config.models.embedder, config.models.embedder));
                const rebuild = config.features.semantic_recall and (!run.state.config.features.semantic_recall or embedding_changed);
                config.embedding_generation = if (embedding_changed) config.generation else run.state.config.embedding_generation;
                try run.configurationTransition(config, false);
                if (rebuild) {
                    run.state.pending_config = config;
                    op.phase = .pending;
                    op.assertions_ready = false;
                } else try run.applyConfiguration(config);
                op.config_generation = config.generation;
                try run.put(try configKey(a, config.generation), config);
            },
            .observe => {
                var request = try parse(t.ObservationRequest, a, admitted_body);
                const original = try parse(t.ObservationRequest, a, body);
                run.original_evidence = original.evidence;
                const sources = try a.dupe(t.Evidence, request.evidence);
                for (sources, original.evidence) |*source, supplied| {
                    if (!eql(u8, source.text, supplied.text)) try run.intake_redacted_evidence.put(a, supplied.id, {});
                    if (supplied.resource != null or supplied.resource_selector != null) source.text = supplied.text;
                }
                request.evidence = sources;
                try run.observe(request, &op);
            },
            .assertions => {
                const request = try parse(t.AssertionRequest, a, admitted_body);
                try run.assertions(request.assertions, false);
            },
            .correct => try run.correct(try parse(t.Correction, a, admitted_body)),
            .forget => try run.forget(try parse(t.Forget, a, admitted_body)),
            .outcome => try run.outcome(try parse(t.Outcome, a, admitted_body)),
            .advance => try run.advance(id, &op),
            .evidence => {
                const source = (try run.evidence(id)) orelse return error.MemoryNotFound;
                try run.authorize(source.owner, source.evidence.visibility);
                if (!try run.evidenceRetained(source)) return error.MemoryNotFound;
                output = try run.encode(source);
            },
            .get => {
                const record = (try run.assertion(id)) orelse return error.MemoryNotFound;
                try run.authorize(record.owner, record.assertion.visibility);
                if (!try run.evidenceVisible(record)) return error.MemoryNotFound;
                output = try run.encode(try run.publicRecord(record));
            },
            .operation => {
                const receipt = (try run.read(t.Operation, try key(a, "operation", id))) orelse return error.MemoryNotFound;
                try run.authorize(receipt.owner, .private);
                output = try run.encode(publicOperation(receipt));
            },
            .history => output = try run.history(id, try parse(operations.Page, a, admitted_body)),
            .recall => output = try run.recall(try parse(t.Recall, a, admitted_body)),
            .list => output = try run.enumerate(try parse(t.Recall, a, admitted_body)),
            .stage_embeddings => try run.stageEmbeddings(try parse(t.EmbeddingBatch, a, admitted_body)),
            .curate_entity => try run.curateEntity(try parse(t.EntityCorrection, a, admitted_body)),
            .propose_relationship => try run.proposeRelationship(try parse(t.RelationshipProposalRequest, a, admitted_body), &op),
            .relationship_proposal => output = try run.encode(try run.publicRelationshipProposal(id)),
            .review_relationship => try run.reviewRelationship(id, try parse(t.EntityVerdict, a, admitted_body), &op),
            .entity_view => output = try run.encode(try run.entityView(id, try parse(t.EntityViewRequest, a, admitted_body))),
            .episodes => output = try run.episodes(try parse(t.EpisodeRequest, a, admitted_body)),
            .episode => output = try run.encode(try run.episode(id, try parse(t.EpisodeRequest, a, admitted_body))),
            .entity_candidates => output = try run.entityCandidates(id, try parse(t.EntityCandidateRequest, a, admitted_body)),
            .propose_entity => try run.proposeEntity(try parse(t.EntityProposalRequest, a, admitted_body), &op),
            .entity_proposals => output = try run.entityProposals(try parse(operations.Page, a, admitted_body)),
            .entity_proposal => output = try run.entityProposal(id),
            .review_entity => try run.reviewEntity(id, try parse(t.EntityVerdict, a, admitted_body), &op),
            .resolve_entity => output = try run.resolveEntity(id),
            .entities => output = try run.entityList(try parse(operations.Page, a, admitted_body)),
        }
        if (mutates(action)) {
            op.intake_screening = run.intake_report;
            try run.recordDecision(op);
            try run.put(receipt_key, op);
            try run.recordOperationVersions(op);
            if (op.phase == .pending and (action == .observe or action == .configure or action == .start_evaluation)) try run.put(try key(a, "pending", op.id), .{ .id = op.id });
            if (action == .configure or action == .curate_entity or action == .review_entity or action == .forget) try run.invalidateSemanticBranches();
            try run.publishKnowledgeRevision(action);
            try run.publishLineage(action);
            try run.publishRootReferences();
            if (action == .advance and id.len == 0 and run.state.persistent_roots) try run.reclaimNativeNodes();
            try run.publishFeed(op);
            try run.put("control", run.state);
            // Authorization callbacks may perform I/O. Revalidate frozen views
            // and source snapshots after the final grant check. A newly used
            // administrative view upgrades the receipt and repeats this fence;
            // the requirement is monotonic, so at most two passes are possible.
            while (true) {
                const administrative = run.administrative_view;
                if (administrative) {
                    try self.context.requireGrant(.admin);
                    if (op.required_grant != .admin) {
                        op.required_grant = .admin;
                        try run.put(receipt_key, op);
                        try run.recordOperationVersions(op);
                    }
                }
                try self.context.finish();
                try run.validateJudgedViews();
                try run.validateResources();
                if (administrative or !run.administrative_view) break;
            }
            try self.backend.commit(self.backend.ptr, a, state_row, run.changes.items);
            try run.finishSemanticPublication();
            try run.finishRecallPublication();
            output = try run.encode(publicOperation(op));
        } else {
            if (run.administrative_view) try self.context.requireGrant(.admin);
            try self.context.finish();
            // A frozen view is not a permission lease after its initial lookup.
            try run.validateResources();
            // Reads spanning pages must not report a mixture of revisions.
            const latest = (try self.backend.get(self.backend.ptr, a, "control")) orelse return error.MemorySpaceNotFound;
            const state = try parse(t.State, a, latest.json);
            if (state.revision != run.state.revision) return error.MemoryRevisionConflict;
        }
        return try alloc.dupe(u8, output.?);
    }

    fn validate(self: Service, a: A, requested: t.Config, has_digest_key: bool) !t.Config {
        var config = requested;
        var screening_config = config.intake_screening;
        screening_config.mode = .reject;
        _ = try screening.typed(t.Config, a, config, screening_config);
        if (config.intake_screening.mode == .redact and !has_digest_key and self.backend.random_bytes == null) return error.MemoryCapabilityUnavailable;
        try config.validate();
        if (self.context.durable_execution_required and self.context.execution_signature.len == 0 and (config.features.assertion_distillation or config.features.entity_extraction or config.features.summaries or ((config.features.semantic_recall or config.semantic_discovery.enabled) and !eql(u8, config.models.embedder.?.provider, "external")) or config.retention_ms != null)) return error.MemoryExecutionIdentityRequired;
        if (config.features.assertion_distillation and self.backend.generate == null) return error.MemoryCapabilityUnavailable;
        if (config.features.reconciliation == .model and self.backend.decide == null) return error.MemoryCapabilityUnavailable;
        if (config.semantic_discovery.enabled and eql(u8, config.models.embedder.?.provider, "external")) return error.MemoryModelRequired;
        if (config.semantic_discovery.enabled and (self.backend.embed_document == null or self.backend.neighbors == null or self.backend.ensure_vector_index == null)) return error.MemoryCapabilityUnavailable;
        if ((config.features.semantic_recall or config.semantic_discovery.enabled) and self.backend.embed == null) return error.MemoryCapabilityUnavailable;
        if (self.backend.qualify_models) |qualify| {
            config.models = try qualify(self.backend.ptr, a, config);
        } else if (self.backend.validate_models) |validate_models| try validate_models(self.backend.ptr, config) else {
            if (config.features.entity_extraction or config.features.summaries or config.features.reconciliation == .model) return error.MemoryCapabilityUnavailable;
        }
        // No unresolved geometry may enter durable configuration, including
        // backends which only implement validation rather than qualification.
        try config.validate();
        if (config.models.embedder) |model| if (model.dimensions == null)
            return error.MemoryEmbeddingDimensionsRequired;
        inline for (@typeInfo(t.Models).@"struct".field_names) |field| {
            if (@field(config.models, field)) |model| if (eql(u8, model.revision, "current"))
                return error.MemoryModelRevisionUnavailable;
        }
        return config;
    }
};

const valueEqual = @import("value_equal.zig").equal;

const Run = struct {
    const ForwardIdentity = struct { canonical: []const u8, suppressed: bool, target_suppressed: bool, distance: u32 };
    const IdentityScope = struct { policy: EntityPolicy, redirects: curation.RedirectIndex, suppression: curation.Index };

    const ChangeTree = std.Treap([]const u8, struct {
        fn order(a: []const u8, b: []const u8) std.math.Order {
            return std.mem.order(u8, a, b);
        }
    }.order);

    const ResourceRead = struct { reference: prov.Resource, snapshot: prov.Snapshot, epoch: u64 };
    const ResourceSet = std.StringHashMapUnmanaged(void);
    service: Service,
    a: A,
    lexical_allocator: A,
    lexical_query: ?lexical.PreparedQuery = null,
    state: t.State = undefined,
    state_row: ?Row = null,
    change_ranges: ChangeTree = .{},
    changes: std.ArrayList(Change) = .empty,
    change_positions: std.StringHashMapUnmanaged(usize) = .empty,
    assertion_rows: std.StringHashMapUnmanaged(t.StoredAssertion) = .empty,
    evidence_rows: std.StringHashMapUnmanaged(t.StoredEvidence) = .empty,
    entity_rows: std.StringHashMapUnmanaged(t.EntityRecord) = .empty,
    projection_rows: std.StringHashMapUnmanaged(t.EntityProjection) = .empty,
    identity_members: std.StringHashMapUnmanaged([]const []const u8) = .empty,
    identity_policy: ?EntityPolicy = null,
    identity_native: bool = false,
    identity_forward: std.StringHashMapUnmanaged(ForwardIdentity) = .empty,
    test_identity_forward_steps: usize = 0,
    identity_scopes: std.StringHashMapUnmanaged(*IdentityScope) = .empty,
    identity_full_scopes: std.StringHashMapUnmanaged(*IdentityScope) = .empty,
    identity_legacy_scope: ?*IdentityScope = null,
    reviewing_entity: ?[]const u8 = null,
    identity_index: ?curation.RedirectIndex = null,
    suppression_index: ?curation.Index = null,
    dependency_depths: std.StringHashMapUnmanaged(u8) = .empty,
    dependency_visits: usize = 0,
    support_work: usize = 0,
    historical_resource_reads: bool = false,
    retention_maintenance: bool = false,
    administrative_view: bool = false,
    resource_states: std.StringHashMapUnmanaged(ResourceRead) = .empty,
    judged_views: std.ArrayList(t.EntityViewInput) = .empty,
    inferred: bool = false,
    semantic_ready_after_commit: bool = false,
    semantic_publication_branch: ?[]const u8 = null,
    recall_ready_after_commit: bool = false,
    temporal_tracking: bool = false,
    semantic_assertions: ResourceSet = .empty,
    temporal_boundary: ?i64 = null,
    temporal_denied: bool = false,
    inference_epoch: u64 = 0,
    capturing_resources: ?*ResourceSet = null,
    intake_report: screening.Report = .{},
    intake_redacted_evidence: ResourceSet = .empty,
    original_evidence: []const t.Evidence = &.{},
    processing_failure: ?anyerror = null,
    defer_timeouts: bool = false,
    explanation_decisions: std.StringHashMapUnmanaged(void) = .empty,
    explanation_truncated: bool = false,
    explanation_incomplete_history: bool = false,
    explanation_records: std.StringHashMapUnmanaged(ExplainedRecord) = .empty,
    explanation_snapshot_candidates: usize = 0,
    explanation_snapshot_scratch: std.heap.ArenaAllocator,
    decision_targets: std.ArrayList(prov.Resource) = .empty,
    views: std.StringHashMapUnmanaged(bool) = .empty,
    backend_rows: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    historical: std.StringHashMapUnmanaged(?t.StoredAssertion) = .empty,
    lineage_head: ?t.LineageHead = null,
    lineage_index: ?@import("persistent_index.zig").Index = null,
    lineage_raw: bool = false,

    fn publishKnowledgeRevision(self: *Run, action: Action) !void {
        var changed = action == .revert_lineage or action == .curate_entity or action == .forget;
        for (self.changes.items) |change| {
            if (!std.mem.startsWith(u8, change.key, "assertion/") and !std.mem.startsWith(u8, change.key, "evidence/") and !std.mem.startsWith(u8, change.key, "entity/") and !eql(u8, change.key, "entity-policy") and !std.mem.startsWith(u8, change.key, "entity-authority/")) continue;
            const old = if (self.backend_rows.getEntry(change.key)) |cached| cached.value_ptr.* else blk: {
                const row = try self.service.backend.get(self.service.backend.ptr, self.a, change.key);
                break :blk if (row) |value| value.json else null;
            };
            if (!optionalEqual(old, change.json)) {
                changed = true;
                break;
            }
        }
        if (changed) self.state.knowledge_revision = try std.math.add(u64, self.state.knowledge_revision, 1);
    }
    fn persistentIndex(self: *Run) *@import("persistent_index.zig").Index {
        if (self.lineage_index == null) self.lineage_index = .{ .a = self.a, .store = .{ .ptr = self, .get = lineageNodeGet, .put = lineageNodePut }, .prefix = "lineage-node/" };
        return &self.lineage_index.?;
    }
    const EvaluationReference = struct { version: u8 = 1, path: []const u8, bytes: u32, nonce: []const u8 };
    const EvaluationIndexItem = struct { key: []const u8, value: []const u8 };
    const EvaluationIndexPage = struct { items: []const EvaluationIndexItem, next: ?[]const u8 };
    fn evaluationReference(self: *Run, value: []const u8) !EvaluationReference {
        // Early manifests stored only the native immutable record path.
        if (std.mem.startsWith(u8, value, "evaluation-case/")) return .{ .path = value, .bytes = 1024 * 1024, .nonce = "" };
        const reference = try parse(EvaluationReference, self.a, value);
        if (reference.version != 1 or !std.mem.startsWith(u8, reference.path, "evaluation-case/") or reference.bytes > 1024 * 1024) return error.MemoryCorruptState;
        return reference;
    }
    fn evaluationIndexPage(self: *Run, root: ?[]const u8, after: ?[]const u8, limit: usize, max_bytes: usize) !EvaluationIndexPage {
        const page = try self.persistentIndex().page(root, "", null, after, limit);
        var items: std.ArrayList(EvaluationIndexItem) = .empty;
        var bytes: usize = 0;
        var next = page.next;
        var last = after;
        for (page.items) |item| {
            const reference = try self.evaluationReference(item.value);
            if (items.items.len > 0 and bytes + reference.bytes > max_bytes) {
                next = last;
                break;
            }
            bytes += reference.bytes;
            try items.append(self.a, .{ .key = item.key, .value = reference.path });
            last = item.key;
        }
        return .{ .items = items.items, .next = next };
    }
    fn evaluationDecidedKey(self: *Run, run: []const u8, case_key: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "evaluation-decided/{s}/{s}", .{ try hash(self.a, run), case_key });
    }
    fn evaluationDecided(self: *Run, run: t.evaluation.Run, case_key: []const u8) !bool {
        if (try self.read(bool, try self.evaluationDecidedKey(run.id, case_key)) != null) return true;
        if (run.version >= 2) return false;
        return try self.read(t.evaluation.Result, try std.fmt.allocPrint(self.a, "evaluation-result/{s}/{s}", .{ try hash(self.a, run.id), case_key })) != null;
    }
    fn evaluationCaseAvailable(self: *Run, value: t.evaluation.StoredCase) !bool {
        if (value.forgotten or try self.read(bool, "forgotten-all") != null) return false;
        for (value.value.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return false;
            if (!self.retention_maintenance) self.authorize(source.owner, source.evidence.visibility) catch return false;
            if (!try self.evidenceRetained(source)) return false;
        }
        return true;
    }
    fn evaluationDataset(self: *Run, id: []const u8) !t.evaluation.Dataset {
        if (try self.read(bool, "forgotten-all") != null) return error.MemoryNotFound;
        const value = (try self.read(t.evaluation.Dataset, try key(self.a, "evaluation-dataset", id))) orelse return error.MemoryNotFound;
        if (!eql(u8, value.branch, self.service.context.branch)) return error.MemoryNotFound;
        try self.authorize(value.owner, value.visibility);
        return value;
    }
    fn putEvaluationDataset(self: *Run, request: t.evaluation.DatasetRequest) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try t.validateId(request.id);
        if (request.cases.len + request.remove.len == 0 or request.cases.len + request.remove.len > 128) return error.InvalidMemoryRequest;
        const path = try key(self.a, "evaluation-dataset", request.id);
        var dataset = (try self.read(t.evaluation.Dataset, path)) orelse t.evaluation.Dataset{ .id = request.id, .revision = self.state.revision, .owner = self.service.context.principal, .branch = self.service.context.branch, .visibility = request.visibility, .recorded_at = self.service.context.now_ms };
        if (!eql(u8, dataset.branch, self.service.context.branch)) return error.MemoryVisibilityConflict;
        try self.authorize(dataset.owner, .private);
        if (dataset.visibility != request.visibility) return error.MemoryVisibilityConflict;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (request.cases) |value| {
            try t.evaluation.validateCase(value);
            if (try encodedSize(value) > 1024 * 1024 - 8192) return error.MemoryInputTooLarge;
            const entry = try seen.getOrPut(self.a, value.id);
            if (entry.found_existing) return error.InvalidMemoryRequest;
            try self.reviewEvidence(value.evidence);
            if (dataset.visibility == .team) for (value.evidence) |ref| {
                if ((try self.evidence(ref.id)).?.evidence.visibility != .team) return error.MemoryVisibilityConflict;
            };
            const node_key = try hash(self.a, value.id);
            if (try self.persistentIndex().get(dataset.root, node_key) == null) dataset.cases += 1;
            const reference = try std.fmt.allocPrint(self.a, "evaluation-case/{s}/{s}/{d:0>20}", .{ try hash(self.a, request.id), node_key, self.state.revision });
            const stored: t.evaluation.StoredCase = .{ .value = value, .dataset = request.id, .revision = self.state.revision, .owner = self.service.context.principal, .branch = self.service.context.branch, .recorded_at = self.service.context.now_ms, .fingerprint = try hash(self.a, t.evaluation.JudgeInput{ .task = value.task, .input = value.input }) };
            const size = try encodedSize(stored);
            if (size > 1024 * 1024) return error.MemoryInputTooLarge;
            try self.put(reference, stored);
            const descriptor: EvaluationReference = .{ .path = reference, .bytes = @intCast(size), .nonce = try inputFingerprint(self.a, .{ .domain = "evaluation-case-ref/v1", .path = reference }, self.state.input_digest_key, if (self.state.input_digest_key.len == 64) 1 else 0) };
            dataset.root = try self.persistentIndex().put(dataset.root, node_key, try self.encode(descriptor));
        }
        for (request.remove) |id| {
            try t.validateId(id);
            const entry = try seen.getOrPut(self.a, id);
            if (entry.found_existing) return error.InvalidMemoryRequest;
            const node_key = try hash(self.a, id);
            if (try self.persistentIndex().get(dataset.root, node_key) != null) {
                dataset.root = try self.persistentIndex().remove(dataset.root, node_key);
                dataset.cases -= 1;
            }
        }
        dataset.revision = self.state.revision;
        try self.put(path, dataset);
        try self.put(try std.fmt.allocPrint(self.a, "evaluation-dataset-version/{s}/{d:0>20}", .{ try hash(self.a, dataset.id), dataset.revision }), dataset);
    }
    fn evaluationCursor(self: *Run, scope: []const u8, position: ?[]const u8) !?[]const u8 {
        return if (position) |value| try std.fmt.allocPrint(self.a, "{s}:{s}", .{ try hash(self.a, .{ "evaluation/v1", scope, self.service.context.principal, self.service.context.branch }), value }) else null;
    }
    fn evaluationPosition(self: *Run, scope: []const u8, cursor: ?[]const u8) !?[]const u8 {
        const value = cursor orelse return null;
        const colon = std.mem.indexOfScalar(u8, value, ':') orelse return error.InvalidMemoryCursor;
        const position = value[colon + 1 ..];
        if (position.len != 64 or !eql(u8, value, (try self.evaluationCursor(scope, position)).?)) return error.InvalidMemoryCursor;
        for (position) |byte| if (!std.ascii.isHex(byte)) return error.InvalidMemoryCursor;
        return position;
    }
    fn evaluationCases(self: *Run, id: []const u8, request: operations.Page) ![]const u8 {
        if (request.limit == 0 or request.limit > 128) return error.InvalidMemoryRequest;
        const dataset = try self.evaluationDataset(id);
        const scope = try hash(self.a, .{ id, dataset.root, dataset.revision });
        const page = try self.evaluationIndexPage(dataset.root, try self.evaluationPosition(scope, request.cursor), request.limit, 1024 * 1024);
        var references: std.ArrayList([]const u8) = .empty;
        for (page.items) |item| try references.append(self.a, item.value);
        try self.prefetch(references.items);
        var values: std.ArrayList(t.evaluation.StoredCase) = .empty;
        var next = page.next;
        var last = try self.evaluationPosition(scope, request.cursor);
        var bytes: usize = 4096;
        for (page.items) |item| if (try self.read(t.evaluation.StoredCase, item.value)) |value| {
            if (try self.evaluationCaseAvailable(value)) {
                const size = try encodedSize(value);
                if (bytes + size > 4 * 1024 * 1024) {
                    if (values.items.len == 0) return error.MemoryInputTooLarge;
                    next = last;
                    break;
                }
                bytes += size;
                try values.append(self.a, value);
            }
            last = item.key;
        };
        return self.encode(t.evaluation.CasePage{ .items = values.items, .next_cursor = try self.evaluationCursor(scope, next), .complete = next == null, .revision = dataset.revision });
    }
    fn startEvaluation(self: *Run, request: t.evaluation.Start, operation: *t.Operation) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try t.validateId(request.id);
        var dataset = try self.evaluationDataset(request.dataset);
        if (request.dataset_revision) |revision| {
            dataset = (try self.read(t.evaluation.Dataset, try std.fmt.allocPrint(self.a, "evaluation-dataset-version/{s}/{d:0>20}", .{ try hash(self.a, request.dataset), revision }))) orelse return error.MemoryNotFound;
            try self.authorize(dataset.owner, dataset.visibility);
        }
        const path = try key(self.a, "evaluation-run", request.id);
        if (try self.read(t.evaluation.Run, path) != null) return error.MemoryEntityConflict;
        const judge = if (request.mode == .managed) request.judge orelse self.state.config.models.decider orelse self.state.config.models.generator orelse return error.MemoryModelRequired else request.judge;
        if (request.mode == .policy and judge != null) return error.InvalidMemoryRequest;
        if (judge) |model| {
            try t.validateId(model.provider);
            try t.validateId(model.model);
            try t.validateId(model.revision);
        }
        if (request.mode == .managed) {
            if (self.service.backend.evaluate == null) return error.MemoryCapabilityUnavailable;
            if (self.service.backend.validate_models) |validate_models| {
                var qualification = self.state.config;
                qualification.features.reconciliation = .model;
                qualification.models.decider = judge;
                try validate_models(self.service.backend.ptr, qualification);
                try self.inferenceCompleted(.empty);
            }
        }
        if (self.service.context.durable_execution_required and self.service.context.execution_signature.len == 0) return error.MemoryExecutionIdentityRequired;
        var evaluated_config = self.state.config;
        if (request.thresholds) |thresholds| {
            if (request.mode != .policy or thresholds.enabled != self.state.config.semantic_discovery.enabled) return error.InvalidMemoryRequest;
            evaluated_config.semantic_discovery = thresholds;
            try evaluated_config.validate();
        }
        const run: t.evaluation.Run = .{ .version = 2, .id = request.id, .dataset = dataset.id, .dataset_revision = dataset.revision, .root = dataset.root, .case_count = dataset.cases, .owner = self.service.context.principal, .branch = self.service.context.branch, .visibility = dataset.visibility, .mode = request.mode, .knowledge_revision = self.state.knowledge_revision, .config_generation = self.state.config.generation, .config_fingerprint = try hash(self.a, evaluated_config), .source_config_fingerprint = try hash(self.a, self.state.config), .thresholds = request.thresholds, .judge = judge, .fingerprint = try hash(self.a, .{ dataset.id, dataset.revision, dataset.root, evaluated_config, request.mode, judge, t.evaluation.prompt_revision }), .phase = if (dataset.cases == 0) .completed else .pending, .revision = self.state.revision, .recorded_at = self.service.context.now_ms };
        try self.put(path, run);
        operation.evaluation_id = run.id;
        if (run.phase == .pending) operation.phase = .pending;
    }
    fn evaluationRun(self: *Run, id: []const u8) !t.evaluation.Run {
        const run = (try self.read(t.evaluation.Run, try key(self.a, "evaluation-run", id))) orelse return error.MemoryNotFound;
        if (!eql(u8, run.branch, self.service.context.branch)) return error.MemoryNotFound;
        try self.authorize(run.owner, run.visibility);
        return run;
    }
    fn evaluationStatus(self: *Run, id: []const u8) ![]const u8 {
        const run = try self.evaluationRun(id);
        return self.encode(t.evaluation.Status{ .run = run, .identity = run.counts[0].report(), .relationship = run.counts[1].report(), .supersession = run.counts[2].report(), .retrieval = run.counts[3].report(), .identity_admission = run.admission[0].report(), .relationship_admission = run.admission[1].report(), .supersession_admission = run.admission[2].report() });
    }
    fn evaluationResultKey(self: *Run, run: []const u8, case_id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "evaluation-result/{s}/{s}", .{ try hash(self.a, run), try hash(self.a, case_id) });
    }
    fn evaluationTasks(self: *Run, id: []const u8, request: operations.Page) ![]const u8 {
        if (request.limit == 0 or request.limit > 128) return error.InvalidMemoryRequest;
        const run = try self.evaluationRun(id);
        if (run.mode != .external) return error.InvalidMemoryRequest;
        if (run.phase != .pending) return self.encode(t.evaluation.TaskPage{ .items = &.{}, .next_cursor = null, .complete = true, .run_fingerprint = run.fingerprint });
        const page = try self.evaluationIndexPage(run.root, (try self.evaluationPosition(run.fingerprint, request.cursor)) orelse run.cursor, request.limit, 1024 * 1024);
        var references: std.ArrayList([]const u8) = .empty;
        for (page.items) |item| {
            try references.append(self.a, item.value);
            try references.append(self.a, try self.evaluationDecidedKey(run.id, item.key));
        }
        try self.prefetch(references.items);
        var values: std.ArrayList(t.evaluation.JudgeTask) = .empty;
        var next = page.next;
        var last = (try self.evaluationPosition(run.fingerprint, request.cursor)) orelse run.cursor;
        var bytes: usize = 4096;
        for (page.items) |item| if (try self.read(t.evaluation.StoredCase, item.value)) |value| {
            if (try self.evaluationDecided(run, item.key) or !try self.evaluationCaseAvailable(value)) {
                last = item.key;
                continue;
            }
            const task: t.evaluation.JudgeTask = .{ .case_id = value.value.id, .case_revision = value.revision, .input_fingerprint = value.fingerprint, .input = .{ .task = value.value.task, .input = value.value.input } };
            const size = try encodedSize(task);
            if (bytes + size > 4 * 1024 * 1024) {
                if (values.items.len == 0) return error.MemoryInputTooLarge;
                next = last;
                break;
            }
            bytes += size;
            try values.append(self.a, task);
            last = item.key;
        };
        return self.encode(t.evaluation.TaskPage{ .items = values.items, .next_cursor = try self.evaluationCursor(run.fingerprint, next), .complete = next == null, .run_fingerprint = run.fingerprint });
    }
    fn evaluationResults(self: *Run, id: []const u8, request: operations.EvaluationResultRequest) ![]const u8 {
        if (request.limit == 0 or request.limit > 8) return error.InvalidMemoryRequest;
        const run = try self.evaluationRun(id);
        const position = try self.evaluationPosition(run.fingerprint, request.cursor);
        const page = try self.evaluationIndexPage(run.root, position, request.limit, 512 * 1024);
        var paths: std.ArrayList([]const u8) = .empty;
        for (page.items) |item| {
            try paths.append(self.a, item.value);
            try paths.append(self.a, try std.fmt.allocPrint(self.a, "evaluation-result/{s}/{s}", .{ try hash(self.a, run.id), item.key }));
        }
        try self.prefetch(paths.items);
        var values: std.ArrayList(t.evaluation.ResultEntry) = .empty;
        var next = page.next;
        var last = position;
        var bytes: usize = 4096;
        for (page.items) |item| {
            const value = (try self.read(t.evaluation.StoredCase, item.value)) orelse continue;
            if (!try self.evaluationCaseAvailable(value)) {
                last = item.key;
                continue;
            }
            const result = try self.read(t.evaluation.Result, try std.fmt.allocPrint(self.a, "evaluation-result/{s}/{s}", .{ try hash(self.a, run.id), item.key }));
            if (result != null and result.?.scrubbed) {
                last = item.key;
                continue;
            }
            const entry: t.evaluation.ResultEntry = .{ .case = value, .result = result };
            const size = try encodedSize(entry);
            if (bytes + size > 4 * 1024 * 1024) {
                if (values.items.len == 0) return error.MemoryInputTooLarge;
                next = last;
                break;
            }
            bytes += size;
            try values.append(self.a, entry);
            last = item.key;
        }
        return self.encode(t.evaluation.ResultPage{ .items = values.items, .next_cursor = try self.evaluationCursor(run.fingerprint, next), .complete = next == null, .run_fingerprint = run.fingerprint });
    }
    fn evaluationCase(self: *Run, run: t.evaluation.Run, id: []const u8) !t.evaluation.StoredCase {
        const reference = (try self.persistentIndex().get(run.root, try hash(self.a, id))) orelse return error.MemoryNotFound;
        return (try self.read(t.evaluation.StoredCase, (try self.evaluationReference(reference)).path)) orelse return error.MemoryNotFound;
    }
    fn recordEvaluationResult(self: *Run, run: *t.evaluation.Run, value: t.evaluation.StoredCase, case_key: []const u8, prediction: ?t.evaluation.Prediction, declared_invalid: bool, actor: []const u8, model: ?t.Model) !void {
        const path = try std.fmt.allocPrint(self.a, "evaluation-result/{s}/{s}", .{ try hash(self.a, run.id), case_key });
        var invalid = declared_invalid;
        if (prediction) |result| {
            if (result.reason.len > t.max_text_bytes) invalid = true;
            if (result.admitted) |admitted| {
                if (run.mode != .policy) return error.InvalidMemoryRequest;
                try t.evaluation.scoreAdmission(&run.admission[@backingInt(value.value.task)], value.value, admitted);
            }
        }
        if (try self.evaluationDecided(run.*, case_key)) return error.MemoryEntityConflict;
        const task = @backingInt(value.value.task);
        var normalized = prediction;
        if (value.value.task != .retrieval and normalized != null and normalized.?.ranking != null and normalized.?.ranking.?.len == 0) normalized.?.ranking = null;
        if (invalid) normalized = .{ .reason = "Invalid judge output" };
        var counts = run.counts[task];
        t.evaluation.score(&counts, value.value, normalized) catch |err| switch (err) {
            error.InvalidMemoryRequest, error.InvalidMemoryId => {
                invalid = true;
                normalized = .{ .reason = "Invalid judge output" };
                counts = run.counts[task];
                try t.evaluation.score(&counts, value.value, normalized);
            },
        };
        if (invalid) counts.invalid_outputs += 1;
        run.counts[task] = counts;
        try self.put(path, t.evaluation.Result{ .invalid_output = invalid, .run = run.id, .case_id = value.value.id, .case_revision = value.revision, .input_fingerprint = value.fingerprint, .prediction = normalized, .unavailable = normalized == null, .principal = self.service.context.principal, .actor = actor, .model = model, .revision = self.state.revision, .recorded_at = self.service.context.now_ms });
        try self.put(try self.evaluationDecidedKey(run.id, case_key), true);
        run.completed += 1;
        if (run.completed > run.case_count) return error.MemoryCorruptState;
        if (run.completed == run.case_count) run.phase = .completed;
        run.revision = self.state.revision;
    }
    fn evaluationVerdict(self: *Run, id: []const u8, request: t.evaluation.Submit, operation: *t.Operation) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        if (request.verdicts.len == 0 or request.verdicts.len > 32) return error.InvalidMemoryRequest;
        if (request.actor.len > 0) try t.validateId(request.actor);
        var run = try self.evaluationRun(id);
        if (run.mode != .external or run.phase != .pending) return error.MemoryEntityConflict;
        if (!valueEqual(?t.Model, run.judge, request.model)) return error.MemoryModelRevisionMismatch;
        if (!eql(u8, run.source_config_fingerprint, try hash(self.a, self.state.config))) return error.MemoryConfigurationChanged;
        for (request.verdicts) |verdict| {
            const value = try self.evaluationCase(run, verdict.case_id);
            if (verdict.case_revision != value.revision or !eql(u8, verdict.input_fingerprint, value.fingerprint)) return error.MemoryResourceChanged;
            if (!try self.evaluationCaseAvailable(value)) return error.MemoryForgotten;
            try self.recordEvaluationResult(&run, value, try hash(self.a, value.value.id), verdict.prediction.prediction(), verdict.invalid_output, request.actor, request.model);
        }
        try self.put(try key(self.a, "evaluation-run", id), run);
        operation.evaluation_id = id;
        if (request.actor.len > 0) operation.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
    }
    fn evaluatePolicy(self: *Run, value: t.evaluation.StoredCase) !t.evaluation.Prediction {
        const input = value.value.policy orelse try parse(t.evaluation.PolicyInput, self.a, try self.encode(value.value.input));
        switch (value.value.task) {
            .identity, .relationship => {
                var request = input.entity_candidates;
                request.kind = if (value.value.task == .identity) .identity else .relationship;
                const page = try parse(t.EntityCandidatePage, self.a, try self.entityCandidates(input.seed, request));
                if (page.retrieval != .indexed_lexical or self.state.config.semantic_discovery.enabled and page.semantic_discovery != .indexed) return .{};
                for (page.items) |candidate| if (eql(u8, candidate.entity.entity.id, input.target)) return .{ .admitted = true };
                if (!page.complete or page.semantic_discovery == .not_ready) return .{};
                return .{ .admitted = false };
            },
            .supersession => {
                // Candidate inclusion measures admission recall. A candidate
                // is not evidence for a four-way semantic replacement verdict.
                const page = try parse(t.SupersessionCandidatePage, self.a, try self.supersessionCandidates(input.seed, input.supersession_candidates));
                if (page.retrieval != .indexed) return .{};
                // Positive membership is conclusive even during a partial union;
                // absence requires every requested lane to be available.
                for (page.items) |candidate| if (eql(u8, candidate.view.memory.assertion.id, input.target)) return .{ .admitted = true };
                const mode = input.supersession_candidates.mode;
                if (mode == .semantic and page.semantic_discovery != .indexed or mode == .union_search and page.semantic_discovery == .not_ready) return .{};
                return if (page.truncated) .{} else .{ .admitted = false };
            },
            .retrieval => {
                const page = try parse(t.RecallResult, self.a, try self.recall(input.recall));
                const ranking = try self.a.alloc([]const u8, page.items.len);
                for (page.items, ranking) |hit, *id| id.* = hit.memory.assertion.id;
                return .{ .ranking = ranking };
            },
        }
    }
    fn evaluationQuantum(self: *Run, path: []const u8, pending: *t.Operation, receipt: *t.Operation) !void {
        const id = pending.evaluation_id orelse return error.MemoryCorruptState;
        var run = try self.evaluationRun(id);
        receipt.evaluation_id = id;
        if (run.phase != .pending) {
            pending.phase = if (run.phase == .completed) .succeeded else .cancelled;
        } else if (!eql(u8, run.source_config_fingerprint, try hash(self.a, self.state.config)) or run.mode == .policy and run.knowledge_revision != self.state.knowledge_revision) {
            run.phase = .cancelled;
            run.error_code = "MemoryConfigurationChanged";
            pending.phase = .cancelled;
        } else {
            const page = try self.evaluationIndexPage(run.root, run.cursor, 1, 1024 * 1024);
            if (page.items.len == 0) return error.MemoryCorruptState;
            const value = (try self.read(t.evaluation.StoredCase, page.items[0].value)) orelse return error.MemoryForgotten;
            const existing_result = try self.evaluationDecided(run, page.items[0].key);
            if (existing_result) {
                run.cursor = page.items[0].key;
                run.revision = self.state.revision;
                try self.put(try key(self.a, "evaluation-run", run.id), run);
                pending.next_retry_at = null;
                try self.put(path, pending.*);
                return;
            }
            const captured = try self.captureResources(evaluationCaseAvailable, .{value});
            if (captured.result and run.mode == .external) {
                pending.next_retry_at = self.service.context.now_ms +| 30000;
                try self.put(path, pending.*);
                return;
            }
            var prediction: ?t.evaluation.Prediction = null;
            var invalid_output = false;
            if (captured.result) {
                if (run.mode == .policy) {
                    const previous = self.state.config.semantic_discovery;
                    defer self.state.config.semantic_discovery = previous;
                    if (run.thresholds) |thresholds| self.state.config.semantic_discovery = thresholds;
                    prediction = try self.evaluatePolicy(value);
                } else if (run.mode == .managed) {
                    const Fence = struct {
                        run: *Run,
                        resources: ResourceSet,
                        fn complete(ptr: *anyopaque) !void {
                            const fence: *@This() = @ptrCast(@alignCast(ptr));
                            try fence.run.inferenceCompleted(fence.resources);
                        }
                    };
                    var fence: Fence = .{ .run = self, .resources = captured.resources };
                    const output = self.service.backend.evaluate.?(self.service.backend.ptr, self.a, self.state.config, run.judge.?, .{ .task = value.value.task, .input = value.value.input }, .{ .ptr = &fence, .complete_fn = Fence.complete }) catch |err| switch (err) {
                        error.MemoryUngroundedOutput => blk: {
                            invalid_output = true;
                            break :blk t.evaluation.JudgeOutput{};
                        },
                        else => return err,
                    };
                    try self.inferenceCompleted(captured.resources);
                    const clean = self.screenTyped(t.evaluation.JudgeOutput, output) catch |err| switch (err) {
                        error.MemorySecretDetected => blk: {
                            invalid_output = true;
                            break :blk t.evaluation.JudgeOutput{};
                        },
                        else => return err,
                    };
                    prediction = clean.prediction();
                } else return error.InvalidMemoryRequest;
            }
            try self.recordEvaluationResult(&run, value, page.items[0].key, prediction, invalid_output, "", run.judge);
            run.cursor = page.items[0].key;
            pending.phase = if (run.phase == .completed) .succeeded else .pending;
            pending.attempts = 0;
            pending.error_code = null;
            pending.next_retry_at = null;
        }
        run.revision = self.state.revision;
        try self.put(try key(self.a, "evaluation-run", id), run);
        try self.put(path, pending.*);
        if (pending.phase != .pending) try self.remove(try key(self.a, "pending", pending.id));
    }
    const NativeNodeMeta = struct { references: u64 = 0, retired_at: ?i64 = null };
    const root_gc_grace_ms = 5 * 60 * 1000;
    fn nodeReference(self: *Run, id: []const u8, add: bool) !void {
        const path = try std.fmt.allocPrint(self.a, "lineage-node-meta/{s}", .{id});
        var meta = (try self.rawRead(NativeNodeMeta, path)) orelse return error.MemoryCorruptState;
        if (add) {
            meta.references = try std.math.add(u64, meta.references, 1);
            meta.retired_at = null;
        } else {
            if (meta.references == 0) return error.MemoryCorruptState;
            meta.references -= 1;
            if (meta.references == 0) meta.retired_at = self.service.context.now_ms;
        }
        try self.stageChange(path, try self.encode(meta));
        if (meta.references == 0) try self.queueNodeReclamation(id);
    }
    fn queueNodeReclamation(self: *Run, id: []const u8) !void {
        const due: u64 = @intCast(@max(0, self.service.context.now_ms +| root_gc_grace_ms));
        try self.stageChange(try std.fmt.allocPrint(self.a, "lineage-gc/{d:0>20}/{s}", .{ due, id }), try self.encode(id));
    }
    fn publishRootReferences(self: *Run) !void {
        const changes = try self.a.dupe(Change, self.changes.items);
        for (changes) |change| {
            if (!std.mem.startsWith(u8, change.key, "lineage-head/") and !std.mem.startsWith(u8, change.key, "lineage-checkpoint/") and !std.mem.startsWith(u8, change.key, "evaluation-dataset/") and !std.mem.startsWith(u8, change.key, "evaluation-dataset-version/") and !std.mem.startsWith(u8, change.key, "evaluation-run/")) continue;
            const Root = struct { root: ?[]const u8 = null };
            const next = if (change.json) |json| (try std.json.parseFromSliceLeaky(Root, self.a, json, .{ .ignore_unknown_fields = true })).root else null;
            const old_row = try self.service.backend.get(self.service.backend.ptr, self.a, change.key);
            const old = if (old_row) |row| (try std.json.parseFromSliceLeaky(Root, self.a, row.json, .{ .ignore_unknown_fields = true })).root else null;
            if (optionalEqual(old, next)) continue;
            try self.persistentIndex().publish(next);
            if (next) |id| try self.nodeReference(id, true);
            if (old) |id| try self.nodeReference(id, false);
        }
    }
    fn reclaimNativeNodes(self: *Run) !void {
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "lineage-gc/", null, 32);
        for (page.rows) |row| {
            const position = row.key["lineage-gc/".len..];
            const slash = std.mem.indexOfScalar(u8, position, '/') orelse return error.MemoryCorruptState;
            const due = std.fmt.parseInt(i64, position[0..slash], 10) catch return error.MemoryCorruptState;
            if (due > self.service.context.now_ms) break;
            const id = try parse([]const u8, self.a, row.json);
            const meta_path = try std.fmt.allocPrint(self.a, "lineage-node-meta/{s}", .{id});
            const meta = (try self.rawRead(NativeNodeMeta, meta_path)) orelse {
                try self.remove(row.key);
                continue;
            };
            if (meta.references == 0 and (meta.retired_at == null or meta.retired_at.? +| root_gc_grace_ms > self.service.context.now_ms)) {
                try self.queueNodeReclamation(id);
            } else if (meta.references == 0) {
                const path = try std.fmt.allocPrint(self.a, "lineage-node/{s}", .{id});
                const Node = struct { left: ?[]const u8 = null, right: ?[]const u8 = null };
                const raw = (try self.service.backend.get(self.service.backend.ptr, self.a, path)) orelse return error.MemoryCorruptState;
                const node = try std.json.parseFromSliceLeaky(Node, self.a, raw.json, .{ .ignore_unknown_fields = true });
                if (node.left) |child| try self.nodeReference(child, false);
                if (node.right) |child| try self.nodeReference(child, false);
                try self.remove(path);
                try self.remove(meta_path);
            }
            try self.remove(row.key);
        }
    }
    fn lineageNodeGet(ptr: *anyopaque, _: A, path: []const u8) !?[]const u8 {
        const self: *Run = @ptrCast(@alignCast(ptr));
        if (self.change_positions.get(path)) |position| return self.changes.items[position].json;
        return if (try self.service.backend.get(self.service.backend.ptr, self.a, path)) |row| row.json else null;
    }
    fn lineageNodePut(ptr: *anyopaque, _: A, path: []const u8, json: []const u8) !void {
        const self: *Run = @ptrCast(@alignCast(ptr));
        // Content-addressed nodes deduplicate across roots and branches. Their
        // outgoing references are charged exactly once, on first publication.
        if (try lineageNodeGet(ptr, self.a, path) != null) return;
        try self.stageChange(path, json);
        const id = path["lineage-node/".len..];
        try self.stageChange(try std.fmt.allocPrint(self.a, "lineage-node-meta/{s}", .{id}), try self.encode(NativeNodeMeta{ .retired_at = self.service.context.now_ms }));
        self.state.persistent_roots = true;
        const Edges = struct { left: ?[]const u8 = null, right: ?[]const u8 = null };
        const edges = try std.json.parseFromSliceLeaky(Edges, self.a, json, .{ .ignore_unknown_fields = true });
        if (edges.left) |child| try self.nodeReference(child, true);
        if (edges.right) |child| try self.nodeReference(child, true);
        try self.queueNodeReclamation(id);
    }
    fn prepareLineage(self: *Run, action: Action, id: []const u8) !void {
        if (action == .advance and id.len > 0) {
            const pending = (try self.rawRead(t.Operation, try key(self.a, "operation", id))) orelse return error.MemoryNotFound;
            self.service.context.branch = pending.branch;
        }
        if (!self.state.config.features.lineage) {
            if (self.service.context.branch.len > 0) return error.MemoryCapabilityUnavailable;
            return;
        }
        self.lineage_index = .{ .a = self.a, .store = .{ .ptr = self, .get = lineageNodeGet, .put = lineageNodePut }, .prefix = "lineage-node/" };
        const branch = self.service.context.branch;
        if (eql(u8, branch, "main")) return error.InvalidMemoryRequest;
        if (branch.len > 0) {
            try t.validateId(branch);
            switch (action) {
                .create, .configure, .stage_embeddings, .curate_entity, .propose_entity, .review_entity, .propose_relationship, .review_relationship => return error.InvalidMemoryRequest,
                else => {},
            }
            if (action == .advance and id.len == 0) return error.InvalidMemoryRequest;
        }
        const head = (try self.rawRead(t.LineageHead, try key(self.a, "lineage-head", if (branch.len == 0) "main" else branch))) orelse blk: {
            if (branch.len > 0) return error.MemoryNotFound;
            break :blk t.LineageHead{ .id = "", .revision = self.state.revision, .owner = self.state.owner, .visibility = .team, .created_at = self.state.created_at, .complete = action == .create };
        };
        try self.authorize(head.owner, head.visibility);
        if (branch.len > 0 and mutates(action)) try self.authorize(head.owner, .private);
        if (!head.complete and branch.len > 0) return error.MemoryCapabilityUnavailable;
        self.lineage_head = head;
        self.lineage_raw = action == .advance and id.len == 0;
    }
    fn lineageRootPut(self: *Run, path: []const u8, json: []const u8) !void {
        var head = self.lineage_head orelse return;
        var reference: []const u8 = path;
        if (std.mem.startsWith(u8, path, "assertion/")) {
            const record = try parse(t.StoredAssertion, self.a, json);
            reference = try historyKey(self.a, record.assertion.id, record.revision);
            // Legacy snapshots may predate the immutable history row.
            if (try self.rawRead(t.StoredAssertion, reference) == null) try self.stageChange(reference, json);
            head.root = try self.lineage_index.?.put(head.root, reference, reference);
            const group_path = try groupKey(self.a, record.assertion, record.owner);
            head.root = try self.lineage_index.?.put(head.root, group_path, path);
        } else if (std.mem.startsWith(u8, path, "entity/")) {
            const record = try parse(t.EntityRecord, self.a, json);
            reference = try std.fmt.allocPrint(self.a, "lineage-entity/{s}/{d:0>20}", .{ path["entity/".len..], record.revision });
            try self.stageChange(reference, json);
        }
        head.root = try self.lineage_index.?.put(head.root, path, reference);
        self.lineage_head = head;
    }
    fn publishLineage(self: *Run, action: Action) !void {
        if (self.lineage_head == null) return;
        const was_complete = self.lineage_head.?.complete;
        if (!self.lineage_raw) {
            const changes = try self.a.dupe(Change, self.changes.items);
            for (changes) |change| if (change.json) |json| {
                if (std.mem.startsWith(u8, change.key, "assertion/") or std.mem.startsWith(u8, change.key, "evidence/") or std.mem.startsWith(u8, change.key, "entity/")) try self.lineageRootPut(change.key, json);
            };
        } else if (!self.lineage_head.?.complete) {
            // One bounded migration quantum; new writes also update the root.
            // Native primary keys, not a process cache, own resumption.
            const lanes = [_][]const u8{ "assertion/", "entity/", "evidence/", "history/" };
            if (self.lineage_head.?.migration_lane >= lanes.len) return error.MemoryCorruptState;
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, lanes[self.lineage_head.?.migration_lane], self.lineage_head.?.cursor, 128);
            for (page.rows) |row| try self.lineageRootPut(row.key, row.json);
            self.lineage_head.?.cursor = page.next;
            if (page.next == null) self.lineage_head.?.migration_lane += 1;
            self.lineage_head.?.complete = self.lineage_head.?.migration_lane == lanes.len;
        }
        if (self.lineage_raw and was_complete and action != .create) return;
        self.lineage_head.?.revision = self.state.revision;
        try self.put(try key(self.a, "lineage-head", if (self.service.context.branch.len == 0) "main" else self.service.context.branch), self.lineage_head.?);
    }
    fn forkLineage(self: *Run, request: t.LineageFork) !void {
        if (!self.state.config.features.lineage) return error.MemoryCapabilityUnavailable;
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try t.validateId(request.id);
        if (eql(u8, request.id, "main")) return error.InvalidMemoryRequest;
        const path = try key(self.a, "lineage-head", request.id);
        if (try self.rawRead(t.LineageHead, path) != null) return error.MemoryEntityConflict;
        const parent = (try self.rawRead(t.LineageHead, try key(self.a, "lineage-head", if (request.from.len == 0) "main" else request.from))) orelse return error.MemoryNotFound;
        try self.authorize(parent.owner, parent.visibility);
        if (!parent.complete) return error.MemoryCapabilityUnavailable;
        var root = parent.root;
        var revision = parent.revision;
        if (request.checkpoint) |id| {
            const checkpoint = (try self.rawRead(t.LineageCheckpoint, try key(self.a, "lineage-checkpoint", id))) orelse return error.MemoryNotFound;
            try self.authorize(checkpoint.owner, checkpoint.visibility);
            if (!eql(u8, checkpoint.branch, request.from)) return error.InvalidMemoryRequest;
            root = checkpoint.root;
            revision = checkpoint.revision;
        }
        try self.queueSemanticBranch(request.id);
        try self.put(path, t.LineageHead{ .id = request.id, .root = root, .revision = self.state.revision, .owner = self.service.context.principal, .visibility = request.visibility, .created_at = self.service.context.now_ms, .parent = request.from, .inherited_revision = revision });
    }
    fn checkpointLineage(self: *Run, request: t.LineageCheckpointRequest) !void {
        const head = self.lineage_head orelse return error.MemoryCapabilityUnavailable;
        if (!head.complete) return error.MemoryCapabilityUnavailable;
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try t.validateId(request.id);
        const path = try key(self.a, "lineage-checkpoint", request.id);
        if (try self.rawRead(t.LineageCheckpoint, path) != null) return error.MemoryEntityConflict;
        try self.put(path, t.LineageCheckpoint{ .id = request.id, .branch = self.service.context.branch, .root = head.root, .revision = head.revision, .owner = self.service.context.principal, .visibility = head.visibility, .created_at = self.service.context.now_ms });
    }
    fn revertLineage(self: *Run, request: t.LineageRevert) !void {
        var head = self.lineage_head orelse return error.MemoryCapabilityUnavailable;
        if (!head.complete) return error.MemoryCapabilityUnavailable;
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        const checkpoint = (try self.rawRead(t.LineageCheckpoint, try key(self.a, "lineage-checkpoint", request.checkpoint))) orelse return error.MemoryNotFound;
        try self.authorize(checkpoint.owner, checkpoint.visibility);
        if (!eql(u8, checkpoint.branch, self.service.context.branch)) return error.InvalidMemoryRequest;
        head.root = checkpoint.root;
        self.lineage_head = head;
        if (self.state.config.semantic_discovery.enabled) {
            try self.put(try self.semanticPath("entity-semantic-index"), t.SemanticIndex{});
            try self.queueSemanticBranch(self.service.context.branch);
        }
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
    }
    const FeedRecord = struct { event: t.SpaceChange, audiences: []const []const u8 };
    fn feedTail(self: *Run) ![]const u8 {
        return std.fmt.allocPrint(self.a, "{d:0>20}/9999999999", .{self.state.revision});
    }
    fn feedAudience(self: *Run, owner: []const u8, visibility: t.Visibility, curator: bool) ![]const u8 {
        if (visibility == .team) return if (curator) "curator-team" else "team";
        return std.fmt.allocPrint(self.a, "{s}/{s}", .{ if (curator) "curator-private" else "private", try hash(self.a, owner) });
    }
    fn feedEmit(self: *Run, items: *std.ArrayList(FeedRecord), kind: t.ChangeKind, target: ?t.ChangeTarget, owner: []const u8, visibility: t.Visibility, curator: bool) !void {
        const position = try std.fmt.allocPrint(self.a, "{d:0>20}/{d:0>10}", .{ self.state.revision, items.items.len });
        const audiences = try self.a.alloc([]const u8, 1);
        const private_branch = if (self.lineage_head) |head| self.service.context.branch.len > 0 and head.visibility == .private else false;
        audiences[0] = try self.feedAudience(if (private_branch) self.lineage_head.?.owner else owner, if (private_branch) .private else visibility, curator);
        try items.append(self.a, .{ .event = .{ .branch = self.service.context.branch, .id = position, .kind = kind, .target = target, .revision = self.state.revision, .recorded_at = self.service.context.now_ms }, .audiences = audiences });
    }
    fn publishFeed(self: *Run, operation: t.Operation) !void {
        // Inspect the final staged values, so retried planners and intermediate
        // revisions cannot publish notifications for work that rolled back.
        var items: std.ArrayList(FeedRecord) = .empty;
        var invalidated = eql(u8, operation.action, "curate_entity") or eql(u8, operation.action, "forget");
        for (self.changes.items) |change| {
            if (change.json == null) {
                if (std.mem.startsWith(u8, change.key, "assertion/") or std.mem.startsWith(u8, change.key, "evidence/") or std.mem.startsWith(u8, change.key, "entity/")) invalidated = true;
                continue;
            }
            if (eql(u8, operation.action, "advance") and std.mem.startsWith(u8, change.key, "resource-watch/")) invalidated = true;
            const json = change.json.?;
            if (std.mem.startsWith(u8, change.key, "assertion/")) {
                const record = try parse(t.StoredAssertion, self.a, json);
                if (record.revision != self.state.revision) continue;
                try self.feedEmit(&items, if (record.stale) .context_invalidated else switch (record.lifecycle) {
                    .superseded => .assertion_superseded,
                    .disputed => .assertion_disputed,
                    .retracted => .assertion_retracted,
                    else => .assertion_changed,
                }, .{ .kind = .assertion, .id = record.assertion.id, .revision = record.revision }, record.owner, record.assertion.visibility, false);
            } else if (std.mem.startsWith(u8, change.key, "evidence/")) {
                const record = try parse(t.StoredEvidence, self.a, json);
                if (record.revision != self.state.revision) continue;
                try self.feedEmit(&items, if (record.forgotten) .context_invalidated else .evidence_observed, .{ .kind = .evidence, .id = record.evidence.id, .revision = record.revision }, record.owner, record.evidence.visibility, false);
            } else if (std.mem.startsWith(u8, change.key, "entity/")) {
                const record = try parse(t.EntityRecord, self.a, json);
                if (record.revision != self.state.revision) continue;
                try self.feedEmit(&items, .entity_changed, .{ .kind = .entity, .id = record.entity.id, .revision = record.revision }, record.owner, record.entity.visibility, false);
            }
        }
        if (operation.evaluation_id != null) try self.feedEmit(&items, .evaluation_changed, null, operation.owner, .team, true);
        if (eql(u8, operation.action, "fork_lineage") or eql(u8, operation.action, "checkpoint_lineage") or eql(u8, operation.action, "revert_lineage")) try self.feedEmit(&items, .lineage_changed, null, operation.owner, .team, false);
        if (operation.proposal_id) |id| {
            // Proposal access is narrower than ordinary team record access.
            const supersession = eql(u8, operation.action, "propose_supersession") or eql(u8, operation.action, "review_supersession");
            const relationship = eql(u8, operation.action, "propose_relationship") or eql(u8, operation.action, "review_relationship");
            var proposal_kind: t.ChangeKind = if (std.mem.startsWith(u8, operation.action, "propose_")) .proposal_opened else .proposal_decided;
            if (proposal_kind == .proposal_decided) {
                const open = if (supersession) ((try self.read(t.SupersessionProposal, try key(self.a, "supersession-proposal", id))) orelse return error.MemoryCorruptState).verdict == null else if (relationship) ((try self.read(t.RelationshipProposal, try key(self.a, "relationship-proposal", id))) orelse return error.MemoryCorruptState).state == .open else blk: {
                    break :blk if (try self.identityProposal(id)) |proposal| proposal.state == .open else false;
                };
                if (open) proposal_kind = .proposal_deferred;
            }
            try self.feedEmit(&items, proposal_kind, .{ .kind = if (supersession) .supersession_proposal else if (relationship) .relationship_proposal else .entity_proposal, .id = id, .revision = self.state.revision }, operation.owner, .team, true);
        }
        if (invalidated) {
            // Erasure and native source/TTL fences affect every retained root,
            // even when requested from a private branch. Never expose a retired
            // target ID merely to explain which cached context must refresh.
            const saved_branch = self.service.context.branch;
            self.service.context.branch = "";
            defer self.service.context.branch = saved_branch;
            try self.feedEmit(&items, .context_invalidated, null, operation.owner, .team, false);
        }
        if (eql(u8, operation.action, "configure") or eql(u8, operation.action, "create")) try self.feedEmit(&items, .configuration_changed, null, operation.owner, .team, false);
        for (items.items) |item| {
            try self.put(try std.fmt.allocPrint(self.a, "change-log/{s}", .{item.event.id}), item);
            for (item.audiences) |audience| try self.put(try std.fmt.allocPrint(self.a, "change-audience/{s}/{s}", .{ audience, item.event.id }), item.event.id);
        }
        // Bounded prefix reclamation. Expiry is checked on reads even while
        // physical cleanup is behind. The monotonic cutoff prevents a later
        // retention increase from resurrecting previously expired events.
        self.state.change_min_recorded_at = @max(self.state.change_min_recorded_at, self.service.context.now_ms -| @as(i64, @intCast(self.state.config.change_retention_ms)));
        self.state.next_change_expiry_at = if (items.items.len > 0) self.service.context.now_ms +| @as(i64, @intCast(self.state.config.change_retention_ms)) else null;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "change-log/", null, 32);
        for (page.rows) |row| {
            const record = try parse(FeedRecord, self.a, row.json);
            if (record.event.recorded_at > self.state.change_min_recorded_at) {
                const due = record.event.recorded_at +| @as(i64, @intCast(self.state.config.change_retention_ms));
                self.state.next_change_expiry_at = @min(self.state.next_change_expiry_at orelse due, due);
                break;
            }
            try self.remove(row.key);
            for (record.audiences) |audience| try self.remove(try std.fmt.allocPrint(self.a, "change-audience/{s}/{s}", .{ audience, record.event.id }));
            if (std.mem.order(u8, record.event.id, self.state.change_floor) == .gt) self.state.change_floor = record.event.id;
            if (page.next != null) self.state.next_change_expiry_at = self.service.context.now_ms;
        }
    }
    fn feedCursor(self: *Run, position: []const u8, version: u8, issued_at: i64) ![]const u8 {
        const scope = try inputFingerprint(self.a, .{ "changes/v1", self.state.table_id, self.state.database, self.state.namespace, self.state.space, self.state.created_at, self.service.context.branch, self.service.context.principal, self.service.context.admin, self.service.context.curator, position, issued_at }, self.state.input_digest_key, version);
        return std.fmt.allocPrint(self.a, "{d}:{s}:{s}:{d}", .{ version, scope, position, issued_at });
    }
    fn feedAvailable(self: *Run, event: t.SpaceChange) !bool {
        const saved_branch = self.service.context.branch;
        const saved_head = self.lineage_head;
        const saved_raw = self.lineage_raw;
        defer {
            self.service.context.branch = saved_branch;
            self.lineage_head = saved_head;
            self.lineage_raw = saved_raw;
        }
        if (self.state.config.features.lineage) {
            const head = (try self.rawRead(t.LineageHead, try key(self.a, "lineage-head", if (event.branch.len == 0) "main" else event.branch))) orelse return false;
            self.authorize(head.owner, head.visibility) catch return false;
            self.service.context.branch = event.branch;
            self.lineage_head = head;
            self.lineage_raw = false;
        }
        const target = event.target orelse return true;
        switch (target.kind) {
            .assertion => {
                const record = (try self.assertion(target.id)) orelse return false;
                self.authorize(record.owner, record.assertion.visibility) catch return false;
                return self.retained(record, 0);
            },
            .evidence => {
                const record = (try self.evidence(target.id)) orelse return false;
                self.authorize(record.owner, record.evidence.visibility) catch return false;
                return self.evidenceRetained(record);
            },
            .entity => return (try self.visibleCanonicalEntity(target.id, null)) != null,
            .entity_proposal => {
                self.service.context.requireGrant(.curator) catch return false;
                const binding = (try self.read(ProposalBinding, try key(self.a, "proposal-input", target.id))) orelse return false;
                return self.proposalInputsRetained(binding);
            },
            .supersession_proposal => {
                self.service.context.requireGrant(.curator) catch return false;
                const proposal = (try self.read(t.SupersessionProposal, try key(self.a, "supersession-proposal", target.id))) orelse return false;
                return self.supersessionProposalRetained(proposal);
            },
            .relationship_proposal => {
                self.service.context.requireGrant(.curator) catch return false;
                _ = self.publicRelationshipProposal(target.id) catch |err| switch (err) {
                    error.MemoryNotFound, error.Forbidden, error.MemoryForgotten => return false,
                    else => return err,
                };
                return true;
            },
            else => return false,
        }
    }
    fn spaceChanges(self: *Run, request: t.ChangeRequest) ![]const u8 {
        if (request.limit == 0 or request.limit > 256) return error.InvalidMemoryRequest;
        const tail = try self.feedTail();
        var position: []const u8 = if (request.start == .latest) tail else self.state.change_floor;
        if (request.cursor) |cursor| {
            var parts = std.mem.splitScalar(u8, cursor, ':');
            const version = std.fmt.parseInt(u8, parts.next() orelse return error.InvalidMemoryCursor, 10) catch return error.InvalidMemoryCursor;
            _ = parts.next() orelse return error.InvalidMemoryCursor;
            position = parts.next() orelse return error.InvalidMemoryCursor;
            const issued_at = std.fmt.parseInt(i64, parts.next() orelse return error.InvalidMemoryCursor, 10) catch return error.InvalidMemoryCursor;
            if (parts.next() != null or version > 1 or issued_at > self.service.context.now_ms or !eql(u8, cursor, try self.feedCursor(position, version, issued_at))) return error.InvalidMemoryCursor;
            if (issued_at <= @max(self.state.change_min_recorded_at, self.service.context.now_ms -| @as(i64, @intCast(self.state.config.change_retention_ms)))) return error.MemoryCursorStale;
            if (std.mem.order(u8, position, self.state.change_floor) == .lt) return error.MemoryCursorStale;
            if (std.mem.order(u8, position, tail) == .gt) return error.InvalidMemoryCursor;
        }
        const Candidate = struct { position: []const u8, path: []const u8 };
        var feed_candidates: std.ArrayList(Candidate) = .empty;
        var audiences: std.ArrayList([]const u8) = .empty;
        if (self.service.context.admin) {
            try self.requireAdmin();
            try audiences.append(self.a, "");
        } else {
            try audiences.append(self.a, "team");
            try audiences.append(self.a, try self.feedAudience(self.service.context.principal, .private, false));
            if (self.service.context.curator) {
                try audiences.append(self.a, "curator-team");
                try audiences.append(self.a, try self.feedAudience(self.service.context.principal, .private, true));
            }
        }
        for (audiences.items) |audience| {
            const prefix = if (audience.len == 0) "change-log/" else try std.fmt.allocPrint(self.a, "change-audience/{s}/", .{audience});
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, if (position.len == 0) null else try std.fmt.allocPrint(self.a, "{s}{s}", .{ prefix, position }), @as(u32, request.limit) + 1);
            for (page.rows) |row| {
                const next = row.key[prefix.len..];
                if (std.mem.order(u8, next, tail) == .gt) continue;
                try feed_candidates.append(self.a, .{ .position = next, .path = try std.fmt.allocPrint(self.a, "change-log/{s}", .{next}) });
            }
        }
        std.mem.sort(Candidate, feed_candidates.items, {}, struct {
            fn less(_: void, x: Candidate, y: Candidate) bool {
                return std.mem.order(u8, x.position, y.position) == .lt;
            }
        }.less);
        var items: std.ArrayList(t.SpaceChange) = .empty;
        var scanned: usize = 0;
        const cutoff = @max(self.state.change_min_recorded_at, self.service.context.now_ms -| @as(i64, @intCast(self.state.config.change_retention_ms)));
        for (feed_candidates.items) |candidate| {
            if (eql(u8, candidate.position, position)) continue;
            if (scanned == request.limit) break;
            scanned += 1;
            position = candidate.position;
            const record = (try self.read(FeedRecord, candidate.path)) orelse continue;
            if (record.event.recorded_at <= cutoff) continue;
            var event = record.event;
            if (!try self.feedAvailable(event)) {
                event.kind = .context_invalidated;
                event.target = null;
                event.branch = "";
            }
            try items.append(self.a, event);
        }
        const caught_up = feed_candidates.items.len <= scanned;
        if (caught_up) position = tail;
        return self.encode(t.ChangePage{ .items = items.items, .next_cursor = try self.feedCursor(position, if (self.state.input_digest_key.len == 64) 1 else 0, self.service.context.now_ms), .caught_up = caught_up, .revision = self.state.revision, .retained_from = self.state.change_floor, .retained_after = cutoff });
    }

    fn screenTyped(self: *Run, comptime T: type, value: T) !T {
        const result = try screening.typed(T, self.a, value, self.state.config.intake_screening);
        self.intake_report.add(result.report);
        return result.value;
    }
    fn encode(self: *Run, value: anytype) ![]const u8 {
        return std.json.Stringify.valueAlloc(self.a, value, .{});
    }
    fn put(self: *Run, path: []const u8, value: anytype) !void {
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
        const json = try self.encode(value);
        if (self.state.config.semantic_discovery.enabled) {
            if (comptime @TypeOf(value) == t.EntityRecord) {
                if (std.mem.startsWith(u8, path, "entity/")) try self.semanticDirty(value.entity.id);
            }
            if (comptime @TypeOf(value) == t.StoredAssertion) {
                if (std.mem.startsWith(u8, path, "assertion/")) {
                    try self.semanticDirty(value.assertion.subject);
                    try self.semanticInvalidateAssertion(value.assertion.id);
                    if (self.objectTyped(value.assertion.predicate)) try self.semanticDirty(value.assertion.value);
                }
            }
        }
        try self.stageChange(path, json);
        if (comptime @TypeOf(value) == t.StoredAssertion) {
            if (self.service.backend.search_assertions != null and std.mem.startsWith(u8, path, "assertion/")) try self.updateRecallProjection(self.state.config, value);
        }
    }
    fn remove(self: *Run, path: []const u8) !void {
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
        try self.stageChange(path, null);
    }
    fn stageChange(self: *Run, path: []const u8, json: ?[]const u8) !void {
        if (eql(u8, path, "entity-policy") or std.mem.startsWith(u8, path, "entity-authority/head/")) {
            self.clearIdentityCaches();
        }
        _ = self.assertion_rows.remove(path);
        _ = self.evidence_rows.remove(path);
        _ = self.entity_rows.remove(path);
        _ = self.projection_rows.remove(path);
        if (self.change_positions.get(path)) |position| {
            self.changes.items[position].json = json;
        } else {
            const owned = try self.a.dupe(u8, path);
            try self.change_positions.put(self.a, owned, self.changes.items.len);
            try self.changes.append(self.a, .{ .key = owned, .json = json });
        }
        const owned = self.changes.items[self.change_positions.get(path).?].key;
        var entry = self.change_ranges.getEntryFor(owned);
        if (json == null) {
            entry.set(null);
        } else if (entry.node == null) {
            entry.set(try self.a.create(ChangeTree.Node));
        }
    }
    // Rollback must restore the lookup index and every decoded overlay value.
    fn resetChanges(self: *Run, checkpoint: []const Change) !void {
        self.changes.clearRetainingCapacity();
        self.change_positions.clearRetainingCapacity();
        self.change_ranges = .{};
        self.assertion_rows.clearRetainingCapacity();
        self.evidence_rows.clearRetainingCapacity();
        self.entity_rows.clearRetainingCapacity();
        self.projection_rows.clearRetainingCapacity();
        self.clearIdentityCaches();
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
        for (checkpoint) |change| try self.stageChange(change.key, change.json);
    }
    fn snapshotPath(self: *Run, path: []const u8) !?[]const u8 {
        if (self.change_positions.contains(path)) return path;
        if (self.lineage_head) |head| if (head.complete and !self.lineage_raw) {
            if (std.mem.startsWith(u8, path, "assertion/") or std.mem.startsWith(u8, path, "evidence/") or std.mem.startsWith(u8, path, "entity/") or std.mem.startsWith(u8, path, "history/")) return self.lineage_index.?.get(head.root, path);
        };
        return path;
    }
    fn read(self: *Run, comptime T: type, path: []const u8) !?T {
        if (comptime T == t.StoredAssertion or T == t.StoredEvidence or T == t.EntityRecord) return self.rawRead(T, (try self.snapshotPath(path)) orelse return null);
        return self.rawRead(T, path);
    }
    fn rawRead(self: *Run, comptime T: type, path: []const u8) !?T {
        if (self.change_positions.get(path)) |position| return if (self.changes.items[position].json) |json| try self.parseRead(T, path, json) else null;
        if (self.backend_rows.getEntry(path)) |entry| return if (entry.value_ptr.*) |json| try self.parseRead(T, path, json) else null;
        const row = try self.service.backend.get(self.service.backend.ptr, self.a, path);
        const json = if (row) |value| value.json else null;
        try self.backend_rows.put(self.a, try self.a.dupe(u8, path), json);
        return if (json) |value| try self.parseRead(T, path, value) else null;
    }
    fn prefetch(self: *Run, paths: []const []const u8) !void {
        var batch: std.ArrayList([]const u8) = .empty;
        var seen: ResourceSet = .empty;
        for (paths) |requested| {
            const path = (try self.snapshotPath(requested)) orelse continue;
            if (self.change_positions.contains(path) or self.backend_rows.contains(path) or seen.contains(path)) continue;
            try seen.put(self.a, path, {});
            try batch.append(self.a, path);
            // Hydration is speculative. Wide support sets must still reach
            // ordinary policy work/dependency budgets before loading a corpus.
            if (batch.items.len == 256) break;
        }
        var offset: usize = 0;
        while (offset < batch.items.len) {
            try self.service.context.check();
            const chunk = batch.items[offset..@min(offset + 64, batch.items.len)];
            if (self.service.backend.get_many) |get_many| {
                const rows = try get_many(self.service.backend.ptr, self.a, chunk);
                if (rows.len != chunk.len) return error.MemoryCorruptState;
                for (chunk, rows) |path, row| {
                    if (row) |value| if (!eql(u8, path, value.key)) return error.MemoryCorruptState;
                    try self.backend_rows.put(self.a, try self.a.dupe(u8, path), if (row) |value| value.json else null);
                }
            } else {
                for (chunk) |path| {
                    const row = try self.service.backend.get(self.service.backend.ptr, self.a, path);
                    try self.backend_rows.put(self.a, try self.a.dupe(u8, path), if (row) |value| value.json else null);
                }
            }
            offset += chunk.len;
        }
    }
    fn decodedRows(self: *Run, comptime T: type) *std.StringHashMapUnmanaged(T) {
        if (T == t.StoredAssertion) return &self.assertion_rows;
        if (T == t.StoredEvidence) return &self.evidence_rows;
        if (T == t.EntityRecord) return &self.entity_rows;
        if (T == t.EntityProjection) return &self.projection_rows;
        @compileError("unsupported decoded memory record");
    }
    fn trackTime(self: *Run, time: ?i64) void {
        if (!self.temporal_tracking) return;
        if (time) |at| if (at > self.service.context.now_ms and (self.temporal_boundary == null or at < self.temporal_boundary.?)) {
            self.temporal_boundary = at;
        };
    }
    fn parseRead(self: *Run, comptime T: type, path: []const u8, json: []const u8) !T {
        const value = if (comptime T == t.StoredAssertion or T == t.StoredEvidence or T == t.EntityRecord or T == t.EntityProjection) blk: {
            const rows = self.decodedRows(T);
            if (rows.get(path)) |cached| break :blk cached;
            const decoded = try parse(T, self.a, json);
            try rows.put(self.a, try self.a.dupe(u8, path), decoded);
            break :blk decoded;
        } else try parse(T, self.a, json);
        // These are read effects, not decode effects: replay them on cache hits.

        if (comptime T == t.StoredAssertion) {
            if (self.temporal_tracking) try self.semantic_assertions.put(self.a, try hash(self.a, value.assertion.id), {});
            self.trackTime(value.assertion.valid_from);
            self.trackTime(value.assertion.valid_to);
            self.trackTime(value.superseded_valid_at);
        }
        if (comptime T == t.StoredEvidence) self.trackTime(self.evidenceExpiry(value));
        return value;
    }
    fn trackResource(self: *Run, snapshot: prov.Snapshot) void {
        // An unavailable support branch may change the rendered view without
        // a memory write. Revisit the projection on the ordinary poll cadence.
        if (self.temporal_tracking and snapshot.state == .denied) self.temporal_denied = true;
        if (snapshot.state == .denied or snapshot.state == .changed) self.trackTime(self.service.context.now_ms +| resource_check_interval_ms);
    }
    fn requireAdmin(self: *Run) !void {
        self.administrative_view = true;
        try self.service.context.requireGrant(.admin);
    }
    fn requiredGrant(self: *Run, action: Action, id: []const u8, body: []const u8) !t.RequiredGrant {
        if (action == .advance) {
            if (id.len == 0) return .admin;
            const pending = (try self.read(t.Operation, try key(self.a, "operation", id))) orelse return error.MemoryNotFound;
            return operationGrant(pending);
        }
        if (action == .forget) {
            const request = try parse(t.Forget, self.a, body);
            if (request.all or request.source_ids.len > 0) return .admin;
        }
        return actionGrant(action);
    }
    fn reasonKey(self: *Run, id: []const u8, revision: u64) ![]const u8 {
        return std.fmt.allocPrint(self.a, "reason-binding/{s}/{d:0>20}", .{ try hash(self.a, id), revision });
    }
    fn reasonBindings(self: *Run, record: t.StoredAssertion) !ReasonBindings {
        return (try self.read(ReasonBindings, try self.reasonKey(record.assertion.id, record.revision))) orelse ReasonBindings{};
    }
    const RecordView = enum { visible, retained };
    fn decisionVisible(self: *Run, path: []const u8, depth: u8) anyerror!bool {
        return self.decisionAvailable(path, depth, .visible);
    }
    fn decisionAvailable(self: *Run, path: []const u8, depth: u8, comptime view: RecordView) anyerror!bool {
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        const cache_key = try self.viewKey(if (view == .visible) "decision" else "decision-retention", path, 0, null, null, depth);
        if (self.views.get(cache_key)) |result| return result;
        try self.supportStep();
        const result = try self.decisionAvailableInner(path, depth, view);
        try self.views.put(self.a, cache_key, result);
        return result;
    }
    fn decisionAvailableInner(self: *Run, path: []const u8, depth: u8, comptime view: RecordView) anyerror!bool {
        const decision = (try self.read(DecisionProvenance, path)) orelse return false;
        if (decision.input_version != judgment_input_version) return false;
        if (try self.read(bool, try key(self.a, "forgotten-assertion", decision.assertion_id)) != null) return false;
        if (view == .visible) {
            if (!try self.generationVisible("assertion", decision.assertion_id)) return false;
        } else if (!try self.generationRetained("assertion", decision.assertion_id)) return false;
        // Legacy decisions without an immutable context cannot establish what
        // supported the payload actually supplied to inference. Fail closed.
        if (!try self.causalContextAvailable(decision.context orelse return false, view == .visible)) return false;
        for (decision.evidence_ids) |id| if (!try self.reasonSupport(&.{.{ .id = id }}, view)) return false;
        if (decision.incoming_reason) |parent| if (!try self.decisionAvailable(parent, depth + 1, view)) return false;
        for (decision.inputs) |input| {
            try self.supportStep();
            const record = (try self.read(t.StoredAssertion, try historyKey(self.a, input.id, input.revision))) orelse return false;
            self.authorize(record.owner, record.assertion.visibility) catch return false;
            if (!try self.retained(record, 0) or (view == .visible and !try self.identityVisible(record.assertion))) return false;
            if (input.reasons.assertion) |parent| if (!try self.decisionAvailable(parent, depth + 1, view)) return false;
            if (input.reasons.disposition) |parent| if (!try self.decisionAvailable(parent, depth + 1, view)) return false;
            if (input.correction_evidence.len > 0 and !try self.reasonSupport(input.correction_evidence, view)) return false;
        }
        return true;
    }
    fn storeDecision(self: *Run, claim: t.Assertion, prior: []const t.StoredAssertion, incoming_reason: ?[]const u8) ![]const u8 {
        var context: JudgmentContext = .{};
        try self.captureClaimInputs(&context, claim, claim.visibility, .retained);
        var inputs: std.ArrayList(DecisionInput) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (prior) |record| {
            try self.captureJudgmentDependency(&context, .{ .resource = try self.resourceRef(.assertion, record.assertion.id, record.revision), .evidence = record.assertion.evidence }, claim.visibility, 0, .retained);
            // Every branch in this public payload was supplied to the model,
            // not just the branch currently sufficient to retain the fact.
            try self.captureRecordInputs(&context, record, claim.visibility);
            var reasons = try self.reasonBindings(record);
            if (record.assertion.reconciliation_reason == null) reasons.assertion = null;
            const correction_reason = if (record.explicit_disposition) |baseline| baseline.reason != null else false;
            if (record.reason == null and !correction_reason) reasons.disposition = null;
            const correction_evidence = if (correction_reason) record.explicit_disposition.?.provenance else null;
            try inputs.append(self.a, .{ .id = record.assertion.id, .revision = record.revision, .reasons = reasons, .correction_evidence = if (correction_evidence) |provenance| provenance.evidence else &.{} });
            try seen.put(self.a, record.assertion.id, {});
        }
        var parents: std.ArrayList([]const u8) = .empty;
        try parents.appendSlice(self.a, claim.depends_on);
        for (claim.support_sets) |support| try parents.appendSlice(self.a, support.depends_on);
        for (parents.items) |id| {
            try self.supportStep();
            if (seen.contains(id)) continue;
            const record = (try self.assertion(id)) orelse return error.MemoryDependencyMissing;
            try inputs.append(self.a, .{ .id = id, .revision = record.revision, .reasons = .{} });
            try seen.put(self.a, id, {});
        }
        const ids = try self.judgmentEvidence(&context, claim.visibility);
        const path = try std.fmt.allocPrint(self.a, "decision/{s}/{d:0>20}", .{ try hash(self.a, claim.id), self.state.revision });
        try self.put(path, DecisionProvenance{ .input_version = judgment_input_version, .principal = self.service.context.principal, .recorded_at = self.service.context.now_ms, .config_generation = self.state.config.generation, .assertion_id = claim.id, .evidence_ids = ids, .inputs = inputs.items, .context = context.dependencies.items, .incoming_reason = incoming_reason });
        return path;
    }
    fn reasonSupport(self: *Run, refs: []const t.EvidenceRef, comptime view: RecordView) !bool {
        return if (view == .visible) self.supportVisible(refs, &.{}, 0) else self.retainedSupport(refs, &.{}, 0);
    }
    fn publicRecord(self: *Run, value: t.StoredAssertion) !t.StoredAssertion {
        return self.projectRecord(value, .visible);
    }
    fn retainedRecord(self: *Run, value: t.StoredAssertion) !t.StoredAssertion {
        const previous = self.retention_maintenance;
        self.retention_maintenance = true;
        defer self.retention_maintenance = previous;
        return self.projectRecord(value, .retained);
    }
    fn projectRecord(self: *Run, value: t.StoredAssertion, comptime view: RecordView) !t.StoredAssertion {
        var record = value;
        const reasons = try self.reasonBindings(record);
        if (reasons.assertion) |path| if (!try self.decisionAvailable(path, 0, view)) {
            record.assertion.reconciliation_reason = null;
        };
        if (reasons.disposition) |path| if (!try self.decisionAvailable(path, 0, view)) {
            record.reason = null;
            if (record.explicit_disposition) |*baseline| baseline.reason = null;
        };
        if (record.explicit_disposition) |baseline| if (baseline.provenance) |provenance| {
            if (provenance.forgotten or (provenance.evidence.len > 0 and !try self.reasonSupport(provenance.evidence, view))) {
                var redacted = baseline;
                var attribution = provenance;
                attribution.forgotten = true;
                attribution.evidence = &.{};
                redacted.provenance = attribution;
                redacted.reason = null;
                record.explicit_disposition = redacted;
                record.reason = null;
            }
        };
        if (view == .visible) {
            try self.publicClaim(&record.assertion);
            record.conflicts_with = try self.publicAssertionRefs(record.conflicts_with);
            if (record.superseded_by) |id| if (!try self.publicAssertionRef(id)) {
                record.superseded_by = null;
            };
            if (record.explicit_disposition) |*baseline| {
                baseline.conflicts_with = try self.publicAssertionRefs(baseline.conflicts_with);
                if (baseline.superseded_by) |id| if (!try self.publicAssertionRef(id)) {
                    baseline.superseded_by = null;
                };
            }
        }
        return record;
    }
    fn publicClaim(self: *Run, claim: *t.Assertion) !void {
        var branches: std.ArrayList(t.Support) = .empty;
        if (try self.retainedSupport(claim.evidence, claim.depends_on, 0)) try branches.append(self.a, .{ .evidence = claim.evidence, .depends_on = claim.depends_on });
        for (claim.support_sets) |support| if (try self.retainedSupport(support.evidence, support.depends_on, 0)) try branches.append(self.a, support);
        claim.evidence = if (branches.items.len > 0) branches.items[0].evidence else &.{};
        claim.depends_on = if (branches.items.len > 0) branches.items[0].depends_on else &.{};
        claim.support_sets = if (branches.items.len > 1) branches.items[1..] else &.{};
        inline for (assertion_reference_fields) |field| @field(claim.*, field) = try self.publicAssertionRefs(@field(claim.*, field));
    }
    fn publicAssertionRef(self: *Run, id: []const u8) !bool {
        const record = (try self.assertion(id)) orelse return false;
        self.authorize(record.owner, record.assertion.visibility) catch return false;
        return self.retained(record, 0);
    }
    fn publicAssertionRefs(self: *Run, ids: []const []const u8) ![]const []const u8 {
        var result: std.ArrayList([]const u8) = .empty;
        for (ids) |id| if (try self.publicAssertionRef(id)) try result.append(self.a, id);
        return result.items;
    }
    fn authorize(self: *Run, owner: []const u8, visibility: t.Visibility) !void {
        if (visibility == .private and !eql(u8, owner, self.service.context.principal)) {
            if (!self.service.context.admin) return error.Forbidden;
            if (!self.administrative_view) try self.requireAdmin();
        }
    }
    fn assertion(self: *Run, id: []const u8) !?t.StoredAssertion {
        return self.read(t.StoredAssertion, try key(self.a, "assertion", id));
    }
    fn evidence(self: *Run, id: []const u8) !?t.StoredEvidence {
        return self.read(t.StoredEvidence, try key(self.a, "evidence", id));
    }
    // One retention gate for evidence reads, support traversal and admission.
    // Fences take effect immediately, independently of the physical scrub cursor.
    // Authorization stays with the caller so denied private evidence cannot be
    // distinguished from an otherwise unavailable support branch.
    fn resourceSnapshot(self: *Run, reference: prov.Resource) !prov.Snapshot {
        try reference.validate();
        if (!reference.external()) return error.InvalidResourceReference;
        if (!eql(u8, reference.database, self.state.database)) return error.Forbidden;
        const cache_key = try hash(self.a, .{ reference, self.service.context.principal });
        if (self.resource_states.getPtr(cache_key)) |value| {
            // Another invocation may have released the lease since this input
            // was checked. Validate before exposing it to a later invocation.
            if (value.epoch != self.inference_epoch) {
                try self.validateResource(value.*);
                value.epoch = self.inference_epoch;
            }
            if (self.capturing_resources) |inputs| try inputs.put(self.a, cache_key, {});
            self.trackResource(value.snapshot);
            return value.snapshot;
        }
        const resolve = self.service.backend.resource orelse return error.MemoryCapabilityUnavailable;
        const value = try resolve(self.service.backend.ptr, self.a, reference);
        try self.cacheResource(reference, value);
        // Failed reads have no admitted snapshot to fence. Partial captures
        // contain only resources actually consumed before the failure.
        if (self.capturing_resources) |inputs| try inputs.put(self.a, cache_key, {});
        self.trackResource(value);
        return value;
    }
    fn cacheResource(self: *Run, reference: prov.Resource, snapshot: prov.Snapshot) !void {
        const cache_key = try hash(self.a, .{ reference, self.service.context.principal });
        // Snapshot candidates may borrow reusable page storage. The resource
        // read set owns its routing metadata independently of that page.
        var owned = reference;
        inline for (.{ "database", "namespace", "table", "id" }) |field| @field(owned, field) = try self.a.dupe(u8, @field(reference, field));
        inline for (.{ "storage_name", "digest" }) |field| if (@field(reference, field)) |value| {
            @field(owned, field) = try self.a.dupe(u8, value);
        };
        try self.resource_states.put(self.a, cache_key, .{ .reference = owned, .snapshot = snapshot, .epoch = self.inference_epoch });
    }
    fn Captured(comptime handler: anytype) type {
        const Result = @typeInfo(@typeInfo(@TypeOf(handler)).@"fn".return_type.?).error_union.payload;
        return struct { resources: ResourceSet, result: Result };
    }
    // Capture the actual native input closure, including cache hits. Visibility
    // memoization is local to this capture, so shared parents and redacted
    // decision lineage cannot disappear from a later invocation's read set.
    fn captureResources(self: *Run, comptime handler: anytype, args: anytype) !Captured(handler) {
        var inputs: ResourceSet = .empty;
        const result = try self.captureResourcesInto(handler, args, &inputs);
        return .{ .resources = inputs, .result = result };
    }
    fn captureResourcesInto(self: *Run, comptime handler: anytype, args: anytype, inputs: *ResourceSet) !@FieldType(Captured(handler), "result") {
        std.debug.assert(self.capturing_resources == null);
        self.capturing_resources = inputs;
        defer self.capturing_resources = null;
        self.views.clearRetainingCapacity();
        return @call(.auto, handler, .{self} ++ args);
    }
    fn validateResource(self: *Run, input: ResourceRead) !void {
        try self.service.context.check();
        const current = try self.service.backend.resource.?(self.service.backend.ptr, self.a, input.reference);
        const admitted = input.snapshot;
        if (current.state == .denied and admitted.state != .denied) return error.MemorySourceAccessDenied;
        if (current.state != admitted.state or current.table_id != admitted.table_id or current.revision != admitted.revision or current.payload_truncated != admitted.payload_truncated or
            !eql(u8, current.json, admitted.json) or !eql(u8, current.digest orelse "", admitted.digest orelse "")) return error.MemoryResourceChanged;
    }
    // The transaction-wide read set is checked once at publication. Each
    // inference boundary checks only that invocation's causal input closure.
    fn validateResources(self: *Run) !void {
        var reads = self.resource_states.valueIterator();
        while (reads.next()) |input| try self.validateResource(input.*);
    }
    fn inferenceCompleted(self: *Run, inputs: ResourceSet) !void {
        // Every model boundary restores live authority and the original memory
        // snapshot before another model may consume cached memory content.
        try self.service.context.finish();
        const expected = self.state_row orelse return error.MemorySpaceNotFound;
        const latest = (try self.service.backend.get(self.service.backend.ptr, self.a, "control")) orelse return error.MemorySpaceNotFound;
        if (latest.version != expected.version or !eql(u8, latest.json, expected.json)) return error.MemoryRevisionConflict;
        self.inferred = true;
        self.inference_epoch += 1;
        var reads = inputs.keyIterator();
        while (reads.next()) |path| {
            const input = self.resource_states.getPtr(path.*).?;
            try self.validateResource(input.*);
            input.epoch = self.inference_epoch;
        }
    }
    fn embed(self: *Run, model: t.Model, text: []const u8, inputs: ResourceSet) ![]const f32 {
        const result = try self.service.backend.embed.?(self.service.backend.ptr, self.a, model, text);
        try self.inferenceCompleted(inputs);
        return result;
    }
    fn embedClaimDocument(self: *Run, model: t.Model, text: []const u8, inputs: ResourceSet) ![]const f32 {
        const result = if (self.service.backend.embed_document) |document| try document(self.service.backend.ptr, self.a, model, text) else try self.service.backend.embed.?(self.service.backend.ptr, self.a, model, text);
        try self.inferenceCompleted(inputs);
        return result;
    }
    fn embeddingInputs(self: *Run, claim: t.Assertion, generated: bool) !void {
        try self.validateAssertion(claim, generated);
        if (!try self.generationVisible("assertion", claim.id)) return error.MemoryForgotten;
    }
    fn generationInputs(self: *Run, sources: []const t.StoredEvidence) !void {
        for (sources) |source| if (source.evidence.resource) |reference| {
            _ = try self.resourceSnapshot(reference);
        };
    }
    fn resourceCurrent(self: *Run, source: t.StoredEvidence) !bool {
        const reference = source.evidence.resource orelse return true;
        const snapshot = try self.resourceSnapshot(reference);
        return snapshot.state == .current or (self.historical_resource_reads and snapshot.state == .changed);
    }
    fn evidenceRetained(self: *Run, source: t.StoredEvidence) !bool {
        if (self.lineage_head) |head| if (head.complete and !self.lineage_raw and !self.change_positions.contains(try key(self.a, "evidence", source.evidence.id))) {
            if (try self.lineage_index.?.get(head.root, try key(self.a, "evidence", source.evidence.id)) == null) return false;
        };
        if (source.forgotten or self.isExpired(source) or
            try self.read(bool, "forgotten-all") != null or
            try self.read(bool, try key(self.a, "forgotten-source", source.evidence.source_id)) != null or
            try self.read(bool, try key(self.a, "forgotten-evidence", source.evidence.id)) != null) return false;
        if (source.evidence.resource) |reference| {
            const snapshot = try self.resourceSnapshot(reference);
            if (snapshot.state == .missing or (snapshot.state == .denied and !self.retention_maintenance)) return false;
        }
        return true;
    }
    fn isExpired(self: *Run, source: t.StoredEvidence) bool {
        if (source.evidence.expires_at) |expiry| if (expiry <= self.service.context.now_ms) return true;
        if (self.state.config.retention_ms) |retention| if (@as(i128, source.recorded_at) + retention <= self.service.context.now_ms) return true;
        return false;
    }
    fn evidenceExpiry(self: *Run, source: t.StoredEvidence) ?i64 {
        var expiration = source.evidence.expires_at;
        if (self.state.config.retention_ms) |retention| {
            const retained_until: i64 = @intCast(@min(@as(i128, source.recorded_at) + retention, std.math.maxInt(i64)));
            if (expiration == null or retained_until < expiration.?) expiration = retained_until;
        }
        return expiration;
    }
    fn generationRetained(self: *Run, kind: []const u8, id: []const u8) !bool {
        const inputs = (try self.read(GenerationProvenance, try self.generationKey(kind, id))) orelse return true;
        return self.provenanceAvailable(inputs, .retained);
    }
    fn entityRetained(self: *Run, record: t.EntityRecord) !bool {
        return try self.retainedSupport(record.entity.evidence, &.{}, 0) and try self.generationRetained("entity", record.entity.id);
    }
    fn generationVisible(self: *Run, kind: []const u8, id: []const u8) !bool {
        const inputs = (try self.read(GenerationProvenance, try self.generationKey(kind, id))) orelse return true;
        return self.provenanceAvailable(inputs, .visible);
    }
    fn provenanceAvailable(self: *Run, inputs: GenerationProvenance, comptime view: RecordView) !bool {
        if (try self.read(bool, "forgotten-all") != null) return false;
        for (inputs.evidence_ids) |input| {
            if (!try self.reasonSupport(&.{.{ .id = input }}, view)) return false;
        }
        return self.causalContextAvailable(inputs.context, view == .visible);
    }
    // Context snapshots carry their complete causal closure, so checking them
    // is bounded and cannot create cycles through later assertion revisions.
    fn causalContextAvailable(self: *Run, inputs: []const t.DerivedDependency, visible: bool) !bool {
        for (inputs) |input| {
            try self.supportStep();
            const ref = input.resource;
            if (ref.kind == .assertion) {
                if (try self.read(bool, try key(self.a, "forgotten-assertion", ref.id)) != null) return false;
                const record = (try self.read(t.StoredAssertion, try historyKey(self.a, ref.id, ref.revision))) orelse return false;
                self.authorize(record.owner, record.assertion.visibility) catch return false;
            } else if (ref.kind == .entity) {
                if (try self.read(bool, try key(self.a, "forgotten-entity", ref.id)) != null) return false;
                const record = (try self.read(t.EntityRecord, try key(self.a, "entity", ref.id))) orelse return false;
                if (record.revision != ref.revision) return false;
                self.authorize(record.owner, record.entity.visibility) catch return false;
            } else return error.MemoryCorruptState;
            if (visible) {
                if (!try self.supportVisible(input.evidence, &.{}, 0)) return false;
            } else if (!try self.retainedSupport(input.evidence, &.{}, 0)) return false;
        }
        return true;
    }
    fn generationKey(self: *Run, kind: []const u8, id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "generation-input/{s}/{s}", .{ kind, try hash(self.a, id) });
    }
    fn entityVisible(self: *Run, record: t.EntityRecord) !bool {
        const cache_key = try self.viewKey("entity-retention", record.entity.id, 0, null, null, 0);
        if (self.views.get(cache_key)) |result| return result;
        const result = try self.supportVisible(record.entity.evidence, &.{}, 0) and try self.generationVisible("entity", record.entity.id);
        try self.views.put(self.a, cache_key, result);
        return result;
    }
    fn curationProvenanceRetained(self: *Run, value: EntityCurationProvenance) !bool {
        if (value.entity_ids.len == 0 or value.entity_ids.len > 2) return error.MemoryCorruptState;
        if (try self.read(bool, "forgotten-all") != null) return false;
        if (value.judgment) |ref| {
            const artifact = (try self.read(JudgmentArtifact, try key(self.a, "judgment", ref.id))) orelse return false;
            if (artifact.operation.revision != ref.revision) return false;
            if (self.retention_maintenance) {
                if (!try self.judgmentAvailable(artifact, .retained)) return false;
            } else if (!try self.judgmentAvailable(artifact, .visible)) return false;
        } else {
            // Pre-binding external verdict rationales cannot prove their input
            // closure. Manual curation still has its explicit evidence policy.
            const event = try std.fmt.allocPrint(self.a, "audit-event/entity/{s}/{d:0>20}", .{ try hash(self.a, value.entity_ids[0]), value.revision });
            if (try self.read(prov.Resource, event)) |ref| if (try self.read(prov.Decision, try key(self.a, "audit", ref.id))) |decision| {
                if (eql(u8, decision.action, "review_entity")) return false;
            };
        }
        for (value.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
            if (!try self.evidenceRetained(source) or (!self.retention_maintenance and !try self.resourceCurrent(source))) return false;
        }
        for (value.entity_ids) |id| {
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return false;
            if (!try self.entityRetained(record)) return false;
        }
        return true;
    }
    // Physical retention is independent of current belief/lifecycle. A
    // superseded premise can still support an auditable historical revision.
    fn supportStep(self: *Run) !void {
        try self.service.context.check();
        if (self.support_work >= max_support_work) return error.MemoryWorkLimit;
        self.support_work += 1;
    }
    fn viewKey(self: *Run, kind: []const u8, id: []const u8, revision: u64, known: ?i64, valid: ?i64, depth: u8) ![]const u8 {
        return hash(self.a, .{ kind, id, revision, known, valid, depth, self.state.config.generation, self.service.context.branch, if (self.lineage_head) |head| head.root else null, self.service.context.principal, self.service.context.admin, self.retention_maintenance });
    }
    fn retained(self: *Run, record: t.StoredAssertion, depth: u8) anyerror!bool {
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        const cache_key = try self.viewKey("retention", record.assertion.id, record.revision, null, null, depth);
        if (self.views.get(cache_key)) |result| return result;
        try self.supportStep();
        const result = try self.retainedInner(record, depth);
        try self.views.put(self.a, cache_key, result);
        return result;
    }
    // Publication coverage excludes only irreversible retirement. A denied
    // source remains an obligation; never publish a generation that silently
    // skipped it, and never spend the model retry budget waiting for access.
    fn publicationCandidate(self: *Run, record: t.StoredAssertion) !bool {
        const previous = self.retention_maintenance;
        self.retention_maintenance = true;
        const durable = self.retained(record, 0);
        self.retention_maintenance = previous;
        if (!try durable) return false;
        if (!try self.retained(record, 0)) return error.MemorySourceAccessDenied;
        return true;
    }
    fn outcomeRetained(self: *Run, record: StoredOutcome) !bool {
        if (try self.read(bool, "forgotten-all") != null or
            try self.read(bool, try key(self.a, "forgotten-outcome", record.outcome.id)) != null) return false;
        for (record.outcome.used_memory_ids) |id| {
            const parent = (try self.assertion(id)) orelse return false;
            self.authorize(parent.owner, parent.assertion.visibility) catch return false;
            if (!try self.retained(parent, 0)) return false;
        }
        return true;
    }
    fn retainedInner(self: *Run, record: t.StoredAssertion, depth: u8) anyerror!bool {
        if (self.lineage_head) |head| if (head.complete and !self.lineage_raw and !self.change_positions.contains(try key(self.a, "assertion", record.assertion.id))) {
            if (try self.lineage_index.?.get(head.root, try key(self.a, "assertion", record.assertion.id)) == null or try self.lineage_index.?.get(head.root, try historyKey(self.a, record.assertion.id, record.revision)) == null) return false;
        };
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        if (try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-assertion", record.assertion.id)) != null) return false;
        if (!try self.generationRetained("assertion", record.assertion.id)) return false;
        if (try self.retainedSupport(record.assertion.evidence, record.assertion.depends_on, depth)) return true;
        for (record.assertion.support_sets) |support| if (try self.retainedSupport(support.evidence, support.depends_on, depth)) return true;
        return false;
    }
    fn retainedSupport(self: *Run, refs: []const t.EvidenceRef, parents: []const []const u8, depth: u8) anyerror!bool {
        if (refs.len == 0) return false;
        for (refs) |ref| {
            try self.supportStep();
            const source = (try self.evidence(ref.id)) orelse return false;
            if (!try self.evidenceRetained(source)) return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
        }
        for (parents) |id| {
            try self.supportStep();
            const parent = (try self.assertion(id)) orelse return false;
            self.authorize(parent.owner, parent.assertion.visibility) catch return false;
            if (!try self.retained(parent, depth + 1)) return false;
        }
        return true;
    }
    fn evidenceVisible(self: *Run, record: t.StoredAssertion) anyerror!bool {
        return self.evidenceVisibleDepth(record, 0);
    }
    fn asOf(self: *Run, id: []const u8, known: i64) !?t.StoredAssertion {
        const cache_key = try hash(self.a, .{ id, known, self.service.context.branch });
        if (self.historical.getEntry(cache_key)) |entry| return entry.value_ptr.*;
        const prefix = try std.fmt.allocPrint(self.a, "history/{s}/", .{try hash(self.a, id)});
        var after: ?[]const u8 = null;
        var selected: ?t.StoredAssertion = null;
        var count: usize = 0;
        while (true) {
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, after, 128);
            for (page.rows) |row| {
                try self.supportStep();
                count += 1;
                if (count > 4096) return error.MemoryWorkLimit;
                const version = (try self.read(t.StoredAssertion, row.key)) orelse continue;
                if (version.recorded_at <= known and (selected == null or version.revision > selected.?.revision)) selected = version;
            }
            after = page.next;
            if (after == null) {
                try self.historical.put(self.a, cache_key, selected);
                return selected;
            }
        }
    }
    fn evidenceVisibleDepth(self: *Run, record: t.StoredAssertion, depth: u8) anyerror!bool {
        return self.evidenceVisibleTime(record, null, null, depth);
    }
    fn evidenceVisibleTime(self: *Run, record: t.StoredAssertion, known: ?i64, valid: ?i64, depth: u8) anyerror!bool {
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        const cache_key = try self.viewKey("visibility", record.assertion.id, record.revision, known, valid, depth);
        if (self.views.get(cache_key)) |result| return result;
        try self.supportStep();
        const result = try self.evidenceVisibleTimeInner(record, known, valid, depth);
        try self.views.put(self.a, cache_key, result);
        return result;
    }
    fn evidenceVisibleTimeInner(self: *Run, record: t.StoredAssertion, known: ?i64, valid: ?i64, depth: u8) anyerror!bool {
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        if (try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-assertion", record.assertion.id)) != null) return false;
        if (!try self.generationVisible("assertion", record.assertion.id)) return false;
        if (!try self.identityVisible(record.assertion)) return false;
        if (try self.supportVisibleTime(record.assertion.evidence, record.assertion.depends_on, known, valid, depth)) return true;
        for (record.assertion.support_sets) |support| if (try self.supportVisibleTime(support.evidence, support.depends_on, known, valid, depth)) return true;
        return false;
    }
    fn supportVisible(self: *Run, refs: []const t.EvidenceRef, dependencies: []const []const u8, depth: u8) anyerror!bool {
        return self.supportVisibleTime(refs, dependencies, null, null, depth);
    }
    fn supportVisibleTime(self: *Run, refs: []const t.EvidenceRef, dependencies: []const []const u8, known: ?i64, valid: ?i64, depth: u8) anyerror!bool {
        if (try self.read(bool, "forgotten-all") != null) return false;
        if (refs.len == 0) return false;
        for (refs) |ref| {
            try self.supportStep();
            const source = (try self.evidence(ref.id)) orelse return false;
            if (!try self.evidenceRetained(source) or !try self.resourceCurrent(source)) return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
        }
        for (dependencies) |dependency| {
            try self.supportStep();
            const parent = (if (known) |time| try self.asOf(dependency, time) else try self.assertion(dependency)) orelse return false;
            self.authorize(parent.owner, parent.assertion.visibility) catch return false;
            if (parent.stale or parent.lifecycle == .retracted or parent.lifecycle == .disputed or parent.conflicts_with.len > 0) return false;
            if (parent.lifecycle == .superseded) {
                // A finite original validity interval is not proof of when an
                // undated replacement took effect. Never invent that boundary.
                const boundary = parent.superseded_valid_at orelse return false;
                if (valid == null or valid.? >= boundary) return false;
            }
            if (valid) |time| {
                if (parent.assertion.valid_from) |start| if (start > time) return false;
                if (parent.assertion.valid_to) |end| if (end <= time) return false;
            }
            if (!try self.evidenceVisibleTime(parent, known, valid, depth + 1)) return false;
        }
        return true;
    }
    fn visibleSupport(self: *Run, claim: t.Assertion, known: ?i64, valid: ?i64) !t.Support {
        if (try self.supportVisibleTime(claim.evidence, claim.depends_on, known, valid, 0)) return .{ .evidence = claim.evidence, .depends_on = claim.depends_on };
        for (claim.support_sets) |support| if (try self.supportVisibleTime(support.evidence, support.depends_on, known, valid, 0)) return support;
        return error.MemoryEvidenceRequired;
    }
    fn observe(self: *Run, request: t.ObservationRequest, op: *t.Operation) !void {
        if (try self.read(bool, "forgotten-all") != null) return error.MemoryForgotten;
        op.origin = request.origin;
        if (op.origin) |*origin| {
            try origin.validate();
            inline for (.{ "trace", "span" }) |field| if (@field(origin, field)) |reference| {
                const snapshot = try self.resourceSnapshot(reference);
                if (snapshot.state == .denied) return error.Forbidden;
                var pinned = reference;
                if (snapshot.table_id == reference.table_id and pinned.storage_name == null) pinned.storage_name = snapshot.storage_name;
                if (snapshot.state == .current and pinned.digest == null) pinned.digest = snapshot.digest;
                @field(origin, field) = pinned;
                try self.cacheResource(pinned, snapshot);
            };
        }
        if (request.evidence.len == 0 or request.evidence.len > t.max_items or request.context_evidence_ids.len > t.max_items or request.entities.len > self.state.config.max_output_items) return error.InvalidMemoryRequest;
        var inputs: std.ArrayList([]const u8) = .empty;
        for (request.evidence, 0..) |original, source_index| {
            var materialization_report: screening.Report = .{};
            if (self.state.config.intake_screening.mode == .redact and source_index < self.original_evidence.len) {
                materialization_report = (try screening.typed(t.Evidence, self.a, self.original_evidence[source_index], self.state.config.intake_screening)).report;
            }
            var source = original;
            if (self.state.config.processing == .managed) source.managed = true;
            if (source.resource_selector) |selector| {
                if (source.resource != null or !eql(u8, selector.database, self.state.database) or (selector.expected_revision != null and selector.expected_revision.? == 0)) return error.InvalidResourceReference;
                const candidate: prov.Resource = .{ .database = selector.database, .namespace = selector.namespace, .table = selector.table, .table_id = 0, .kind = switch (selector.kind) {
                    .record => .record,
                    .relationship => .relationship,
                    .trace => .trace,
                }, .id = selector.id, .revision = selector.expected_revision orelse 0 };
                // Validate names before invoking a transport callback; zero IDs
                // are permitted only for this explicit admission selector.
                var checked = candidate;
                checked.table_id = 1;
                checked.revision = 1;
                try checked.validate();
                const resolve = self.service.backend.resource orelse return error.MemoryCapabilityUnavailable;
                const snapshot = try resolve(self.service.backend.ptr, self.a, candidate);
                if (snapshot.state == .denied) return error.Forbidden;
                if (snapshot.state != .current or snapshot.table_id == 0 or snapshot.revision == 0) return error.MemoryResourceChanged;
                var pinned = candidate;
                pinned.table_id = snapshot.table_id;
                pinned.revision = snapshot.revision;
                pinned.digest = snapshot.digest;
                pinned.storage_name = snapshot.storage_name;
                source.resource = pinned;
                source.resource_selector = null;
                try self.cacheResource(pinned, snapshot);
            }
            if (source.resource) |initial_reference| {
                if (self.service.context.durable_execution_required and self.state.execution_signature.len == 0) return error.MemoryExecutionIdentityRequired;
                const snapshot = try self.resourceSnapshot(initial_reference);
                var reference = initial_reference;
                if (reference.storage_name == null) reference.storage_name = snapshot.storage_name;
                if (reference.digest == null) reference.digest = snapshot.digest;
                source.resource = reference;
                try self.cacheResource(reference, snapshot);
                if (snapshot.state == .denied) return error.Forbidden;
                if (snapshot.state != .current) return error.MemoryResourceChanged;
                if (snapshot.payload_truncated) return error.MemoryInputTooLarge;
                if (source.text.len > 0 and !eql(u8, source.text, snapshot.json)) return error.MemoryResourceChanged;
                const screened = try screening.json(self.a, snapshot.json, self.state.config.intake_screening);
                self.intake_report.add(screened.report);
                materialization_report.add(screened.report);
                if (!eql(u8, source.text, screened.value) and screened.report.redactions > 0) try self.intake_redacted_evidence.put(self.a, source.id, {});
                source.text = screened.value;
                if (source.source_id.len == 0) source.source_id = try hash(self.a, .{ reference.database, reference.namespace, reference.table_id, reference.kind, reference.id });
                if (source.source_version.len == 0) source.source_version = try std.fmt.allocPrint(self.a, "{d}", .{reference.revision});
                try self.put(try std.fmt.allocPrint(self.a, "resource-evidence/{s}/{s}", .{ try hash(self.a, reference.identity()), try hash(self.a, source.id) }), .{ .evidence_id = source.id });
                try self.put(try key(self.a, "resource-watch", source.id), .{ .evidence_id = source.id, .last_state = snapshot.state });
                self.scheduleResourceCheck();
            }
            const screened_source = try screening.typed(t.Evidence, self.a, source, self.state.config.intake_screening);
            self.intake_report.add(screened_source.report);
            materialization_report.add(screened_source.report);
            source = screened_source.value;
            try t.validateId(source.id);
            // An ungrouped observation is its own logical source. Its default
            // version is a digest of screened evidence, never wall-clock state.
            if (source.source_id.len == 0) source.source_id = source.id;
            if (source.source_version.len == 0) source.source_version = try hash(self.a, source);
            try t.validateId(source.source_id);
            try t.validateId(source.source_version);
            if (self.service.context.durable_execution_required) {
                const processing = source.managed or self.state.config.features.entity_extraction or self.state.config.features.summaries;
                if (processing and self.service.context.execution_signature.len == 0) return error.MemoryExecutionIdentityRequired;
                if (source.expires_at != null and self.state.execution_signature.len == 0) return error.MemoryExecutionIdentityRequired;
            }
            if (source.text.len == 0 or source.text.len > t.max_text_bytes or !std.unicode.utf8ValidateSlice(source.text)) return error.InvalidMemoryRequest;
            if (try self.read(bool, try key(self.a, "forgotten-source", source.source_id)) != null or try self.read(bool, try key(self.a, "forgotten-evidence", source.id)) != null) return error.MemoryForgotten;
            for (self.state.config.source_rules) |rule| if (eql(u8, rule.source_id, source.source_id)) {
                if (rule.principal) |principal| if (!eql(u8, principal, self.service.context.principal)) return error.Forbidden;
                if (rule.producer) |producer| if (!eql(u8, producer, source.producer)) return error.MemoryProducerConflict;
            };
            const source_key = try key(self.a, "producer", try hash(self.a, .{ source.source_id, source.source_version }));
            const producer_identity = try self.encode(.{ .owner = self.service.context.principal, .producer = source.producer, .managed = source.managed });
            if (try self.read([]const u8, source_key)) |old| {
                if (!eql(u8, old, producer_identity)) return error.MemoryProducerConflict;
            } else try self.put(source_key, producer_identity);
            const path = try key(self.a, "evidence", source.id);
            if (try self.read(t.StoredEvidence, path)) |old| {
                try self.authorize(old.owner, .private);
                // Evidence identifiers name immutable versions. Source edits
                // use a fresh evidence ID, preserving historical provenance.
                if (!eql(u8, try self.encode(old.evidence), try self.encode(source))) return error.MemoryEvidenceConflict;
            } else {
                const existing = if (self.lineage_head != null) try self.rawRead(t.StoredEvidence, path) else null;
                if (existing) |old| {
                    try self.authorize(old.owner, .private);
                    if (!valueEqual(t.Evidence, old.evidence, source)) return error.MemoryEvidenceConflict;
                }
                try self.put(path, existing orelse t.StoredEvidence{ .materialization = if (materialization_report.redactions > 0) .{ .screening = materialization_report, .text_redacted = self.intake_redacted_evidence.contains(source.id), .text_sha256 = try textDigest(self.a, source.text) } else null, .evidence = source, .owner = self.service.context.principal, .recorded_at = self.service.context.now_ms, .revision = self.state.revision });
            }
            try self.indexEpisode((try self.evidence(source.id)).?);
            const expires = self.evidenceExpiry((try self.evidence(source.id)).?);
            if (expires) |time| if (self.state.next_expiry_at == null or time < self.state.next_expiry_at.?) {
                self.state.next_expiry_at = time;
            };
            if (source.managed) {
                if (self.state.config.processing == .external or !(self.state.config.features.assertion_distillation or self.state.config.features.entity_extraction or self.state.config.features.summaries)) return error.MemoryManagedProcessingDisabled;
                try inputs.append(self.a, source.id);
            } else if (self.state.config.features.entity_extraction or self.state.config.features.summaries) try inputs.append(self.a, source.id);
        }
        for (request.context_evidence_ids) |id| {
            const source = (try self.evidence(id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (!try self.evidenceRetained(source) or !try self.resourceCurrent(source)) return error.MemoryForgotten;
            for (request.evidence) |current| if (eql(u8, current.id, id)) return error.InvalidMemoryRequest;
        }
        op.context_inputs = request.context_evidence_ids;
        for (request.entities) |value| try self.entity(value);
        if (request.mentions.len > self.state.config.max_output_items) return error.MemoryInputTooLarge;
        for (request.mentions) |value| try self.mention(value);
        try self.assertions(request.assertions, false);
        if (inputs.items.len > 0) {
            op.phase = .pending;
            op.assertions_ready = false;
            op.inputs = inputs.items;
        }
    }
    fn validateEntityReference(self: *Run, id: []const u8, expected: ?[]const u8, visibility: t.Visibility) !void {
        // An absent untyped subject can be a literal domain ID. A retired
        // entity ID cannot become a literal again once its payload is gone.
        if (try self.read(bool, try key(self.a, "forgotten-entity", id)) != null) return error.MemoryForgotten;
        const record = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse {
            if (expected != null) return error.MemoryEntityRequired;
            return;
        };
        try self.authorize(record.owner, record.entity.visibility);
        if (!try self.entityVisible(record)) return error.MemoryEntityRequired;
        if (expected) |type_name| if (!eql(u8, type_name, record.entity.type)) return error.MemoryOntologyMismatch;
        if (visibility == .team and record.entity.visibility == .private) return error.MemoryVisibilityConflict;
    }
    fn validateOntology(self: *Run, claim: t.Assertion) !void {
        try self.validateEntityReference(claim.subject, null, claim.visibility);
        if (self.state.config.ontology_pack == .software_team_v1 and isConceptRelation(claim.predicate)) {
            try self.validateEntityReference(claim.subject, "concept", claim.visibility);
            try self.validateEntityReference(claim.value, "concept", claim.visibility);
        }
        for (self.state.config.predicates) |predicate| if (eql(u8, predicate.name, claim.predicate)) {
            if (predicate.subject_type) |expected| try self.validateEntityReference(claim.subject, expected, claim.visibility);
            if (predicate.object_type) |expected| try self.validateEntityReference(claim.value, expected, claim.visibility);
        };
    }
    // Validate the whole immutable DAG, not just the currently visible support
    // branch. Losing a short primary branch must not reveal an over-depth one.
    // Memoization bounds work for shared ancestors and repeated batch inputs.
    fn dependencyDepth(self: *Run, claim: t.Assertion, depth: u8) anyerror!u8 {
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        var levels: u8 = 1;
        levels = @max(levels, try self.parentDepth(claim.id, claim.depends_on, depth));
        for (claim.support_sets) |support| levels = @max(levels, try self.parentDepth(claim.id, support.depends_on, depth));
        if (@as(u16, depth) + levels > max_dependency_depth) return error.MemoryWorkLimit;
        return levels;
    }
    fn parentDepth(self: *Run, child: []const u8, parents: []const []const u8, depth: u8) !u8 {
        var levels: u8 = 1;
        for (parents) |id| {
            try self.service.context.check();
            if (eql(u8, id, child)) return error.MemoryDependencyCycle;
            const parent_levels = self.dependency_depths.get(id) orelse value: {
                if (self.dependency_visits >= 4096) return error.MemoryWorkLimit;
                self.dependency_visits += 1;
                const parent = (try self.assertion(id)) orelse return error.MemoryDependencyMissing;
                const result = try self.dependencyDepth(parent.assertion, depth + 1);
                try self.dependency_depths.put(self.a, id, result);
                break :value result;
            };
            levels = @max(levels, parent_levels + 1);
        }
        return levels;
    }
    fn validateAssertion(self: *Run, claim: t.Assertion, generated: bool) anyerror!void {
        try t.validateId(claim.id);
        try t.validateId(claim.subject);
        try t.validateId(claim.predicate);
        try self.validateOntology(claim);
        if (claim.value.len == 0 or claim.statement.len == 0 or claim.statement.len > t.max_text_bytes or !std.unicode.utf8ValidateSlice(claim.statement)) return error.InvalidMemoryAssertion;
        if (claim.evidence.len == 0 or claim.evidence.len > t.max_items or claim.depends_on.len > t.max_items) return error.InvalidMemoryAssertion;
        if (claim.valid_from != null and claim.valid_to != null and claim.valid_from.? >= claim.valid_to.?) return error.InvalidMemoryAssertion;
        if (try self.read(bool, try key(self.a, "forgotten-assertion", claim.id)) != null) return error.MemoryForgotten;
        for (claim.evidence) |ref| {
            try self.supportStep();
            const source = (try self.evidence(ref.id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (!try self.evidenceRetained(source) or !try self.resourceCurrent(source)) return error.MemoryForgotten;
            // Every admitted branch owns a producer path. An external claim
            // cannot borrow managed evidence through an alternative branch.
            if (!generated and source.evidence.managed) return error.MemoryProducerConflict;
            if (claim.visibility == .team and source.evidence.visibility == .private) return error.MemoryVisibilityConflict;
            if (ref.span != null and self.intake_redacted_evidence.contains(ref.id)) return error.InvalidMemorySpan;
            if (ref.span) |span| {
                if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
            }
        }
        for (claim.depends_on) |dependency| {
            try self.supportStep();
            if (eql(u8, dependency, claim.id)) return error.MemoryDependencyCycle;
            const parent = (try self.assertion(dependency)) orelse return error.MemoryDependencyMissing;
            try self.authorize(parent.owner, parent.assertion.visibility);
            // The new assertion occupies depth zero. Admission must reserve
            // its level, using the same bound as recall and retention.
            if (parent.stale or parent.lifecycle != .active or !try self.evidenceVisibleDepth(parent, 1)) return error.MemoryDependencyStale;
            if (claim.visibility == .team and parent.assertion.visibility == .private) return error.MemoryVisibilityConflict;
        }
        for ([_][]const []const u8{ claim.motivates, claim.rejected_in_favor_of }) |links| {
            if (links.len > t.max_items) return error.MemoryInputTooLarge;
            for (links) |target| {
                const linked = (try self.assertion(target)) orelse return error.MemoryDependencyMissing;
                try self.authorize(linked.owner, linked.assertion.visibility);
                if (!try self.evidenceVisible(linked)) return error.MemoryDependencyStale;
                if (claim.visibility == .team and linked.assertion.visibility == .private) return error.MemoryVisibilityConflict;
            }
        }
        _ = try self.dependencyDepth(claim, 0);
        if (claim.support_sets.len > t.max_items) return error.MemoryInputTooLarge;
        for (claim.support_sets) |support| {
            var branch = claim;
            branch.evidence = support.evidence;
            branch.depends_on = support.depends_on;
            branch.support_sets = &.{};
            try self.validateAssertion(branch, generated);
        }
        if (claim.embedding) |vector| {
            if (!self.state.config.features.semantic_recall) return error.MemoryCapabilityUnavailable;
            const model = self.state.config.models.embedder orelse return error.MemoryModelRequired;
            if (vector.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
            if (claim.embedding_model == null or !valueEqual(t.Model, claim.embedding_model.?, model)) return error.MemoryEmbeddingMismatch;
            for (vector) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        }
    }
    fn assertions(self: *Run, input: []const t.Assertion, generated: bool) !void {
        const before = self.intake_report.redactions;
        const claims = try self.screenTyped([]const t.Assertion, input);
        if (self.intake_report.redactions != before) for (claims) |claim| if (claim.embedding != null) return error.MemoryEmbeddingMismatch;
        if (claims.len > self.state.config.max_output_items) return error.MemoryInputTooLarge;
        for (claims) |original| {
            var claim = original;
            if (!generated and std.mem.startsWith(u8, claim.producer, "managed:")) return error.MemoryProducerConflict;
            if (generated) claim.producer = try std.fmt.allocPrint(self.a, "managed:{d}", .{self.state.config.generation});
            const input_hash = try hash(self.a, claim);
            var embedding_inputs: ResourceSet = .empty;
            if (self.state.config.features.semantic_recall and claim.embedding == null) {
                embedding_inputs = (try self.captureResources(embeddingInputs, .{ claim, generated })).resources;
            } else try self.validateAssertion(claim, generated);
            if (try self.assertion(claim.id)) |old| {
                try self.authorize(old.owner, .private);
                if (old.input_hash.len > 0) {
                    if (!eql(u8, old.input_hash, input_hash)) return error.MemoryAssertionConflict;
                    continue;
                }
                var prior_claim = old.assertion;
                if (claim.embedding == null and self.state.config.features.semantic_recall) {
                    prior_claim.embedding = null;
                    prior_claim.embedding_model = null;
                }
                if (!eql(u8, try self.encode(prior_claim), try self.encode(claim))) return error.MemoryAssertionConflict;
                continue;
            }
            if (self.lineage_head != null) if (try self.rawRead(t.StoredAssertion, try key(self.a, "assertion", claim.id))) |existing| {
                try self.authorize(existing.owner, .private);
                if (existing.input_hash.len == 0 or !eql(u8, existing.input_hash, input_hash)) return error.MemoryAssertionConflict;
            };
            if (self.state.config.features.semantic_recall and claim.embedding == null) {
                claim.embedding = try self.embedClaimDocument(self.state.config.models.embedder.?, claim.statement, embedding_inputs);
                claim.embedding_model = self.state.config.models.embedder;
            }
            try self.validateAssertion(claim, generated);
            const model_decision = if (self.state.config.features.reconciliation == .model) try self.decideClaim(&claim, self.service.context.principal, null) else null;
            if (claim.embedding) |embedding| try self.putVector(self.state.config, claim, embedding);
            claim.embedding = null;
            claim.embedding_model = null;
            const record: t.StoredAssertion = .{ .assertion = claim, .input_hash = input_hash, .asserted_valid_to = claim.valid_to, .owner = self.service.context.principal, .recorded_from = self.service.context.now_ms, .revision = self.state.revision, .config_generation = self.state.config.generation };
            try self.putAssertionWithReasons(record, .{ .assertion = model_decision });
            if (claim.kind == .summary or claim.kind == .procedural) try self.put(try dependencyKey(self.a, "entity", claim.subject, claim.id), .{ .child = claim.id });
            for (claim.evidence) |ref| try self.put(try dependencyKey(self.a, "evidence", ref.id, claim.id), .{ .child = claim.id });
            for (claim.depends_on) |parent| try self.put(try dependencyKey(self.a, "assertion", parent, claim.id), .{ .child = claim.id });
            for (claim.support_sets) |support| {
                for (support.evidence) |ref| try self.put(try dependencyKey(self.a, "evidence", ref.id, claim.id), .{ .child = claim.id });
                for (support.depends_on) |parent| try self.put(try dependencyKey(self.a, "assertion", parent, claim.id), .{ .child = claim.id });
            }
            try self.applySupersessions(claim, self.service.context.principal, model_decision);
            if (self.state.config.features.reconciliation == .deterministic) try self.reconcile(record);
        }
    }
    fn applySupersessions(self: *Run, claim: t.Assertion, owner: []const u8, model_decision: ?[]const u8) !void {
        for (claim.supersedes) |previous| {
            if (eql(u8, previous, claim.id)) return error.MemoryInvalidSupersession;
            var old = (try self.assertion(previous)) orelse return error.MemoryNotFound;
            try self.authorize(old.owner, old.assertion.visibility);
            if (!try self.retained(old, 0)) return error.MemoryForgotten;
            if (claim.visibility == .private and !eql(u8, old.owner, owner)) return error.MemoryVisibilityConflict;
            if (!try self.sameCanonicalGroup(claim, old.assertion) or claim.modality != .actual or claim.role == .hypothesized or claim.stability != .settled) return error.MemoryInvalidSupersession;
            old.lifecycle = .superseded;
            old.superseded_by = claim.id;
            old.superseded_valid_at = claim.valid_from;
            if (claim.valid_from) |start| {
                if (old.assertion.valid_from) |previous_start| if (start < previous_start) return error.MemoryInvalidSupersession;
                if (old.assertion.valid_to == null or old.assertion.valid_to.? > start) old.assertion.valid_to = start;
            }
            old.recorded_to = self.service.context.now_ms;
            old.reason = claim.reconciliation_reason;
            old.explicit_disposition = explicitDisposition(old);
            old.explicit_disposition.?.provenance = .{ .principal = self.service.context.principal, .recorded_at = self.service.context.now_ms, .evidence = claim.evidence };
            old.revision = self.state.revision;
            var reasons = try self.reasonBindings((try self.assertion(previous)).?);
            reasons.disposition = model_decision;
            try self.putAssertionWithReasons(old, reasons);
            try self.invalidate("assertion", previous);
        }
    }
    fn decideClaim(self: *Run, claim: *t.Assertion, owner: []const u8, previous_reason: ?[]const u8) !?[]const u8 {
        if (!self.singlePredicate(claim.predicate) or claim.modality != .actual or claim.role == .hypothesized or claim.stability != .settled) return null;
        const decide = self.service.backend.decide orelse return error.MemoryCapabilityUnavailable;
        // Capture the artifact actually supplied to inference before replacing
        // its text. A rebuild rewrites this assertion's history revision in the
        // same transaction, so a self-reference to that row is not a snapshot.
        const incoming_reason = if (claim.reconciliation_reason != null) previous_reason else null;
        const inputs = try self.captureResources(decisionInputs, .{ claim.*, owner, incoming_reason });
        const prior = inputs.result;
        if (prior.len == 0) return null;
        const incoming = claim.*;
        const raw_decision = try decide(self.service.backend.ptr, self.a, self.state.config, incoming, prior);
        try self.inferenceCompleted(inputs.resources);
        const decision = try self.screenTyped(t.ReconciliationDecision, raw_decision);
        if (decision.reason.len == 0 or decision.reason.len > t.max_text_bytes) return error.MemoryUngroundedOutput;
        for (decision.supersedes) |target| {
            var found = false;
            for (prior) |candidate| if (eql(u8, candidate.assertion.id, target)) {
                found = true;
                break;
            };
            if (!found) return error.MemoryUngroundedOutput;
        }
        claim.supersedes = decision.supersedes;
        claim.reconciliation_reason = decision.reason;
        return try self.storeDecision(incoming, prior, incoming_reason);
    }
    fn decisionInputs(self: *Run, claim: t.Assertion, owner: []const u8, incoming_reason: ?[]const u8) ![]const t.StoredAssertion {
        _ = try self.supportVisible(claim.evidence, claim.depends_on, 0);
        for (claim.support_sets) |support| _ = try self.supportVisible(support.evidence, support.depends_on, 0);
        _ = try self.generationVisible("assertion", claim.id);
        _ = try self.identityVisible(claim);
        if (incoming_reason) |reason| _ = try self.decisionVisible(reason, 0);
        return self.candidates(claim, owner);
    }
    fn reconcileModel(self: *Run, incoming: t.StoredAssertion) !void {
        if (incoming.lifecycle != .active or incoming.stale or !try self.evidenceVisible(incoming)) return;
        var record = try self.retainedRecord(incoming);
        var visible = try self.publicRecord(incoming);
        var reasons = try self.reasonBindings(incoming);
        const previous_links = record.assertion.supersedes;
        const decision = (try self.decideClaim(&visible.assertion, record.owner, reasons.assertion)) orelse return;
        // Only decision fields flow back from the public/model projection.
        // A temporary source denial must never delete retained alternatives.
        record.assertion.supersedes = visible.assertion.supersedes;
        record.assertion.reconciliation_reason = visible.assertion.reconciliation_reason;
        // Apply only the new verdict; retain already accepted relationship links.
        try self.applySupersessions(visible.assertion, record.owner, decision);
        var links: std.ArrayList([]const u8) = .empty;
        for (previous_links) |id| try links.append(self.a, id);
        for (record.assertion.supersedes) |id| {
            var present = false;
            for (links.items) |old| if (eql(u8, id, old)) {
                present = true;
                break;
            };
            if (!present) try links.append(self.a, id);
        }
        if (links.items.len > t.max_items) return error.MemoryWorkLimit;
        record.assertion.supersedes = links.items;
        record.revision = self.state.revision;
        reasons.assertion = decision;
        try self.putAssertionWithReasons(record, reasons);
    }
    fn candidates(self: *Run, claim: t.Assertion, owner: []const u8) ![]const t.StoredAssertion {
        var result: std.ArrayList(t.StoredAssertion) = .empty;
        for (try self.canonicalGroup(claim, owner)) |old| {
            if (eql(u8, old.assertion.id, claim.id) or old.lifecycle != .active or old.stale or old.assertion.modality != .actual or old.assertion.role == .hypothesized or old.assertion.stability != .settled) continue;
            self.authorize(old.owner, old.assertion.visibility) catch continue;
            if (!try self.evidenceVisible(old)) continue;
            if (result.items.len >= t.max_items) return error.MemoryWorkLimit;
            try result.append(self.a, try self.publicRecord(old));
        }
        return result.items;
    }
    fn claimView(self: *Run, record: t.StoredAssertion) !t.ClaimView {
        try self.authorize(record.owner, record.assertion.visibility);
        if (!try self.evidenceVisible(record)) return error.MemoryNotFound;
        var public = try self.publicRecord(record);
        public.assertion.embedding = null;
        public.assertion.embedding_model = null;
        return .{ .memory = public, .fingerprint = try hash(self.a, .{ public, self.state.config.generation }), .config_generation = self.state.config.generation };
    }
    fn pinnedClaim(self: *Run, pin: t.ClaimPin, current: bool, generation: u64) !t.ClaimView {
        const present = (try self.assertion(pin.id)) orelse return error.MemoryNotFound;
        try self.authorize(present.owner, present.assertion.visibility);
        if (!try self.retained(present, 0)) return error.MemoryForgotten;
        const record = if (current) present else (try self.read(t.StoredAssertion, try historyKey(self.a, pin.id, pin.revision))) orelse return error.MemoryNotFound;
        if (record.revision != pin.revision) return error.MemoryRevisionConflict;
        // Re-render even a frozen revision through live source authorization.
        // Access loss may alter the input without changing a memory revision.
        var public = try self.publicRecord(record);
        public.assertion.embedding = null;
        public.assertion.embedding_model = null;
        if (!try self.retained(record, 0)) return error.MemoryForgotten;
        const fingerprint = try hash(self.a, .{ public, generation });
        if (!eql(u8, fingerprint, pin.fingerprint)) return error.MemoryResourceChanged;
        return .{ .memory = public, .fingerprint = fingerprint, .config_generation = generation };
    }
    fn claimsOverlap(self: *Run, lhs: t.StoredAssertion, rhs: t.StoredAssertion) !bool {
        const left = lhs.assertion;
        const right = rhs.assertion;
        if (eql(u8, left.id, right.id) or left.visibility != right.visibility or (left.visibility == .private and !eql(u8, lhs.owner, rhs.owner))) return false;
        const subject = (try self.canonical(left.subject)) orelse return false;
        if (!eql(u8, subject, (try self.canonical(right.subject)) orelse return false)) return false;
        if (left.scope.len != 0 and right.scope.len != 0 and !eql(u8, left.scope, right.scope)) return false;
        if (left.task != null and right.task != null and !optionalEqual(left.task, right.task)) return false;
        if (left.environment != null and right.environment != null and !optionalEqual(left.environment, right.environment)) return false;
        return intervalsOverlap(left, right);
    }
    fn supersessionCandidates(self: *Run, id: []const u8, request: t.SupersessionCandidateRequest) ![]const u8 {
        if (request.max_bytes < 4096 or request.max_bytes > 4 * 1024 * 1024 or request.limit == 0 or request.limit > 64 or !std.math.isFinite(request.lexical_threshold) or request.lexical_threshold <= 0 or request.lexical_threshold > 1 or !std.math.isFinite(request.semantic_threshold) or request.semantic_threshold <= 0) return error.InvalidMemoryRequest;
        const seed_record = (try self.assertion(id)) orelse return error.MemoryNotFound;
        const captured = try self.captureResources(claimView, .{seed_record});
        const seed = captured.result;
        if (try encodedSize(seed) + 1024 > request.max_bytes) return error.MemoryInputTooLarge;
        const index = (try self.read(t.RecallIndex, "assertion-recall-index")) orelse t.RecallIndex{};
        const ready = self.service.backend.search_assertions != null and index.version == t.recall_index_version and index.generation == self.state.config.generation and index.complete and (try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion-recall-work/", null, 1)).rows.len == 0;
        var page: t.SupersessionCandidatePage = .{ .seed = seed, .items = &.{}, .retrieval = if (ready) .indexed else .not_ready, .semantic_discovery = if (!self.state.config.features.semantic_recall) .disabled else .not_ready, .truncated = false, .revision = self.state.revision };
        if (!ready) return self.encode(page);
        var query_vector: ?[]const f32 = null;
        var self_similarity: f64 = 0;
        if (request.mode != .lexical and self.state.config.features.semantic_recall and self.service.backend.embed != null and !eql(u8, self.state.config.models.embedder.?.provider, "external")) {
            if (try self.vectorFor(self.state.config, seed_record.assertion)) |document| {
                query_vector = try self.embed(self.state.config.models.embedder.?, seed.memory.assertion.statement, captured.resources);
                self_similarity = try cosine(query_vector.?, document);
                if (self_similarity > 0) page.semantic_discovery = .indexed;
            }
        }
        if (request.mode == .semantic and page.semantic_discovery != .indexed) return self.encode(page);
        var text: std.ArrayList([]const u8) = .empty;
        var dense: std.ArrayList([]const u8) = .empty;
        const audience = try self.recallAudience(seed_record);
        if (try self.read(bool, try self.recallAudienceKey(self.state.config, audience)) == null) return self.encode(page);
        try text.append(self.a, try self.recallIndexName(self.state.config, audience, false));
        if (page.semantic_discovery == .indexed) try dense.append(self.a, try self.recallIndexName(self.state.config, audience, true));
        var found: RecallSearchPage = .{ .keys = &.{}, .more = false };
        var discovered: std.StringHashMapUnmanaged(void) = .empty;
        var found_keys: std.ArrayList([]const u8) = .empty;
        for ([_]bool{ false, true }) |semantic_lane| {
            if (semantic_lane and dense.items.len == 0 or !semantic_lane and request.mode == .semantic) continue;
            const lane = self.service.backend.search_assertions.?(self.service.backend.ptr, self.a, .{ .text_indexes = text.items, .vector_indexes = if (semantic_lane) dense.items else &.{}, .query = if (semantic_lane) "" else seed.memory.assertion.statement, .filters = .{ .subjects = try self.identityMembers(seed_record.assertion.subject), .overlapping_scope = if (seed_record.assertion.scope.len > 0) seed_record.assertion.scope else null, .environment = seed_record.assertion.environment, .task = seed_record.assertion.task }, .embedding = if (semantic_lane) query_vector else null, .limit = request.limit + 1 }) catch |err| switch (err) {
                // Catalog admission precedes serving-index publication. Keep
                // readiness explicit and discard partial audience/lane results.
                error.IndexNotFound, error.IndexRebuilding, error.Timeout, error.EnrichmentWaitTimeout => {
                    page.retrieval = .not_ready;
                    if (self.state.config.features.semantic_recall) page.semantic_discovery = .not_ready;
                    return self.encode(page);
                },
                else => return err,
            };
            if (lane.keys.len > request.limit + 1) return error.MemoryCorruptState;
            found.more = found.more or lane.more;
            for (lane.keys) |path| {
                const entry = try discovered.getOrPut(self.a, path);
                if (!entry.found_existing) try found_keys.append(self.a, path);
            }
        }
        found.keys = found_keys.items;
        if (found.keys.len > 256) return error.MemoryCorruptState;
        var rows: std.ArrayList(t.StoredAssertion) = .empty;
        var offset: usize = 0;
        while (offset < found.keys.len) {
            const end = @min(found.keys.len, offset + 128);
            try rows.appendSlice(self.a, try self.indexedRecallPage(found.keys[offset..end]));
            offset = end;
        }
        var query = try lexical.PreparedQuery.init(self.lexical_allocator, seed.memory.assertion.statement);
        defer query.deinit();
        var items: std.ArrayList(t.SupersessionCandidate) = .empty;
        for (rows.items) |record| {
            if (record.lifecycle != .active or record.stale or !try self.claimsOverlap(seed_record, record)) continue;
            self.authorize(record.owner, record.assertion.visibility) catch continue;
            if (!try self.evidenceVisible(record)) continue;
            const lexical_score = query.coverage(record.assertion.statement);
            const semantic: ?f64 = if (query_vector != null and self_similarity > 0) if (try self.vectorFor(self.state.config, record.assertion)) |document| (try cosine(query_vector.?, document)) / self_similarity else null else null;
            if (!(request.mode != .semantic and lexical_score >= request.lexical_threshold) and !(request.mode != .lexical and semantic != null and semantic.? >= request.semantic_threshold)) continue;
            try items.append(self.a, .{ .view = try self.claimView(record), .lexical_score = lexical_score, .semantic_similarity = semantic });
        }
        std.mem.sort(t.SupersessionCandidate, items.items, {}, struct {
            fn less(_: void, left: t.SupersessionCandidate, right: t.SupersessionCandidate) bool {
                const l = @max(left.lexical_score, left.semantic_similarity orelse 0);
                const r = @max(right.lexical_score, right.semantic_similarity orelse 0);
                return l > r or (l == r and std.mem.lessThan(u8, left.view.memory.assertion.id, right.view.memory.assertion.id));
            }
        }.less);
        page.truncated = found.more or items.items.len > request.limit;
        page.items = items.items[0..@min(items.items.len, request.limit)];
        return self.renderSupersessionCandidates(page, request.max_bytes);
    }
    /// Size the ranked prefix until its first excluded item, then emit once.
    /// Discarded suffixes never become serialized buffers in the request arena.
    fn renderSupersessionCandidates(self: *Run, page: t.SupersessionCandidatePage, max_bytes: usize) ![]const u8 {
        try self.service.context.check();
        std.debug.assert(page.items.len <= 64);
        var result = page;
        result.items = &.{};
        const original_envelope = try encodedSize(result);
        result.truncated = true;
        const trimmed_envelope = if (page.truncated) original_envelope else try encodedSize(result);
        if (trimmed_envelope > max_bytes) return error.MemoryInputTooLarge;
        var count: usize = 0;
        var item_bytes: usize = 0;
        for (page.items) |item| {
            try self.service.context.check();
            const next_bytes = item_bytes + try encodedSize(item);
            if (trimmed_envelope + next_bytes + count > max_bytes) break;
            item_bytes = next_bytes;
            count += 1;
        }
        const separators = count -| 1;
        result.truncated = page.truncated or count < page.items.len or original_envelope + item_bytes + separators > max_bytes;
        const total = (if (result.truncated) trimmed_envelope else original_envelope) + item_bytes + separators;
        result.items = page.items[0..count];
        try self.service.context.check();
        const output = try self.a.alloc(u8, total);
        var writer: std.Io.Writer = .fixed(output);
        try std.json.Stringify.value(result, .{}, &writer);
        std.debug.assert(writer.end == total);
        return output;
    }
    fn supersessionOpenKey(self: *Run, id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "supersession-open/{s}/{s}", .{ try hash(self.a, self.service.context.branch), try hash(self.a, id) });
    }
    fn supersessionProposalRetained(self: *Run, proposal: t.SupersessionProposal) !bool {
        for ([_]t.ClaimPin{ proposal.incoming, proposal.previous }) |pin| {
            const record = (try self.assertion(pin.id)) orelse return false;
            self.authorize(record.owner, record.assertion.visibility) catch return false;
            if (!try self.retained(record, 0)) return false;
        }
        for (proposal.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
            if (!try self.evidenceRetained(source)) return false;
        }
        return true;
    }
    fn supersessionProposal(self: *Run, id: []const u8) !t.SupersessionProposal {
        try self.service.context.requireGrant(.curator);
        const proposal = (try self.read(t.SupersessionProposal, try key(self.a, "supersession-proposal", id))) orelse return error.MemoryNotFound;
        if (!eql(u8, proposal.branch, self.service.context.branch) or !try self.supersessionProposalRetained(proposal)) return error.MemoryNotFound;
        return proposal;
    }
    fn supersessionProposalDetail(self: *Run, id: []const u8) !t.SupersessionProposalDetail {
        const proposal = try self.supersessionProposal(id);
        return .{ .proposal = proposal, .incoming = try self.pinnedClaim(proposal.incoming, false, proposal.config_generation), .previous = try self.pinnedClaim(proposal.previous, false, proposal.config_generation), .stale_configuration = proposal.config_generation != self.state.config.generation };
    }
    fn supersessionProposals(self: *Run, request: operations.Page) ![]const u8 {
        try self.service.context.requireGrant(.curator);
        if (request.limit == 0 or request.limit > 256) return error.InvalidMemoryRequest;
        const prefix = try std.fmt.allocPrint(self.a, "supersession-open/{s}/", .{try hash(self.a, self.service.context.branch)});
        const page = try self.recordPage(prefix, try self.decodeCursor(prefix, request.cursor), request.limit);
        var items: std.ArrayList(t.SupersessionProposal) = .empty;
        for (page.rows) |row| {
            const id = try parse([]const u8, self.a, row.json);
            const proposal = (try self.read(t.SupersessionProposal, try key(self.a, "supersession-proposal", id))) orelse continue;
            if (proposal.verdict == null and try self.supersessionProposalRetained(proposal)) try items.append(self.a, proposal);
        }
        return self.encode(t.SupersessionProposalPage{ .items = items.items, .next_cursor = try self.encodeCursor(prefix, page.next), .complete = page.next == null, .revision = self.state.revision });
    }
    fn proposeSupersession(self: *Run, request: t.SupersessionProposalRequest, receipt: *t.Operation) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try t.validateId(request.id);
        if (request.actor.len > 0) try t.validateId(request.actor);
        if (try self.read(bool, try key(self.a, "forgotten-supersession-proposal", request.id)) != null) return error.MemoryForgotten;
        if (try self.read(t.SupersessionProposal, try key(self.a, "supersession-proposal", request.id)) != null) return error.MemoryEntityConflict;
        const incoming = try self.pinnedClaim(request.incoming, true, self.state.config.generation);
        const previous = try self.pinnedClaim(request.previous, true, self.state.config.generation);
        if (incoming.memory.lifecycle != .active or previous.memory.lifecycle != .active or !try self.claimsOverlap(incoming.memory, previous.memory)) return error.MemoryInvalidSupersession;
        try self.put(try key(self.a, "supersession-proposal", request.id), t.SupersessionProposal{ .branch = self.service.context.branch, .id = request.id, .incoming = request.incoming, .previous = request.previous, .owner = self.service.context.principal, .actor = request.actor, .config_generation = self.state.config.generation, .revision = self.state.revision, .recorded_at = self.service.context.now_ms });
        try self.put(try self.supersessionOpenKey(request.id), request.id);
        receipt.proposal_id = request.id;
        if (request.actor.len > 0) receipt.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
    }
    fn reviewSupersession(self: *Run, id: []const u8, request: t.SupersessionVerdict, receipt: *t.Operation) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        if (request.reason.len > t.max_text_bytes or !std.unicode.utf8ValidateSlice(request.reason)) return error.InvalidMemoryRequest;
        if (request.actor.len > 0) try t.validateId(request.actor);
        if (request.model) |model| {
            try t.validateId(model.provider);
            try t.validateId(model.model);
            try t.validateId(model.revision);
        }
        var proposal = try self.supersessionProposal(id);
        if (proposal.verdict != null) return error.MemoryEntityConflict;
        if (proposal.config_generation != self.state.config.generation) return error.MemoryConfigurationChanged;
        const incoming_view = try self.pinnedClaim(proposal.incoming, true, proposal.config_generation);
        const previous_view = try self.pinnedClaim(proposal.previous, true, proposal.config_generation);
        var incoming = (try self.assertion(proposal.incoming.id)).?;
        var previous = (try self.assertion(proposal.previous.id)).?;
        if (incoming.lifecycle != .active or previous.lifecycle != .active or !try self.claimsOverlap(incoming, previous)) return error.MemoryInvalidSupersession;
        try self.reviewEvidence(request.evidence);
        if (request.verdict) |verdict| {
            const valid = incoming.assertion.valid_from;
            const incoming_authority = (try self.authority(incoming.assertion, valid)) orelse return error.MemoryForgotten;
            const previous_authority = (try self.authority(previous.assertion, valid)) orelse return error.MemoryForgotten;
            if (incoming.assertion.visibility == .team) for (request.evidence) |ref| {
                if ((try self.evidence(ref.id)).?.evidence.visibility != .team) return error.MemoryVisibilityConflict;
            };
            if (verdict == .supersedes or verdict == .contradicts) {
                if (incoming_authority < previous_authority or incoming.assertion.modality != .actual or incoming.assertion.role == .hypothesized or incoming.assertion.stability != .settled or previous.assertion.modality != .actual) return error.MemoryInvalidSupersession;
            }
            if (verdict == .supersedes) {
                // Similarity and an outside judgment cannot broaden the native
                // replacement policy's subject, predicate or scope partition.
                if (!try self.sameCanonicalGroup(incoming.assertion, previous.assertion)) return error.MemoryInvalidSupersession;
                if (incoming.assertion.valid_to) |end| if (previous.asserted_valid_to == null or end < previous.asserted_valid_to.?) return error.MemoryInvalidSupersession;
                var claim = incoming.assertion;
                claim.supersedes = &.{previous.assertion.id};
                claim.reconciliation_reason = request.reason;
                try self.applySupersessions(claim, incoming.owner, null);
                var links: std.ArrayList([]const u8) = .empty;
                try links.appendSlice(self.a, incoming.assertion.supersedes);
                for (links.items) |link| if (eql(u8, link, previous.assertion.id)) return error.MemoryInvalidSupersession;
                if (links.items.len >= t.max_items) return error.MemoryWorkLimit;
                try links.append(self.a, previous.assertion.id);
                incoming.assertion.supersedes = links.items;
                incoming.assertion.reconciliation_reason = request.reason;
                incoming.revision = self.state.revision;
                try self.putAssertion(incoming);
            } else if (verdict == .contradicts) {
                try self.conflict(&previous, incoming.assertion.id, request.reason);
                try self.conflict(&incoming, previous.assertion.id, request.reason);
                for ([_]*t.StoredAssertion{ &incoming, &previous }) |record| {
                    record.revision = self.state.revision;
                    record.explicit_disposition = explicitDisposition(record.*);
                    record.explicit_disposition.?.provenance = .{ .principal = self.service.context.principal, .recorded_at = self.service.context.now_ms, .evidence = request.evidence };
                    try self.putAssertion(record.*);
                    try self.invalidate("assertion", record.assertion.id);
                }
            }
            // Restatement records a relationship without merging claim IDs or
            // discarding independent citations. Independent is an explicit no-op.
            try self.put(try std.fmt.allocPrint(self.a, "claim-verdict/{s}/{s}", .{ try hash(self.a, incoming.assertion.id), try hash(self.a, previous.assertion.id) }), .{ .proposal_id = id, .relation = verdict });
            try self.remove(try self.supersessionOpenKey(id));
        }
        for ([_]t.ClaimView{ incoming_view, previous_view }) |view| try self.decision_targets.append(self.a, try self.resourceRef(.assertion, view.memory.assertion.id, view.memory.revision));
        proposal.verdict = request.verdict;
        proposal.verdict_revision = self.state.revision;
        proposal.verdict_principal = self.service.context.principal;
        proposal.verdict_actor = request.actor;
        proposal.reason = request.reason;
        proposal.evidence = request.evidence;
        proposal.model = request.model;
        proposal.revision = self.state.revision;
        try self.put(try key(self.a, "supersession-proposal", id), proposal);
        receipt.proposal_id = id;
        if (request.actor.len > 0) receipt.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
    }
    fn objectTyped(self: *Run, predicate: []const u8) bool {
        if (self.state.config.ontology_pack == .software_team_v1 and isConceptRelation(predicate)) return true;
        for (self.state.config.predicates) |definition| if (eql(u8, definition.name, predicate)) return definition.object_type != null;
        return false;
    }
    const identity_template = "{{_key}}";
    fn identityGet(ptr: *anyopaque, _: A, path: []const u8) !?[]const u8 {
        const self: *Run = @ptrCast(@alignCast(ptr));
        if (self.change_positions.get(path)) |position| return self.changes.items[position].json;
        if (self.backend_rows.getEntry(path)) |entry| return entry.value_ptr.*;
        const row = try self.service.backend.get(self.service.backend.ptr, self.a, path);
        const raw = if (row) |value| value.json else null;
        try self.backend_rows.put(self.a, try self.a.dupe(u8, path), raw);
        return raw;
    }
    fn identityPut(ptr: *anyopaque, _: A, path: []const u8, raw: []const u8) !void {
        const self: *Run = @ptrCast(@alignCast(ptr));
        try self.stageChange(path, raw);
    }
    fn identityRemove(ptr: *anyopaque, _: A, path: []const u8) !void {
        const self: *Run = @ptrCast(@alignCast(ptr));
        try self.stageChange(path, null);
    }
    fn identityScan(ptr: *anyopaque, _: A, prefix: []const u8, after: ?[]const u8, limit: u32) !identity_records.Page {
        const self: *Run = @ptrCast(@alignCast(ptr));
        const page = try self.scanOverlay(prefix, after, limit);
        const rows = try self.a.alloc(identity_records.Row, page.rows.len);
        for (page.rows, rows) |row, *out| out.* = .{ .key = row.key, .value = row.json };
        return .{ .rows = rows, .next = page.next };
    }
    fn identityStore(self: *Run) identity_records.Store {
        return .{ .a = self.a, .prefix = "entity-authority/", .backend = .{ .ptr = self, .get = identityGet, .put = identityPut, .remove = identityRemove, .scan = identityScan }, .max_namespace_bytes = null, .max_queue_bytes = null, .max_queue_count = null };
    }
    fn clearIdentityCaches(self: *Run) void {
        self.identity_policy = null;
        self.identity_index = null;
        self.suppression_index = null;
        self.identity_forward = .empty;
        self.identity_scopes = .empty;
        self.identity_full_scopes = .empty;
        self.identity_legacy_scope = null;
        self.identity_members = .empty;
    }
    fn ensureIdentityAuthority(self: *Run) !void {
        const store = self.identityStore();
        self.state.identity_format = 3;
        if (try store.head(identity_template) != null) return;
        const legacy = (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
        var state = legacy.state;
        state.proposals = &.{};
        state.receipt_scope = null;
        try store.adoptNamespace(.{ .record_store = true, .key_template = identity_template, .revision = self.state.revision -| 1, .merges = legacy.redirects, .state = state });
        if (legacy.state.proposals.len > 0 or legacy.state.receipt_scope != null) {
            try store.applyQueue(.{ .expected_revision = 0, .revision = 1, .receipt_scope = legacy.state.receipt_scope, .proposals = legacy.state.proposals });
            for (legacy.state.proposals) |proposal| try self.put(try std.fmt.allocPrint(self.a, "proposal-binding-id/{s}", .{try hash(self.a, proposal.id)}), proposal.id);
        }
        try self.remove("entity-policy");
    }
    fn identityProposal(self: *Run, id: []const u8) !?curation.Proposal {
        const store = self.identityStore();
        if (try store.head(identity_template) != null) return store.proposal(id);
        const legacy = (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
        for (legacy.state.proposals) |proposal| if (eql(u8, proposal.id, id)) return proposal;
        return null;
    }
    fn identityPolicy(self: *Run, seeds: []const []const u8, proposal_id: ?[]const u8) !EntityPolicy {
        const store = self.identityStore();
        const snapshot = (try store.component(identity_template, seeds)) orelse return (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
        var keys: std.ArrayList([]const u8) = .empty;
        try keys.appendSlice(self.a, seeds);
        for (snapshot.namespace.merges) |alias| {
            try keys.append(self.a, alias.key);
            try keys.append(self.a, alias.target);
        }
        try keys.appendSlice(self.a, snapshot.namespace.state.suppress.keys);
        var state = snapshot.namespace.state;
        const queue = try store.proposalHeadersForKeys(keys.items);
        var proposals: std.ArrayList(curation.Proposal) = .empty;
        for (queue.proposals) |proposal| {
            if (self.reviewing_entity) |selected| if (eql(u8, selected, proposal.id)) continue;
            try proposals.append(self.a, proposal);
        }
        if (proposal_id) |id| if (try store.proposal(id)) |proposal| {
            if (for (proposals.items) |value| {
                if (eql(u8, value.id, id)) break false;
            } else true) try proposals.append(self.a, proposal);
        };
        state.proposals = proposals.items;
        state.receipt_scope = queue.receipt_scope;
        return .{ .state = state, .redirects = snapshot.namespace.merges };
    }
    fn writeIdentityPolicy(self: *Run, before: EntityPolicy, next: EntityPolicy, seeds: []const []const u8) !void {
        const store = self.identityStore();
        const current = (try store.component(identity_template, seeds)) orelse return error.MemoryCorruptState;
        var state = next.state;
        state.proposals = &.{};
        state.receipt_scope = null;
        var namespace = current.namespace;
        namespace.state = state;
        namespace.merges = next.redirects;
        namespace.revision = try std.math.add(u64, current.head.revision, 1);
        const patch = try identity_records.difference(self.a, current.namespace, namespace);
        if (patch.edits.len > 0 or patch.next.min_text_chars != current.head.min_text_chars) try store.apply(patch);
        var old: std.StringHashMapUnmanaged(curation.Proposal) = .empty;
        for (before.state.proposals) |proposal| try old.put(self.a, proposal.id, proposal);
        var changed: std.ArrayList(curation.Proposal) = .empty;
        var full: std.ArrayList(curation.Proposal) = .empty;
        for (next.state.proposals) |proposal| {
            if (old.get(proposal.id)) |previous| if (eql(u8, try self.encode(previous), try self.encode(proposal))) continue;
            var header = proposal;
            header.evidence = null;
            const lifecycle_only = if (old.get(proposal.id)) |previous| blk: {
                var normalized = previous;
                normalized.evidence = null;
                normalized.state = header.state;
                normalized.decided = header.decided;
                break :blk eql(u8, try self.encode(normalized), try self.encode(header));
            } else false;
            // Refreshing an open proposal may replace its original inputs;
            // lifecycle edits retain evidence while refresh/create owns a body.
            if (lifecycle_only) try changed.append(self.a, header) else try full.append(self.a, proposal);
            try self.put(try std.fmt.allocPrint(self.a, "proposal-binding-id/{s}", .{try hash(self.a, proposal.id)}), proposal.id);
        }
        if (changed.items.len > 0) {
            const head = try store.queueHead();
            try store.applyQueue(.{ .expected_revision = head.revision, .revision = try std.math.add(u64, head.revision, 1), .receipt_scope = next.state.receipt_scope, .proposals = changed.items, .retain_evidence = true });
        }
        if (full.items.len > 0) {
            const head = try store.queueHead();
            try store.applyQueue(.{ .expected_revision = head.revision, .revision = try std.math.add(u64, head.revision, 1), .receipt_scope = next.state.receipt_scope, .proposals = full.items });
        }
    }
    fn prepareIdentityIndex(self: *Run, seeds: []const []const u8, full: bool) !void {
        if (seeds.len != 1) return error.MemoryCorruptState;
        const id = seeds[0];
        // A copied/rolled-back Run must not inherit newer overlay scopes.
        if (self.identity_index == null) {
            self.identity_scopes = .empty;
            self.identity_full_scopes = .empty;
            self.identity_legacy_scope = null;
            self.identity_members = .empty;
        }
        const cached = self.identity_legacy_scope orelse self.identity_full_scopes.get(id) orelse if (!full) self.identity_scopes.get(id) else null;
        const scope = cached orelse blk: {
            const store = self.identityStore();
            const snapshot = if (full) try store.component(identity_template, seeds) else try store.forwardScope(identity_template, seeds);
            self.identity_native = snapshot != null;
            const policy = if (snapshot) |value| EntityPolicy{ .redirects = value.namespace.merges, .state = value.namespace.state } else (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
            const result = try self.a.create(IdentityScope);
            const redirects = try curation.RedirectIndex.init(self.a, policy.redirects);
            var suppression = try curation.Index.init(self.a, "", try self.encode(policy.state));
            try suppression.bindRedirects(self.a, redirects);
            result.* = .{ .policy = policy, .redirects = redirects, .suppression = suppression };
            if (snapshot == null) {
                // Legacy blobs are decoded and indexed once per operation.
                self.identity_legacy_scope = result;
                var groups: std.StringHashMapUnmanaged(std.ArrayList([]const u8)) = .empty;
                for (policy.redirects) |alias| {
                    const target = redirects.canonical(alias.key);
                    const entry = try groups.getOrPut(self.a, target);
                    if (!entry.found_existing) {
                        entry.value_ptr.* = .empty;
                        try entry.value_ptr.append(self.a, target);
                    }
                    if (!eql(u8, alias.key, target)) try entry.value_ptr.append(self.a, alias.key);
                }
                var it = groups.iterator();
                while (it.next()) |entry| {
                    std.mem.sort([]const u8, entry.value_ptr.items, {}, stringLess);
                    try self.identity_members.put(self.a, entry.key_ptr.*, entry.value_ptr.items);
                }
            } else if (full) {
                const target = redirects.canonical(id);
                var members: std.ArrayList([]const u8) = .empty;
                try members.append(self.a, target);
                try self.identity_full_scopes.put(self.a, target, result);
                for (policy.redirects) |alias| if (eql(u8, redirects.canonical(alias.key), target)) {
                    try self.identity_full_scopes.put(self.a, alias.key, result);
                    if (!eql(u8, alias.key, target)) try members.append(self.a, alias.key);
                };
                std.mem.sort([]const u8, members.items, {}, stringLess);
                try self.identity_members.put(self.a, target, members.items);
                try self.identity_full_scopes.put(self.a, id, result);
            } else try self.identity_scopes.put(self.a, id, result);
            break :blk result;
        };
        if (full and self.identity_legacy_scope != null) {
            const target = scope.redirects.canonical(id);
            if (!self.identity_members.contains(target)) {
                const member = try self.a.alloc([]const u8, 1);
                member[0] = target;
                try self.identity_members.put(self.a, target, member);
            }
        }
        self.identity_policy = scope.policy;
        self.identity_index = scope.redirects;
        self.suppression_index = scope.suppression;
    }
    fn forwardIdentity(self: *Run, id: []const u8) !ForwardIdentity {
        if (self.identity_forward.get(id)) |cached| return cached;
        const store = self.identityStore();
        if (try store.head(identity_template) == null) {
            try self.prepareIdentityIndex(&.{id}, false);
            const target = self.identity_index.?.canonical(id);
            return .{ .canonical = target, .suppressed = self.suppression_index.?.keySuppressed(id), .target_suppressed = self.suppression_index.?.keySuppressed(target), .distance = 0 };
        }
        const Step = struct { id: []const u8, suppressed: bool };
        var path: std.ArrayList(Step) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var current = id;
        var tail: ForwardIdentity = undefined;
        while (true) {
            if (self.identity_forward.get(current)) |cached| {
                tail = cached;
                break;
            }
            if ((try seen.getOrPut(self.a, current)).found_existing) return error.InvalidResolverAliasCycle;
            if (seen.count() > 16384) return error.ResolverCurationLimitExceeded;
            if (comptime @import("builtin").is_test) self.test_identity_forward_steps += 1;
            const node = try store.forwardNode(identity_template, current);
            const owned = try self.a.dupe(u8, current);
            if (node.target) |next| {
                try path.append(self.a, .{ .id = owned, .suppressed = node.suppressed });
                current = next;
            } else {
                tail = .{ .canonical = owned, .suppressed = node.suppressed, .target_suppressed = node.suppressed, .distance = 0 };
                try self.identity_forward.put(self.a, owned, tail);
                break;
            }
        }
        if (path.items.len + tail.distance >= 16384) return error.ResolverCurationLimitExceeded;
        var remaining = path.items.len;
        while (remaining > 0) {
            remaining -= 1;
            const step = path.items[remaining];
            tail = .{ .canonical = tail.canonical, .suppressed = step.suppressed, .target_suppressed = tail.target_suppressed, .distance = tail.distance + 1 };
            try self.identity_forward.put(self.a, step.id, tail);
        }
        return self.identity_forward.get(id).?;
    }
    fn identityMembers(self: *Run, id: []const u8) ![]const []const u8 {
        try self.prepareIdentityIndex(&.{id}, true);
        return self.identity_members.get(self.identity_index.?.canonical(id)) orelse return error.MemoryCorruptState;
    }
    fn identityPinnedDistinct(self: *Run, from: []const u8, to: []const u8) !bool {
        try self.prepareIdentityIndex(&.{from}, true);
        return self.suppression_index.?.pinnedDistinct(self.a, from, to);
    }
    fn sameCanonicalGroup(self: *Run, left: t.Assertion, right: t.Assertion) !bool {
        var lhs = left;
        var rhs = right;
        lhs.subject = (try self.canonical(left.subject)) orelse return false;
        rhs.subject = (try self.canonical(right.subject)) orelse return false;
        return sameGroup(lhs, rhs);
    }
    fn sameCanonicalValue(self: *Run, left: t.Assertion, right: t.Assertion) !bool {
        if (!self.objectTyped(left.predicate)) return eql(u8, left.value, right.value);
        const lhs = (try self.canonical(left.value)) orelse return false;
        const rhs = (try self.canonical(right.value)) orelse return false;
        return eql(u8, lhs, rhs);
    }
    fn canonicalGroup(self: *Run, claim: t.Assertion, owner: []const u8) ![]const t.StoredAssertion {
        var records: std.ArrayList(t.StoredAssertion) = .empty;
        for (try self.identityMembers(claim.subject)) |subject| {
            var raw = claim;
            raw.subject = subject;
            const prefix = try groupPrefix(self.a, raw, owner);
            if (self.lineage_head) |head| if (head.complete and !self.lineage_raw) {
                for (self.changes.items) |change| if (change.json != null and std.mem.startsWith(u8, change.key, prefix) and try self.lineage_index.?.get(head.root, change.key) == null) {
                    const entry = try parse(struct { id: []const u8 }, self.a, change.json.?);
                    if (try self.assertion(entry.id)) |record| {
                        if (records.items.len >= 4096) return error.MemoryWorkLimit;
                        try records.append(self.a, record);
                    }
                };
            };
            var cursor: ?[]const u8 = null;
            while (true) {
                try self.service.context.check();
                if (self.lineage_head) |head| if (head.complete and !self.lineage_raw) {
                    const page = try self.lineage_index.?.page(head.root, prefix, try std.mem.concat(self.a, u8, &.{ prefix, "~" }), cursor, 128);
                    for (page.items) |item| if (try self.read(t.StoredAssertion, item.value)) |record| {
                        if (records.items.len >= 4096) return error.MemoryWorkLimit;
                        try records.append(self.a, record);
                    };
                    cursor = page.next;
                    if (cursor == null) break;
                    continue;
                };
                const page = try self.scanOverlay(prefix, cursor, 128);
                for (page.rows) |row| {
                    const entry = try parse(struct { id: []const u8 }, self.a, row.json);
                    const record = (try self.assertion(entry.id)) orelse continue;
                    if (records.items.len >= 4096) return error.MemoryWorkLimit;
                    try records.append(self.a, record);
                }
                cursor = page.next;
                if (cursor == null) break;
            }
        }
        return records.items;
    }
    fn collectIdentityGroups(self: *Run, names: *std.StringHashMapUnmanaged(void)) !std.StringHashMapUnmanaged(t.StoredAssertion) {
        var groups: ResourceSet = .empty;
        var affected: std.StringHashMapUnmanaged(t.StoredAssertion) = .empty;
        var iterator = names.keyIterator();
        while (iterator.next()) |name| {
            const prefix = try std.fmt.allocPrint(self.a, "dependency/identity/{s}/", .{try hash(self.a, name.*)});
            var cursor: ?[]const u8 = null;
            while (true) {
                const page = try self.scanOverlay(prefix, cursor, 128);
                for (page.rows) |row| {
                    const entry = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
                    const seed = (try self.assertion(entry.child)) orelse continue;
                    if (!names.contains(seed.assertion.subject) and !(self.objectTyped(seed.assertion.predicate) and names.contains(seed.assertion.value))) continue;
                    var canonical_claim = seed.assertion;
                    canonical_claim.subject = (try self.forwardIdentity(seed.assertion.subject)).canonical;
                    const group = try groupPrefix(self.a, canonical_claim, seed.owner);
                    if ((try groups.getOrPut(self.a, group)).found_existing) continue;
                    for (try self.canonicalGroup(canonical_claim, seed.owner)) |record| {
                        try affected.put(self.a, record.assertion.id, record);
                        if (affected.count() > t.max_items) return error.MemoryWorkLimit;
                    }
                }
                cursor = page.next;
                if (cursor == null) break;
            }
        }
        return affected;
    }
    fn rebuildIdentityGroups(self: *Run, names: *std.StringHashMapUnmanaged(void)) !void {
        var affected = try self.collectIdentityGroups(names);
        try self.rebuildBeliefs(&affected);
    }
    fn rebuildBeliefs(self: *Run, affected: *std.StringHashMapUnmanaged(t.StoredAssertion)) !void {
        var values = affected.valueIterator();
        while (values.next()) |value| {
            var record = value.*;
            // Missing forgotten replacements do not authorize revival.
            var retained_links = true;
            if (record.superseded_by) |id| if (try self.assertion(id)) |linked| {
                if (!try self.retained(linked, 0)) retained_links = false;
            } else {
                retained_links = false;
            };
            for (record.conflicts_with) |id| if (try self.assertion(id)) |linked| {
                if (!try self.retained(linked, 0)) retained_links = false;
            } else {
                retained_links = false;
            };
            if (retained_links) {
                const baseline = record.explicit_disposition orelse t.ExplicitDisposition{ .lifecycle = .active, .valid_to = record.asserted_valid_to, .superseded_by = null, .recorded_to = null, .conflicts_with = &.{}, .reason = null };
                record.lifecycle = baseline.lifecycle;
                record.assertion.valid_to = baseline.valid_to;
                record.superseded_by = baseline.superseded_by;
                record.superseded_valid_at = baseline.superseded_valid_at;
                record.recorded_to = baseline.recorded_to;
                record.conflicts_with = baseline.conflicts_with;
                record.reason = baseline.reason;
            }
            if (record.assertion.kind == .summary or record.assertion.kind == .procedural) record.stale = true;
            record.revision = self.state.revision;
            try self.putAssertion(record);
        }
        values = affected.valueIterator();
        while (values.next()) |value| if (try self.assertion(value.assertion.id)) |record| switch (self.state.config.features.reconciliation) {
            .explicit => {},
            .deterministic => try self.reconcile(record),
            .model => try self.reconcileModel(record),
        };
    }
    fn identityVisible(self: *Run, claim: t.Assertion) anyerror!bool {
        if (try self.canonical(claim.subject) == null) return false;
        if (self.objectTyped(claim.predicate)) {
            if (try self.read(t.EntityRecord, try key(self.a, "entity", claim.value)) == null or try self.canonical(claim.value) == null) return false;
        }
        for (self.state.config.predicates) |predicate| if (eql(u8, predicate.name, claim.predicate)) {
            if (predicate.subject_type != null and try self.read(t.EntityRecord, try key(self.a, "entity", claim.subject)) == null) return false;
            if (predicate.object_type != null) {
                if (try self.read(t.EntityRecord, try key(self.a, "entity", claim.value)) == null or try self.canonical(claim.value) == null) return false;
            }
        };
        return true;
    }
    // Semantic configuration changes are atomic and bounded independently of
    // vector backfills. A rejection leaves the old ontology and beliefs intact.
    fn configurationTransition(self: *Run, replacement: t.Config, publish: bool) !void {
        const previous = self.state.config;
        if (eql(u8, try self.encode(.{ previous.ontology_pack, previous.predicates, previous.source_rules, previous.features.reconciliation, previous.ontology_version, previous.policy_version }), try self.encode(.{ replacement.ontology_pack, replacement.predicates, replacement.source_rules, replacement.features.reconciliation, replacement.ontology_version, replacement.policy_version }))) return;
        self.state.config = replacement;
        defer self.state.config = previous;
        var affected: std.StringHashMapUnmanaged(t.StoredAssertion) = .empty;
        var after: ?[]const u8 = null;
        while (true) {
            try self.service.context.check();
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion/", after, 64);
            for (page.rows) |row| {
                const record = try parse(t.StoredAssertion, self.a, row.json);
                if (!try self.publicationCandidate(record)) continue;
                if (affected.count() >= t.max_items) return error.MemoryWorkLimit;
                try self.validateOntology(record.assertion);
                try affected.put(self.a, record.assertion.id, record);
            }
            after = page.next;
            if (after == null) break;
        }
        if (publish) try self.rebuildBeliefs(&affected);
    }
    fn applyConfiguration(self: *Run, replacement: t.Config) !void {
        try self.configurationTransition(replacement, true);
        if (replacement.retention_ms != self.state.config.retention_ms) self.state.next_expiry_at = self.service.context.now_ms;
        self.state.config = replacement;
        if (self.service.backend.search_assertions != null) try self.put("assertion-recall-index", t.RecallIndex{ .generation = replacement.generation });
    }
    fn canonical(self: *Run, id: []const u8) !?[]const u8 {
        if (try self.read(bool, try key(self.a, "forgotten-entity", id)) != null) return null;
        const input = try self.read(t.EntityRecord, try key(self.a, "entity", id));
        if (input) |record| {
            self.authorize(record.owner, record.entity.visibility) catch return null;
            if (!try self.entityVisible(record)) return null;
        }
        const identity = try self.forwardIdentity(id);
        if (identity.suppressed or identity.target_suppressed) return null;
        const canonical_id = identity.canonical;
        if (!eql(u8, id, canonical_id)) {
            if (input == null) return null;
            const target = (try self.read(t.EntityRecord, try key(self.a, "entity", canonical_id))) orelse return null;
            self.authorize(target.owner, target.entity.visibility) catch return null;
            if (!try self.entityVisible(target)) return null;
        }
        return canonical_id;
    }
    /// Every derived identity projection uses the same temporal admission of
    /// both the mentioned identity and the current canonical destination.
    fn visibleCanonicalEntity(self: *Run, id: []const u8, known: ?i64) !?t.EntityRecord {
        const original = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return null;
        self.authorize(original.owner, original.entity.visibility) catch return null;
        if (known) |time| if ((original.recorded_at orelse 0) > time) return null;
        if (!try self.entityVisible(original)) return null;
        const target = (try self.canonical(id)) orelse return null;
        const record = (try self.read(t.EntityRecord, try key(self.a, "entity", target))) orelse return null;
        self.authorize(record.owner, record.entity.visibility) catch return null;
        if (known) |time| if ((record.recorded_at orelse 0) > time) return null;
        if (!try self.entityVisible(record)) return null;
        return record;
    }
    fn requireDistinctIdentities(self: *Run, names: *std.StringHashMapUnmanaged(void), redirects: curation.RedirectIndex) !void {
        var members = names.keyIterator();
        while (members.next()) |id| {
            try self.service.context.check();
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", id.*))) orelse continue;
            for (record.entity.distinct_from) |distinct| if (eql(u8, redirects.canonical(id.*), redirects.canonical(distinct))) return error.MemoryEntityConflict;
        }
    }
    fn proposalInputsRetained(self: *Run, binding: ProposalBinding) !bool {
        if (!try self.viewInputsRetained(binding.views)) return false;
        for (binding.entities) |ref| {
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", ref.id))) orelse return false;
            self.authorize(record.owner, record.entity.visibility) catch return false;
            if (record.revision != ref.revision or !(if (self.retention_maintenance) try self.entityRetained(record) else try self.entityVisible(record))) return false;
        }
        for (binding.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
            if (!try self.evidenceRetained(source)) return false;
        }
        return true;
    }
    fn entityViewInput(self: *Run, id: []const u8) !t.EntityViewInput {
        const record = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return error.MemoryNotFound;
        try self.authorize(record.owner, record.entity.visibility);
        if (!try self.entityVisible(record)) return error.MemoryNotFound;
        const selected = try self.canonical(id);
        if (selected == null) return .{ .id = id, .canonical_id = null, .fingerprint = try hash(self.a, .{ id, record.revision, self.state.config.generation, selected }), .dependencies = try self.a.dupe(t.DerivedDependency, &.{.{ .resource = try self.resourceRef(.entity, id, record.revision), .evidence = record.entity.evidence }}) };
        const view = try self.entityView(id, .{});
        return .{ .id = id, .canonical_id = view.canonical_id, .fingerprint = view.fingerprint, .dependencies = view.dependencies };
    }
    fn captureEntityViews(self: *Run, ids: []const []const u8, expected: []const t.EntityViewPin) ![]const t.EntityViewInput {
        var inputs: std.ArrayList(t.EntityViewInput) = .empty;
        for (ids) |id| {
            var duplicate = false;
            for (inputs.items) |input| if (eql(u8, input.id, id)) {
                duplicate = true;
                break;
            };
            if (!duplicate) try inputs.append(self.a, try self.entityViewInput(id));
        }
        try self.checkViewPins(inputs.items, expected);
        try self.judged_views.appendSlice(self.a, inputs.items);
        return inputs.items;
    }
    fn checkViewPins(self: *Run, inputs: []const t.EntityViewInput, expected: []const t.EntityViewPin) !void {
        _ = self;
        if (expected.len == 0) return;
        if (expected.len != inputs.len) return error.InvalidMemoryRequest;
        for (inputs) |input| {
            var found: usize = 0;
            for (expected) |pin| if (eql(u8, pin.id, input.id)) {
                if (!eql(u8, pin.fingerprint, input.fingerprint)) return error.MemoryResourceChanged;
                found += 1;
            };
            if (found != 1) return error.InvalidMemoryRequest;
        }
    }
    fn bindJudgedViews(self: *Run, stored: ?[]const t.EntityViewInput, expected: []const t.EntityViewPin) !void {
        // Legacy attempts without an admitted view snapshot require an explicit
        // proposal refresh. Never silently judge a freshly reconstructed view.
        const inputs = stored orelse return error.MemoryResourceChanged;
        try self.checkViewPins(inputs, expected);
        for (inputs) |input| {
            const current = try self.entityViewInput(input.id);
            if (!eql(u8, try self.encode(input), try self.encode(current))) return error.MemoryResourceChanged;
        }
        try self.judged_views.appendSlice(self.a, inputs);
    }
    /// Reconstruct the admitted read cut independently of this verdict's
    /// overlay and every cache derived from that overlay.
    fn admittedViewRun(self: *Run) Run {
        var before = self.*;
        before.changes = .empty;
        before.change_positions = .empty;
        before.change_ranges = .{};
        before.assertion_rows = .empty;
        before.evidence_rows = .empty;
        before.entity_rows = .empty;
        before.projection_rows = .empty;
        before.backend_rows = .empty;
        before.clearIdentityCaches();
        before.views = .empty;
        before.historical = .empty;
        before.temporal_tracking = true;
        before.temporal_boundary = null;
        before.semantic_assertions = .empty;
        before.service.context.now_ms = self.service.context.currentTime();
        return before;
    }
    fn validateJudgedViews(self: *Run) !void {
        if (self.judged_views.items.len == 0) return;
        // Validate against the admitted state, excluding the verdict's own
        // redirect/assertion overlay. The control CAS fences concurrent writes.
        var before = self.admittedViewRun();
        for (self.judged_views.items) |input| {
            const current = try before.entityViewInput(input.id);
            if (!eql(u8, try self.encode(input), try self.encode(current))) return error.MemoryResourceChanged;
        }
        // A validity boundary crossed during the final reads also invalidates
        // the judgment, even if rendering began before that boundary.
        if (before.temporal_boundary) |boundary| if (boundary <= self.service.context.currentTime()) return error.MemoryResourceChanged;
    }
    fn viewInputsRetained(self: *Run, inputs: ?[]const t.EntityViewInput) !bool {
        for (inputs orelse return true) |input| for (input.dependencies) |dependency| {
            const ref = dependency.resource;
            if (ref.kind == .assertion) {
                const record = (try self.read(t.StoredAssertion, try historyKey(self.a, ref.id, ref.revision))) orelse return false;
                self.authorize(record.owner, record.assertion.visibility) catch return false;
                if (!try self.retained(record, 0)) return false;
            } else if (ref.kind == .entity) {
                const record = (try self.read(t.EntityRecord, try key(self.a, "entity", ref.id))) orelse return false;
                self.authorize(record.owner, record.entity.visibility) catch return false;
                if (!try self.entityRetained(record)) return false;
            } else return error.MemoryCorruptState;
        };
        return true;
    }
    const JudgmentContext = struct {
        dependencies: std.ArrayList(t.DerivedDependency) = .empty,
        seen: ResourceSet = .empty,
        evidence: ResourceSet = .empty,
    };
    const SupportSnapshot = enum { current, retained };
    fn snapshotSupport(self: *Run, claim: t.Assertion, mode: SupportSnapshot) !t.Support {
        if (mode == .current) return self.visibleSupport(claim, null, self.service.context.now_ms);
        // A native model receives public records, including authorized
        // historical alternatives. Freeze what was supplied without requiring
        // those historical parents to be active current beliefs.
        if (try self.retainedSupport(claim.evidence, claim.depends_on, 0)) return .{ .evidence = claim.evidence, .depends_on = claim.depends_on };
        for (claim.support_sets) |support| if (try self.retainedSupport(support.evidence, support.depends_on, 0)) return support;
        return error.MemoryEvidenceRequired;
    }
    fn captureJudgmentDependency(self: *Run, context: *JudgmentContext, input: t.DerivedDependency, audience: t.Visibility, depth: u8, mode: SupportSnapshot) anyerror!void {
        if (depth >= max_dependency_depth) return error.MemoryWorkLimit;
        try self.supportStep();
        const path = try hash(self.a, input.resource);
        if (context.seen.contains(path)) return;
        try context.seen.put(self.a, path, {});
        try context.dependencies.append(self.a, input);
        const ref = input.resource;
        var parents: []const []const u8 = &.{};
        if (ref.kind == .assertion) {
            const record = (try self.read(t.StoredAssertion, try historyKey(self.a, ref.id, ref.revision))) orelse return error.MemoryResourceChanged;
            if (audience == .team and record.assertion.visibility == .private) return error.MemoryVisibilityConflict;
            const selected = try self.snapshotSupport(record.assertion, mode);
            parents = selected.depends_on;
        } else if (ref.kind == .entity) {
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", ref.id))) orelse return error.MemoryResourceChanged;
            if (audience == .team and record.entity.visibility == .private) return error.MemoryVisibilityConflict;
        } else return error.MemoryCorruptState;
        for (input.evidence) |source| try context.evidence.put(self.a, source.id, {});
        if (try self.read(GenerationProvenance, try self.generationKey(@tagName(ref.kind), ref.id))) |generation| {
            for (generation.evidence_ids) |source| try context.evidence.put(self.a, source, {});
            // Ancestors already froze their selected transitive closure. Copy
            // it directly, never resolve those inputs against today's parents.
            for (generation.context) |dependency| {
                const inherited_path = try hash(self.a, dependency.resource);
                if (!context.seen.contains(inherited_path)) {
                    try self.supportStep();
                    try context.seen.put(self.a, inherited_path, {});
                    try context.dependencies.append(self.a, dependency);
                    for (dependency.evidence) |source| try context.evidence.put(self.a, source.id, {});
                }
            }
        }
        for (parents) |id| {
            const parent = (try self.assertion(id)) orelse return error.MemoryResourceChanged;
            const selected = try self.snapshotSupport(parent.assertion, mode);
            try self.captureJudgmentDependency(context, .{ .resource = try self.resourceRef(.assertion, id, parent.revision), .evidence = selected.evidence }, audience, depth + 1, mode);
        }
    }
    // The candidate payload and every support branch supplied to inference
    // are causal inputs even when none appears in an entity's rendered view.
    fn captureClaimInputs(self: *Run, context: *JudgmentContext, claim: t.Assertion, audience: t.Visibility, mode: SupportSnapshot) !void {
        try self.captureSupport(context, .{ .evidence = claim.evidence, .depends_on = claim.depends_on }, audience, mode);
        for (claim.support_sets) |support| try self.captureSupport(context, support, audience, mode);
        inline for (assertion_reference_fields) |field| try self.captureAssertionReferences(context, @field(claim, field), audience);
    }
    fn captureRecordInputs(self: *Run, context: *JudgmentContext, record: t.StoredAssertion, audience: t.Visibility) !void {
        try self.captureClaimInputs(context, record.assertion, audience, .retained);
        try self.captureDispositionInputs(context, record, audience);
        if (record.explicit_disposition) |baseline| {
            try self.captureDispositionInputs(context, baseline, audience);
            // Correction attribution is part of the public model payload even
            // when its free-text reason is empty.
            if (baseline.provenance) |provenance| try self.captureSupport(context, .{ .evidence = provenance.evidence }, audience, .retained);
        }
    }
    fn captureDispositionInputs(self: *Run, context: *JudgmentContext, disposition: anytype, audience: t.Visibility) !void {
        try self.captureAssertionReferences(context, disposition.conflicts_with, audience);
        if (disposition.superseded_by) |id| try self.captureAssertionReferences(context, &.{id}, audience);
    }
    fn captureAssertionReferences(self: *Run, context: *JudgmentContext, ids: []const []const u8, audience: t.Visibility) !void {
        for (ids) |id| {
            const record = (try self.assertion(id)) orelse return error.MemoryResourceChanged;
            try self.authorize(record.owner, record.assertion.visibility);
            if (!try self.retained(record, 0)) return error.MemoryResourceChanged;
            // An annotation can refer to an authorized historical assertion.
            // Pin its revision and selected support, but do not recursively
            // follow annotations: they are identifiers, not expanded payloads.
            const support = try self.snapshotSupport(record.assertion, .retained);
            try self.captureJudgmentDependency(context, .{ .resource = try self.resourceRef(.assertion, id, record.revision), .evidence = support.evidence }, audience, 0, .retained);
        }
    }
    fn captureSupport(self: *Run, context: *JudgmentContext, support: t.Support, audience: t.Visibility, mode: SupportSnapshot) !void {
        for (support.evidence) |ref| {
            try self.supportStep();
            try context.evidence.put(self.a, ref.id, {});
        }
        for (support.depends_on) |id| {
            const parent = (try self.assertion(id)) orelse return error.MemoryResourceChanged;
            const selected = try self.snapshotSupport(parent.assertion, mode);
            try self.captureJudgmentDependency(context, .{ .resource = try self.resourceRef(.assertion, id, parent.revision), .evidence = selected.evidence }, audience, 0, mode);
        }
    }
    fn judgmentEvidence(self: *Run, context: *JudgmentContext, audience: t.Visibility) ![]const []const u8 {
        const ids = try self.a.alloc([]const u8, context.evidence.count());
        var sources = context.evidence.keyIterator();
        var i: usize = 0;
        while (sources.next()) |source| : (i += 1) {
            ids[i] = source.*;
            const input = (try self.evidence(source.*)) orelse return error.MemoryForgotten;
            if (audience == .team and input.evidence.visibility == .private) return error.MemoryVisibilityConflict;
        }
        std.mem.sort([]const u8, ids, {}, stringLess);
        return ids;
    }
    fn storeJudgment(self: *Run, views: []const t.EntityViewInput, refs: []const t.EvidenceRef, candidate: ?t.Assertion, audience: t.Visibility, reason: []const u8, receipt: t.Operation, output_assertion_id: ?[]const u8) !JudgmentArtifact {
        var context: JudgmentContext = .{};
        for (refs) |ref| try context.evidence.put(self.a, ref.id, {});
        for (views) |view| for (view.dependencies) |dependency| try self.captureJudgmentDependency(&context, dependency, audience, 0, .current);
        if (candidate) |claim| try self.captureClaimInputs(&context, claim, audience, .current);
        const ids = try self.judgmentEvidence(&context, audience);
        const artifact: JudgmentArtifact = .{
            .input_version = judgment_input_version,
            .operation = try self.resourceRef(.operation, receipt.id, receipt.revision),
            .inputs = .{ .operation_id = receipt.id, .config_generation = self.state.config.generation, .evidence_ids = ids, .context = context.dependencies.items, .views = views },
            .reason = reason,
            .output_assertion_id = output_assertion_id,
        };
        try self.put(try key(self.a, "judgment", receipt.id), artifact);
        return artifact;
    }
    fn judgmentAvailable(self: *Run, artifact: JudgmentArtifact, comptime view: RecordView) !bool {
        if (artifact.input_version != judgment_input_version) return false;
        if (artifact.output_assertion_id) |id| if (try self.read(bool, try key(self.a, "forgotten-assertion", id)) != null) return false;
        return self.provenanceAvailable(artifact.inputs, view);
    }
    fn relationshipJudgment(self: *Run, proposal: t.RelationshipProposal) !?JudgmentArtifact {
        const revision = proposal.verdict_revision orelse return null;
        const ref = (try self.read(prov.Resource, try key(self.a, "relationship-judgment", proposal.id))) orelse return null;
        if (ref.revision != revision) return error.MemoryCorruptState;
        const artifact = (try self.read(JudgmentArtifact, try key(self.a, "judgment", ref.id))) orelse return null;
        if (artifact.operation.revision != revision or !eql(u8, artifact.operation.id, ref.id)) return error.MemoryCorruptState;
        return artifact;
    }
    fn redactVerdict(proposal: *t.RelationshipProposal) void {
        proposal.reason = "";
        proposal.verdict_evidence = &.{};
        proposal.verdict_actor = "";
        proposal.verdict_principal = "";
        proposal.model = null;
    }
    fn reviewEvidence(self: *Run, refs: []const t.EvidenceRef) !void {
        if (refs.len > t.max_items) return error.MemoryInputTooLarge;
        for (refs) |ref| {
            const source = (try self.evidence(ref.id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (!try self.evidenceRetained(source) or !try self.resourceCurrent(source)) return error.MemoryForgotten;
            if (ref.span != null and self.intake_redacted_evidence.contains(ref.id)) return error.InvalidMemorySpan;
            if (ref.span) |span| if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
        }
    }
    fn episodeAudience(self: *Run, visibility: t.Visibility, owner: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "episode-access/{s}/", .{try hash(self.a, .{ visibility, if (visibility == .team) "" else owner })});
    }
    fn episodeTurnKey(self: *Run, source: t.Evidence) ![]const u8 {
        // Sign-bit rotation makes lexicographic order agree with signed ordinals.
        const ordered = @as(u64, @bitCast(source.ordinal.?)) ^ (@as(u64, 1) << 63);
        return std.fmt.allocPrint(self.a, "episode-turn/{s}/{d:0>20}/{s}", .{ try hash(self.a, source.thread_id.?), ordered, try hash(self.a, source.id) });
    }
    fn indexEpisode(self: *Run, record: t.StoredEvidence) !void {
        const source = record.evidence;
        const thread = source.thread_id orelse return;
        if (source.ordinal == null) return;
        try t.validateId(thread);
        if (source.turn_id) |turn| try t.validateId(turn);
        const turn = try self.episodeTurnKey(source);
        const catalog = try key(self.a, "episode", thread);
        // The global lane serves administrators; ordinary reads merge only
        // the team lane and their own private lane. Counts keep catalog
        // membership exact without scanning a thread during retirement.
        for ([_][]const u8{ "", try self.episodeAudience(source.visibility, record.owner) }) |root| {
            const turn_path = try std.fmt.allocPrint(self.a, "{s}{s}", .{ root, turn });
            if (try self.read(std.json.Value, turn_path) != null) continue;
            const catalog_path = try std.fmt.allocPrint(self.a, "{s}{s}", .{ root, catalog });
            var entry = (try self.read(EpisodeCatalog, catalog_path)) orelse EpisodeCatalog{ .thread_id = thread };
            if (entry.members == std.math.maxInt(u64)) return error.MemoryCorruptState;
            entry.members += 1;
            try self.put(catalog_path, entry);
            try self.put(turn_path, .{ .id = source.id });
        }
    }
    fn retireEpisode(self: *Run, record: t.StoredEvidence) !void {
        const source = record.evidence;
        const thread = source.thread_id orelse return;
        if (source.ordinal == null) return;
        const turn = try self.episodeTurnKey(source);
        const catalog = try key(self.a, "episode", thread);
        for ([_][]const u8{ "", try self.episodeAudience(source.visibility, record.owner) }) |root| {
            const turn_path = try std.fmt.allocPrint(self.a, "{s}{s}", .{ root, turn });
            if (try self.read(std.json.Value, turn_path) == null) continue;
            try self.remove(turn_path);
            const catalog_path = try std.fmt.allocPrint(self.a, "{s}{s}", .{ root, catalog });
            var entry = (try self.read(EpisodeCatalog, catalog_path)) orelse continue;
            if (entry.members <= 1) try self.remove(catalog_path) else {
                entry.members -= 1;
                try self.put(catalog_path, entry);
            }
        }
    }
    fn scanEpisodeRows(self: *Run, prefix: []const u8, after: ?[]const u8, limit: u32) !Page {
        if (self.service.context.admin) {
            try self.requireAdmin();
            return self.service.backend.scan(self.service.backend.ptr, self.a, prefix, after, limit);
        }
        var rows: std.ArrayList(Row) = .empty;
        var more = false;
        for ([_][]const u8{ try self.episodeAudience(.team, ""), try self.episodeAudience(.private, self.service.context.principal) }) |root| {
            try self.service.context.check();
            const physical_prefix = try std.fmt.allocPrint(self.a, "{s}{s}", .{ root, prefix });
            const cursor = if (after) |position| try std.fmt.allocPrint(self.a, "{s}{s}", .{ root, position }) else null;
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, physical_prefix, cursor, limit);
            more = more or page.next != null;
            for (page.rows) |row| try rows.append(self.a, .{ .key = row.key[root.len..], .json = row.json, .version = row.version, .digest = row.digest });
        }
        std.mem.sort(Row, rows.items, {}, struct {
            fn less(_: void, lhs: Row, rhs: Row) bool {
                return std.mem.lessThan(u8, lhs.key, rhs.key);
            }
        }.less);
        // Catalog entries may occur in both lanes; one shared virtual key
        // also lets turn pages preserve ordinal order across audiences.
        var count: usize = 0;
        for (rows.items) |row| {
            if (count > 0 and eql(u8, rows.items[count - 1].key, row.key)) continue;
            rows.items[count] = row;
            count += 1;
        }
        const selected = @min(count, limit);
        more = more or count > selected;
        return .{ .rows = rows.items[0..selected], .next = if (more and selected > 0) rows.items[selected - 1].key else null };
    }
    fn mention(self: *Run, value: t.EntityMention) !void {
        try t.validateId(value.id);
        try t.validateId(value.entity_id);
        if (value.evidence.len == 0 or value.evidence.len > t.max_items) return error.MemoryEvidenceRequired;
        if (try self.read(bool, try key(self.a, "forgotten-mention", value.id)) != null) return error.MemoryForgotten;
        if (try self.read(t.EntityRecord, try key(self.a, "entity", value.entity_id)) == null) return error.MemoryEntityRequired;
        try self.validateEntityReference(value.entity_id, null, value.visibility);
        if (!try self.supportVisible(value.evidence, &.{}, 0)) return error.MemoryEvidenceRequired;
        for (value.evidence) |ref| {
            const source = (try self.evidence(ref.id)).?;
            if (value.visibility == .team and source.evidence.visibility == .private) return error.MemoryVisibilityConflict;
            if (ref.span != null and self.intake_redacted_evidence.contains(ref.id)) return error.InvalidMemorySpan;
            if (ref.span) |span| if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
        }
        const path = try key(self.a, "mention", value.id);
        if (try self.read(t.StoredMention, path)) |prior| {
            try self.authorize(prior.owner, .private);
            if (!eql(u8, try self.encode(prior.mention), try self.encode(value))) return error.MemoryEntityConflict;
            return;
        }
        try self.put(path, t.StoredMention{ .mention = value, .owner = self.service.context.principal, .revision = self.state.revision, .recorded_at = self.service.context.now_ms });
        for (value.evidence) |ref| try self.put(try dependencyKey(self.a, "mention-evidence", ref.id, value.id), .{ .child = value.id });
        try self.put(try dependencyKey(self.a, "mention-entity", value.entity_id, value.id), .{ .child = value.id });
    }
    fn mentionVisible(self: *Run, record: t.StoredMention, known: ?i64) !bool {
        self.authorize(record.owner, record.mention.visibility) catch return false;
        if (known) |time| if (record.recorded_at > time) return false;
        if (!try self.supportVisibleTime(record.mention.evidence, &.{}, known, null, 0) or !try self.generationVisible("mention", record.mention.id)) return false;
        return try self.visibleCanonicalEntity(record.mention.entity_id, known) != null;
    }
    fn evidenceEntities(self: *Run, source: t.StoredEvidence, known: ?i64) !struct { ids: []const []const u8, complete: bool } {
        var ids: std.StringHashMapUnmanaged(void) = .empty;
        const prefix = try std.fmt.allocPrint(self.a, "dependency/mention-evidence/{s}/", .{try hash(self.a, source.evidence.id)});
        const page = try self.scanOverlay(prefix, null, 64);
        for (page.rows) |row| {
            const binding = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
            const stored = (try self.read(t.StoredMention, try key(self.a, "mention", binding.child))) orelse continue;
            if (!try self.mentionVisible(stored, known)) continue;
            const canonical_id = (try self.canonical(stored.mention.entity_id)) orelse continue;
            try ids.put(self.a, canonical_id, {});
        }
        const result = try self.a.alloc([]const u8, ids.count());
        var keys = ids.keyIterator();
        var i: usize = 0;
        while (keys.next()) |id| : (i += 1) result[i] = id.*;
        std.mem.sort([]const u8, result, {}, stringLess);
        return .{ .ids = result, .complete = page.next == null };
    }
    fn episode(self: *Run, id: []const u8, request: t.EpisodeRequest) !t.Episode {
        try t.validateId(id);
        if (request.limit == 0 or request.limit > 64) return error.InvalidMemoryRequest;
        self.historical_resource_reads = request.known_at != null;
        const prefix = try std.fmt.allocPrint(self.a, "episode-turn/{s}/", .{try hash(self.a, id)});
        const page = try self.scanEpisodeRows(prefix, try self.decodeCursor(prefix, request.cursor), 64);
        var turns: std.ArrayList(t.EpisodeTurn) = .empty;
        var complete = true;
        var next = page.next;
        for (page.rows, 0..) |row, index| {
            const binding = try parse(struct { id: []const u8 }, self.a, row.json);
            const source = (try self.evidence(binding.id)) orelse continue;
            self.authorize(source.owner, source.evidence.visibility) catch continue;
            if (request.known_at) |known| if (source.recorded_at > known) continue;
            if (!try self.evidenceRetained(source) or !try self.resourceCurrent(source)) continue;
            const entities = try self.evidenceEntities(source, request.known_at);
            complete = complete and entities.complete;
            try turns.append(self.a, .{ .evidence = source, .entities = entities.ids });
            if (turns.items.len == request.limit) {
                next = if (index + 1 < page.rows.len or page.next != null) row.key else null;
                break;
            }
        }
        // Foreign private turns never participate in these scans, including
        // their cursor/absence decisions. Empty progress pages are possible
        // only within the caller's own authorized audience (e.g. expired turns).
        if (turns.items.len == 0 and request.cursor == null and next == null) return error.MemoryNotFound;
        return .{ .thread_id = id, .turns = turns.items, .next_cursor = try self.encodeCursor(prefix, next), .complete = complete and next == null, .revision = self.state.revision };
    }
    fn episodeListPosition(self: *Run, cursor: ?[]const u8) !EpisodeListPosition {
        const path = (try self.decodeCursor("episode-list/", cursor)) orelse return .{};
        const encoded = path["episode-list/".len..];
        if (encoded.len > 8192) return error.InvalidMemoryCursor;
        const decoder = std.base64.url_safe_no_pad.Decoder;
        const bytes = try self.a.alloc(u8, decoder.calcSizeForSlice(encoded) catch return error.InvalidMemoryCursor);
        decoder.decode(bytes, encoded) catch return error.InvalidMemoryCursor;
        const position = parse(EpisodeListPosition, self.a, bytes) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidMemoryCursor,
        };
        if (position.after) |after| if (!std.mem.startsWith(u8, after, "episode/")) return error.InvalidMemoryCursor;
        return position;
    }
    fn episodeListCursor(self: *Run, position: EpisodeListPosition) !?[]const u8 {
        const bytes = try self.encode(position);
        const encoder = std.base64.url_safe_no_pad.Encoder;
        const encoded = try self.a.alloc(u8, encoder.calcSize(bytes.len));
        _ = encoder.encode(encoded, bytes);
        return self.encodeCursor("episode-list/", try std.fmt.allocPrint(self.a, "episode-list/{s}", .{encoded}));
    }
    fn episodes(self: *Run, request: t.EpisodeRequest) ![]const u8 {
        if (request.limit == 0 or request.limit > 64) return error.InvalidMemoryRequest;
        var position = try self.episodeListPosition(request.cursor);
        var items: std.ArrayList(t.Episode) = .empty;
        for (0..request.limit) |_| {
            try self.service.context.check();
            // The revision-fenced catalog is immutable across continuation.
            // Resolve its next authorized entry again rather than storing an
            // unproven thread name inside the externally carried cursor.
            const page = try self.scanEpisodeRows("episode/", position.after, 1);
            if (page.rows.len == 0) return self.encode(t.EpisodePage{ .items = items.items, .next_cursor = null, .complete = true, .revision = self.state.revision });
            const thread = (try parse(EpisodeCatalog, self.a, page.rows[0].json)).thread_id;
            const item = self.episode(thread, .{ .limit = 1, .known_at = request.known_at, .cursor = position.pending }) catch |err| switch (err) {
                error.MemoryNotFound => {
                    position = .{ .after = try key(self.a, "episode", thread) };
                    continue;
                },
                else => return err,
            };
            if (item.turns.len == 0 and item.next_cursor != null) {
                // Never advance the outer catalog past an unfinished member
                // scan. The scoped cursor owns both levels of continuation.
                position.pending = item.next_cursor;
                return self.encode(t.EpisodePage{ .items = items.items, .next_cursor = try self.episodeListCursor(position), .complete = false, .revision = self.state.revision });
            }
            if (item.turns.len > 0) try items.append(self.a, item);
            position = .{ .after = try key(self.a, "episode", thread) };
        }
        const remaining = try self.scanEpisodeRows("episode/", position.after, 1);
        return self.encode(t.EpisodePage{ .items = items.items, .next_cursor = if (remaining.rows.len > 0) try self.episodeListCursor(position) else null, .complete = remaining.rows.len == 0, .revision = self.state.revision });
    }
    fn relatedAssertions(self: *Run, id: []const u8, limit: usize) !struct { items: []const t.StoredAssertion, scanned: usize, complete: bool } {
        var records: std.StringHashMapUnmanaged(t.StoredAssertion) = .empty;
        var scanned: usize = 0;
        var complete = true;
        // Components are sorted once when the redirect index is built.
        const names = try self.identityMembers(id);
        for (names) |name| {
            if (scanned + 1 >= limit) {
                complete = false;
                break;
            }
            const prefix = try std.fmt.allocPrint(self.a, "dependency/identity/{s}/", .{try hash(self.a, name)});
            const page = try self.scanOverlay(prefix, null, @intCast(limit - scanned - 1));
            scanned += page.rows.len + 1;
            complete = complete and page.next == null;
            var paths: std.ArrayList([]const u8) = .empty;
            for (page.rows) |row| {
                const entry = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
                try paths.append(self.a, try key(self.a, "assertion", entry.child));
            }
            try self.prefetch(paths.items);
            for (page.rows) |row| {
                const entry = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
                const record = (try self.assertion(entry.child)) orelse continue;
                if (!eql(u8, name, record.assertion.subject) and !(self.objectTyped(record.assertion.predicate) and eql(u8, name, record.assertion.value))) continue;
                try records.put(self.a, record.assertion.id, record);
            }
        }
        const result = try self.a.alloc(t.StoredAssertion, records.count());
        var values = records.valueIterator();
        var i: usize = 0;
        while (values.next()) |record| : (i += 1) result[i] = record.*;
        std.mem.sort(t.StoredAssertion, result, {}, struct {
            fn less(_: void, lhs: t.StoredAssertion, rhs: t.StoredAssertion) bool {
                if (lhs.assertion.stability != rhs.assertion.stability) return lhs.assertion.stability == .settled;
                return if (lhs.recorded_at == rhs.recorded_at) std.mem.lessThan(u8, lhs.assertion.id, rhs.assertion.id) else lhs.recorded_at > rhs.recorded_at;
            }
        }.less);
        return .{ .items = result, .scanned = scanned, .complete = complete };
    }
    fn entityView(self: *Run, id: []const u8, request: t.EntityViewRequest) !t.EntityView {
        return self.entityViewFiltered(id, request, .{ .known_at = request.known_at, .valid_at = request.valid_at });
    }
    fn entityViewFiltered(self: *Run, id: []const u8, request: t.EntityViewRequest, filters: t.Recall) !t.EntityView {
        if (request.limit == 0 or request.limit > 32) return error.InvalidMemoryRequest;
        self.historical_resource_reads = request.known_at != null;
        const requested = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return error.MemoryNotFound;
        try self.authorize(requested.owner, requested.entity.visibility);
        if (!try self.entityVisible(requested)) return error.MemoryNotFound;
        const selected = (try self.canonical(id)) orelse return error.MemoryEntitySuppressed;
        const record = (try self.visibleCanonicalEntity(id, request.known_at)) orelse return error.MemoryNotFound;
        const related = try self.relatedAssertions(selected, 256);
        var dependencies: std.ArrayList(t.DerivedDependency) = .empty;
        try dependencies.append(self.a, .{ .resource = try self.resourceRef(.entity, selected, record.revision), .evidence = record.entity.evidence });
        var text: std.ArrayList(u8) = .empty;
        try text.appendSlice(self.a, record.entity.name);
        var complete = related.complete;
        for (related.items) |original| {
            // Once full, older rows cannot contribute to this rendering. Do
            // not traverse their support merely to discard their text.
            if (dependencies.items.len > request.limit) {
                complete = false;
                break;
            }
            // Historical rendering must size the selected historical row,
            // rather than reject it using today's statement length.
            if (request.known_at == null and text.items.len + original.assertion.statement.len + 2 > 8192) {
                complete = false;
                continue;
            }
            // A shared description has the entity's audience, regardless of
            // whether an administrator or curator renders it. Private support
            // cannot flow into a team projection or another owner's identity.
            if (original.assertion.visibility == .private and (record.entity.visibility == .team or !eql(u8, original.owner, record.owner))) continue;
            const hit = (try self.recallHit(original, filters, null, false)) orelse continue;
            if (dependencies.items.len > request.limit or text.items.len + hit.memory.assertion.statement.len + 2 > 8192) {
                complete = false;
                continue;
            }
            try text.appendSlice(self.a, "\n");
            try text.appendSlice(self.a, hit.memory.assertion.statement);
            try dependencies.append(self.a, .{ .resource = try self.resourceRef(.assertion, hit.memory.assertion.id, hit.memory.revision), .evidence = hit.citations });
        }
        return .{ .entity = record.entity, .canonical_id = selected, .description = text.items, .dependencies = dependencies.items, .fingerprint = try hash(self.a, .{ selected, record.revision, self.state.config.generation, dependencies.items, text.items }), .revision = self.state.revision, .config_generation = self.state.config.generation, .complete = complete, .scanned = related.scanned };
    }
    fn relationshipRetained(self: *Run, proposal: t.RelationshipProposal) !bool {
        if (try self.read(bool, try key(self.a, "forgotten-assertion", proposal.assertion.id)) != null) return false;
        if (!try self.viewInputsRetained(proposal.views)) return false;
        if (try self.read(bool, "forgotten-all") != null) return false;
        if (!try self.retainedSupport(proposal.assertion.evidence, proposal.assertion.depends_on, 0)) return false;
        for ([_][]const u8{ proposal.assertion.subject, proposal.assertion.value }) |id| {
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return false;
            if (!try self.entityRetained(record)) return false;
        }
        return true;
    }
    fn relationshipProposal(self: *Run, id: []const u8) !t.RelationshipProposal {
        var proposal = (try self.read(t.RelationshipProposal, try key(self.a, "relationship-proposal", id))) orelse return error.MemoryNotFound;
        try self.authorize(proposal.owner, proposal.assertion.visibility);
        if (!try self.relationshipRetained(proposal)) return error.MemoryNotFound;
        if (!try self.supportVisible(proposal.assertion.evidence, proposal.assertion.depends_on, 0)) return error.MemoryNotFound;
        for ([_][]const u8{ proposal.assertion.subject, proposal.assertion.value }) |entity_id| {
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", entity_id))) orelse return error.MemoryNotFound;
            try self.authorize(record.owner, record.entity.visibility);
            if (!try self.entityVisible(record)) return error.MemoryNotFound;
        }
        const judgment = try self.relationshipJudgment(proposal);
        if (judgment == null or !try self.judgmentAvailable(judgment.?, .visible) or !try self.supportVisible(proposal.verdict_evidence, &.{}, 0)) {
            redactVerdict(&proposal);
        } else proposal.reason = judgment.?.reason;
        return proposal;
    }
    fn publicViewInputs(self: *Run, inputs: ?[]const t.EntityViewInput) !?[]const t.EntityViewInput {
        const stored = inputs orelse return null;
        const result = try self.a.dupe(t.EntityViewInput, stored);
        for (result) |*input| {
            var dependencies: std.ArrayList(t.DerivedDependency) = .empty;
            for (input.dependencies) |dependency| if (try self.causalContextAvailable(&.{dependency}, false)) try dependencies.append(self.a, dependency);
            input.dependencies = dependencies.items;
        }
        return result;
    }
    fn publicProposalBinding(self: *Run, stored: ProposalBinding) !ProposalBinding {
        var result = stored;
        result.views = try self.publicViewInputs(stored.views);
        return result;
    }
    fn publicRelationshipProposal(self: *Run, id: []const u8) !t.RelationshipProposal {
        var result = try self.relationshipProposal(id);
        result.views = try self.publicViewInputs(result.views);
        try self.publicClaim(&result.assertion);
        return result;
    }
    fn proposeRelationship(self: *Run, request: t.RelationshipProposalRequest, receipt: *t.Operation) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try t.validateId(request.id);
        if (request.actor.len > 0) try t.validateId(request.actor);
        if (try self.read(bool, try key(self.a, "forgotten-relationship-proposal", request.id)) != null) return error.MemoryForgotten;
        if (try self.read(t.RelationshipProposal, try key(self.a, "relationship-proposal", request.id)) != null) return error.MemoryEntityConflict;
        if (!self.objectTyped(request.assertion.predicate)) return error.MemoryOntologyMismatch;
        // A proposal is an immutable candidate, not a second truth store.
        // Multiple support branches complicate the retention of the candidate
        // text itself; external judges submit one independently grounded pair.
        if (request.assertion.support_sets.len != 0 or request.assertion.embedding != null) return error.InvalidMemoryRequest;
        try self.validateAssertion(request.assertion, false);
        if (try self.assertion(request.assertion.id) != null) return error.MemoryAssertionConflict;
        const proposal: t.RelationshipProposal = .{ .views = try self.captureEntityViews(&.{ request.assertion.subject, request.assertion.value }, request.views), .id = request.id, .assertion = request.assertion, .owner = self.service.context.principal, .actor = request.actor, .config_generation = self.state.config.generation, .revision = self.state.revision, .recorded_at = self.service.context.now_ms };
        if (!try self.relationshipRetained(proposal)) return error.MemoryEvidenceRequired;
        try self.put(try key(self.a, "relationship-proposal", request.id), proposal);
        if (request.actor.len > 0) receipt.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
        receipt.proposal_id = request.id;
    }
    fn reviewRelationship(self: *Run, id: []const u8, request: t.EntityVerdict, receipt: *t.Operation) !void {
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        if (request.pin_distinct or request.reason.len > t.max_text_bytes or !std.unicode.utf8ValidateSlice(request.reason)) return error.InvalidMemoryRequest;
        if (request.actor.len > 0) try t.validateId(request.actor);
        if (request.model) |model| {
            try t.validateId(model.provider);
            try t.validateId(model.model);
            try t.validateId(model.revision);
        }
        var proposal = try self.relationshipProposal(id);
        if (proposal.state != .open) return error.MemoryEntityConflict;
        if (proposal.config_generation != self.state.config.generation) return error.MemoryConfigurationChanged;
        try self.reviewEvidence(request.evidence);
        try self.bindJudgedViews(proposal.views, request.views);
        const views = proposal.views orelse return error.MemoryResourceChanged;
        // The candidate is immutable: silently stripping a reference here
        // cannot prove it was absent from the external judge's earlier input.
        // Lost reference access requires a fresh independently grounded proposal.
        const artifact = try self.storeJudgment(views, request.evidence, proposal.assertion, proposal.assertion.visibility, request.reason, receipt.*, proposal.assertion.id);
        if (!try self.judgmentAvailable(artifact, .visible)) return error.MemoryResourceChanged;
        try self.put(try key(self.a, "relationship-judgment", proposal.id), artifact.operation);
        if (request.accept) |accept| {
            if (accept) {
                for (request.evidence) |ref| {
                    const source = (try self.evidence(ref.id)).?;
                    if (proposal.assertion.visibility == .team and source.evidence.visibility == .private) return error.MemoryVisibilityConflict;
                }
                // All external review context is a conjunctive causal input
                // of the accepted assertion, independent of factual support.
                if (try self.assertion(proposal.assertion.id) != null) return error.MemoryAssertionConflict;
                try self.put(try self.generationKey("assertion", proposal.assertion.id), artifact.inputs);
                try self.assertions(&.{proposal.assertion}, false);
                for (artifact.inputs.evidence_ids) |input| try self.put(try dependencyKey(self.a, "evidence", input, proposal.assertion.id), .{ .child = proposal.assertion.id });
                for (artifact.inputs.context) |input| try self.put(try dependencyKey(self.a, @tagName(input.resource.kind), input.resource.id, proposal.assertion.id), .{ .child = proposal.assertion.id });
            }
            proposal.state = if (accept) .accepted else .rejected;
        }
        proposal.verdict_actor = request.actor;
        proposal.verdict_principal = self.service.context.principal;
        proposal.verdict_revision = self.state.revision;
        const verdict_evidence = try self.a.alloc(t.EvidenceRef, proposal.assertion.evidence.len + request.evidence.len);
        if (verdict_evidence.len > t.max_items) return error.MemoryInputTooLarge;
        @memcpy(verdict_evidence[0..proposal.assertion.evidence.len], proposal.assertion.evidence);
        @memcpy(verdict_evidence[proposal.assertion.evidence.len..], request.evidence);
        proposal.verdict_evidence = verdict_evidence;
        proposal.reason = request.reason;
        proposal.model = request.model;
        try self.put(try key(self.a, "relationship-proposal", id), proposal);
        if (request.actor.len > 0) receipt.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
        receipt.proposal_id = id;
    }
    fn declaredDistinct(self: *Run, id: []const u8) !std.StringHashMapUnmanaged(void) {
        var result: std.StringHashMapUnmanaged(void) = .empty;
        for (try self.identityMembers(id)) |name| {
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", name))) orelse continue;
            for (record.entity.distinct_from) |other| {
                try result.put(self.a, (try self.forwardIdentity(other)).canonical, {});
            }
        }
        return result;
    }
    fn relationshipPredicates(self: *Run, subject_type: []const u8, object_type: []const u8) ![]const []const u8 {
        var names: std.StringHashMapUnmanaged(void) = .empty;
        for (self.state.config.predicates) |predicate| {
            const target = predicate.object_type orelse continue;
            if (!eql(u8, target, object_type)) continue;
            if (predicate.subject_type) |source_type| if (!eql(u8, source_type, subject_type)) continue;
            try names.put(self.a, predicate.name, {});
        }
        if (self.state.config.ontology_pack == .software_team_v1 and eql(u8, subject_type, "concept") and eql(u8, object_type, "concept")) {
            for ([_][]const u8{ "is_a", "part_of", "alternative_to" }) |name| try names.put(self.a, name, {});
        }
        const result = try self.a.alloc([]const u8, names.count());
        var iterator = names.keyIterator();
        var i: usize = 0;
        while (iterator.next()) |name| : (i += 1) result[i] = name.*;
        std.mem.sort([]const u8, result, {}, stringLess);
        return result;
    }
    const CandidateIndex = struct { cursor: ?[]const u8 = null, complete: bool = false };
    fn candidateTerms(self: *Run, entity_value: t.Entity) ![][]const u8 {
        var terms: std.StringHashMapUnmanaged(void) = .empty;
        const labels = try self.a.alloc([]const u8, entity_value.aliases.len + 1);
        labels[0] = entity_value.name;
        @memcpy(labels[1..], entity_value.aliases);
        for (labels) |label| {
            const compact = try compactName(self.a, label);
            if (compact.len > 0) try terms.put(self.a, try std.fmt.allocPrint(self.a, "compact/{s}", .{compact}), {});
            if (compact.len >= 4) for (0..compact.len - 2) |i| {
                const gram = compact[i..][0..3];
                // ASCII spelling features never split a UTF-8 code point.
                if (std.ascii.isAlphanumeric(gram[0]) and std.ascii.isAlphanumeric(gram[1]) and std.ascii.isAlphanumeric(gram[2])) try terms.put(self.a, try std.fmt.allocPrint(self.a, "gram/{s}", .{gram}), {});
            };
            const words = try lexical.tokens(self.a, label);
            var iterator = words.keyIterator();
            while (iterator.next()) |token| try terms.put(self.a, try std.fmt.allocPrint(self.a, "word/{s}", .{token.*}), {});
        }
        const result = try self.a.alloc([]const u8, terms.count());
        var iterator = terms.keyIterator();
        var i: usize = 0;
        while (iterator.next()) |term| : (i += 1) result[i] = term.*;
        std.mem.sort([]const u8, result, {}, stringLess);
        return result;
    }
    fn candidateNameScore(left: []const []const u8, right: []const []const u8) f64 {
        var left_words: usize = 0;
        var right_words: usize = 0;
        var left_grams: usize = 0;
        var right_grams: usize = 0;
        for (left) |term| {
            if (std.mem.startsWith(u8, term, "word/")) left_words += 1;
            if (std.mem.startsWith(u8, term, "gram/")) left_grams += 1;
        }
        for (right) |term| {
            if (std.mem.startsWith(u8, term, "word/")) right_words += 1;
            if (std.mem.startsWith(u8, term, "gram/")) right_grams += 1;
        }
        var l: usize = 0;
        var r: usize = 0;
        var matches: usize = 0;
        var gram_matches: usize = 0;
        while (l < left.len and r < right.len) {
            switch (std.mem.order(u8, left[l], right[r])) {
                .lt => l += 1,
                .gt => r += 1,
                .eq => {
                    if (std.mem.startsWith(u8, left[l], "compact/")) return 2;
                    if (std.mem.startsWith(u8, left[l], "word/")) matches += 1;
                    if (std.mem.startsWith(u8, left[l], "gram/")) gram_matches += 1;
                    l += 1;
                    r += 1;
                },
            }
        }
        const denominator = @min(left_words, right_words);
        const word_score: f64 = if (denominator == 0) 0 else @as(f64, @floatFromInt(matches)) / @as(f64, @floatFromInt(denominator));
        const gram_denominator = @min(left_grams, right_grams);
        const gram_score: f64 = if (gram_denominator < 2) 0 else @as(f64, @floatFromInt(gram_matches)) / @as(f64, @floatFromInt(gram_denominator));
        // A shared three-byte fragment alone is not a useful spelling neighbor.
        return @max(word_score, if (gram_score >= 0.5) gram_score else 0);
    }
    fn candidateScope(self: *Run, id: []const u8, request: t.EntityCandidateRequest) ![]const u8 {
        const index = (try self.read(CandidateIndex, "entity-candidate-index")) orelse CandidateIndex{};
        return std.fmt.allocPrint(self.a, "entity-candidates/{s}/", .{try hash(self.a, .{ id, request.kind, index.complete })});
    }
    fn candidatePrefix(self: *Run, record: t.EntityRecord, term: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "entity-candidate-term/{s}/{s}/", .{ try hash(self.a, .{ record.entity.visibility, if (record.entity.visibility == .team) "" else record.owner }), try hash(self.a, term) });
    }
    fn candidateAudiencePrefix(self: *Run, record: t.EntityRecord) ![]const u8 {
        return std.fmt.allocPrint(self.a, "entity-candidate-audience/{s}/", .{try hash(self.a, .{ record.entity.visibility, if (record.entity.visibility == .team) "" else record.owner })});
    }
    fn indexCandidate(self: *Run, record: t.EntityRecord, remove_index: bool) !void {
        const audience_path = try std.fmt.allocPrint(self.a, "{s}{s}", .{ try self.candidateAudiencePrefix(record), try hash(self.a, record.entity.id) });
        if (remove_index) try self.remove(audience_path) else try self.put(audience_path, .{ .id = record.entity.id });
        const terms = try self.candidateTerms(record.entity);
        // Very wide alias vocabularies use one bounded overflow posting rather
        // than amplifying a single entity into tens of thousands of writes.
        for (if (terms.len > 128) &[_][]const u8{"overflow"} else terms) |term| {
            const path = try std.fmt.allocPrint(self.a, "{s}{s}", .{ try self.candidatePrefix(record, term), try hash(self.a, record.entity.id) });
            if (remove_index) try self.remove(path) else try self.put(path, .{ .id = record.entity.id });
        }
    }
    fn candidateBindingRows(self: *Run, bindings: []const Row) ![]const Row {
        var paths: std.ArrayList([]const u8) = .empty;
        for (bindings) |row| {
            const binding = try parse(struct { id: []const u8 }, self.a, row.json);
            try paths.append(self.a, try key(self.a, "entity", binding.id));
        }
        try self.prefetch(paths.items);
        return self.cachedRows(paths.items);
    }
    fn cachedRows(self: *Run, paths: []const []const u8) ![]const Row {
        var rows: std.ArrayList(Row) = .empty;
        for (paths) |path| {
            const json = if (self.change_positions.get(path)) |position| self.changes.items[position].json else self.backend_rows.get(path) orelse null;
            if (json) |value| try rows.append(self.a, .{ .key = path, .json = value });
        }
        return rows.items;
    }
    // Each page examines at most 64 postings, independently of space size.
    // Term position and physical posting position both survive continuation.
    fn candidatePage(self: *Run, seed: t.EntityRecord, request: t.EntityCandidateRequest) !Page {
        const index = (try self.read(CandidateIndex, "entity-candidate-index")) orelse CandidateIndex{};
        const scope = try self.candidateScope(seed.entity.id, request);
        const position = try self.decodeCursor(scope, request.cursor);
        const name_terms = try self.candidateTerms(seed.entity);
        if (!index.complete) {
            if (!self.service.context.admin) return error.MemoryCapabilityUnavailable;
            // Existing spaces retain the bounded legacy path until the owner
            // finishes a resumable primary-row backfill under the same CAS.
            const after = if (position) |value| value[scope.len..] else null;
            if (after) |value| if (!std.mem.startsWith(u8, value, "entity/")) return error.InvalidMemoryCursor;
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "entity/", after, 64);
            return .{ .rows = page.rows, .next = if (page.next) |next| try std.fmt.allocPrint(self.a, "{s}{s}", .{ scope, next }) else null };
        }
        if (name_terms.len > 128) {
            const prefix = try self.candidateAudiencePrefix(seed);
            const after = if (position) |value| value[scope.len..] else null;
            if (after) |value| if (!std.mem.startsWith(u8, value, prefix)) return error.InvalidMemoryCursor;
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, after, 64);
            return .{ .rows = try self.candidateBindingRows(page.rows), .next = if (page.next) |next| try std.fmt.allocPrint(self.a, "{s}{s}", .{ scope, next }) else null };
        }
        const terms = try self.a.alloc([]const u8, name_terms.len + 1);
        @memcpy(terms[0..name_terms.len], name_terms);
        terms[name_terms.len] = "overflow";
        var term: usize = 0;
        var after: ?[]const u8 = null;
        if (position) |value| {
            const tail = value[scope.len..];
            const split = std.mem.indexOfScalar(u8, tail, '/') orelse return error.InvalidMemoryCursor;
            term = std.fmt.parseInt(usize, tail[0..split], 10) catch return error.InvalidMemoryCursor;
            if (term >= terms.len) return error.InvalidMemoryCursor;
            if (tail[split + 1 ..].len > 0) after = tail[split + 1 ..];
        }
        var rows: std.ArrayList(Row) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var examined: u32 = 0;
        while (term < terms.len and examined < 64) {
            const prefix = try self.candidatePrefix(seed, terms[term]);
            if (after) |value| if (!std.mem.startsWith(u8, value, prefix)) return error.InvalidMemoryCursor;
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, after, 64 - examined);
            examined += @intCast(page.rows.len);
            for (try self.candidateBindingRows(page.rows)) |row| {
                if (seen.contains(row.key)) continue;
                try seen.put(self.a, row.key, {});
                try rows.append(self.a, row);
            }
            after = page.next;
            if (after == null) term += 1;
        }
        return .{ .rows = rows.items, .next = if (term < terms.len) try std.fmt.allocPrint(self.a, "{s}{d}/{s}", .{ scope, term, after orelse "" }) else null };
    }
    const SemanticMatch = struct { id: []const u8, names: ?f64 = null, view: ?f64 = null };
    const SemanticMatches = struct { items: []const SemanticMatch = &.{}, queries: [2]?[]const f32 = .{ null, null }, self_scores: [2]f64 = .{ 0, 0 }, state: @FieldType(t.EntityCandidatePage, "semantic_discovery") = .not_indexed };
    fn semanticPath(self: *Run, path: []const u8) ![]const u8 {
        if (self.service.context.branch.len == 0) return path;
        return std.fmt.allocPrint(self.a, "entity-semantic-branch/{s}/{s}", .{ try hash(self.a, self.service.context.branch), path });
    }
    fn semanticKey(self: *Run, prefix: []const u8, id: []const u8) ![]const u8 {
        return self.semanticPath(try key(self.a, prefix, id));
    }
    fn semanticChildKey(self: *Run, kind: []const u8, parent: []const u8, id: []const u8) ![]const u8 {
        return dependencyKey(self.a, kind, parent, if (self.service.context.branch.len == 0) id else try hash(self.a, .{ self.service.context.branch, id }));
    }
    fn queueSemanticBranch(self: *Run, branch: []const u8) !void {
        if (!self.state.config.features.lineage or !self.state.config.semantic_discovery.enabled) return;
        const binding = try key(self.a, "branch-semantic-binding", if (branch.len == 0) "main" else branch);
        if (try self.read([]const u8, binding) != null) return;
        const path = try std.fmt.allocPrint(self.a, "branch-semantic-work/{d:0>20}/{s}", .{ self.state.revision, try hash(self.a, branch) });
        try self.put(path, .{ .branch = branch });
        try self.put(binding, path);
    }
    fn invalidateSemanticBranches(self: *Run) !void {
        if (self.state.config.features.lineage and self.state.config.semantic_discovery.enabled) try self.put("branch-semantic-invalidation", .{ .cursor = @as(?[]const u8, null) });
    }
    fn semanticBranchFanout(self: *Run) !void {
        const work = (try self.read(struct { cursor: ?[]const u8 }, "branch-semantic-invalidation")) orelse return;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "lineage-head/", work.cursor, 8);
        const old_branch = self.service.context.branch;
        defer self.service.context.branch = old_branch;
        for (page.rows) |row| {
            const head = try parse(t.LineageHead, self.a, row.json);
            if (!head.complete) continue;
            self.service.context.branch = head.id;
            try self.put(try self.semanticPath("entity-semantic-index"), t.SemanticIndex{});
            try self.queueSemanticBranch(head.id);
        }
        if (page.next) |next| try self.put("branch-semantic-invalidation", .{ .cursor = next }) else try self.remove("branch-semantic-invalidation");
    }
    fn semanticEntityPage(self: *Run, initial: bool, after: ?[]const u8) !Page {
        if (initial) if (self.lineage_head) |head| if (head.complete) {
            const page = try self.persistentIndex().page(head.root, "entity/", "entity/~", after, 8);
            var paths: std.ArrayList([]const u8) = .empty;
            for (page.items) |item| try paths.append(self.a, item.value);
            try self.prefetch(paths.items);
            var rows: std.ArrayList(Row) = .empty;
            for (page.items) |item| if (try self.rawRead(t.EntityRecord, item.value)) |record| try rows.append(self.a, .{ .key = item.key, .json = try self.encode(record) });
            return .{ .rows = rows.items, .next = page.next };
        };
        return self.service.backend.scan(self.service.backend.ptr, self.a, if (initial) "entity/" else try self.semanticPath("entity-semantic-work/"), after, 8);
    }
    fn semanticMaintenance(self: *Run) !void {
        if (!self.state.config.semantic_discovery.enabled) return;
        try self.requireAdmin();
        try self.semanticBranchFanout();
        const jobs = if (self.state.config.features.lineage) try self.service.backend.scan(self.service.backend.ptr, self.a, "branch-semantic-work/", null, 1) else Page{ .rows = &.{}, .next = null };
        const saved_branch = self.service.context.branch;
        const saved_head = self.lineage_head;
        const saved_raw = self.lineage_raw;
        defer {
            self.service.context.branch = saved_branch;
            self.lineage_head = saved_head;
            self.lineage_raw = saved_raw;
            self.views.clearRetainingCapacity();
            self.historical.clearRetainingCapacity();
        }
        if (jobs.rows.len > 0) {
            const branch = (try parse(struct { branch: []const u8 }, self.a, jobs.rows[0].json)).branch;
            const head = (try self.rawRead(t.LineageHead, try key(self.a, "lineage-head", if (branch.len == 0) "main" else branch))) orelse return error.MemoryCorruptState;
            try self.remove(jobs.rows[0].key);
            try self.remove(try key(self.a, "branch-semantic-binding", if (branch.len == 0) "main" else branch));
            if (!head.complete) return;
            self.service.context.branch = branch;
            self.lineage_head = head;
        }
        self.lineage_raw = false;
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
        try self.semanticMaintenanceScope();
        if (self.semantic_ready_after_commit) self.semantic_publication_branch = self.service.context.branch;
        if (self.state.config.features.lineage) {
            const index = (try self.read(t.SemanticIndex, try self.semanticPath("entity-semantic-index"))) orelse return error.MemoryCorruptState;
            if (!index.backfill_complete or (try self.service.backend.scan(self.service.backend.ptr, self.a, try self.semanticPath("entity-semantic-work/"), null, 1)).rows.len != 0) try self.queueSemanticBranch(self.service.context.branch);
        }
    }
    fn semanticDirty(self: *Run, id: []const u8) anyerror!void {
        try self.queueSemanticBranch(self.service.context.branch);
        try self.put(try self.semanticKey("entity-semantic-work", id), .{ .id = id });
        if (try self.canonical(id)) |selected| if (!eql(u8, selected, id)) {
            try self.put(try self.semanticKey("entity-semantic-work", selected), .{ .id = selected });
        };
    }
    fn semanticQueryKey(self: *Run, record: t.EntityRecord, kind: t.ProjectionKind) ![]const u8 {
        return std.fmt.allocPrint(self.a, "entity-semantic-query/{s}/{s}", .{ try self.semanticIndexName(record, kind), try hash(self.a, record.entity.id) });
    }
    fn semanticProjectionKey(self: *Run, record: t.EntityRecord, kind: t.ProjectionKind) ![]const u8 {
        return std.fmt.allocPrint(self.a, "entity-semantic/{s}/{s}", .{ try self.semanticIndexName(record, kind), try hash(self.a, record.entity.id) });
    }
    fn semanticIndexName(self: *Run, record: t.EntityRecord, kind: t.ProjectionKind) ![]const u8 {
        // Separate native index namespaces enforce the audience before ANN,
        // without a corpus-sized postfilter or an ordinal membership scan.
        const audience = .{ self.state.config.embedding_generation, self.state.config.models.embedder, record.entity.visibility, if (record.entity.visibility == .private) record.owner else "" };
        return std.fmt.allocPrint(self.a, "memory_{s}_{s}", .{ @tagName(kind), if (self.service.context.branch.len == 0) try hash(self.a, audience) else try hash(self.a, .{ audience, self.service.context.branch }) });
    }
    const SemanticSnapshot = struct { view: ?t.EntityView = null, canonical: bool = true };
    fn semanticSnapshot(self: *Run, record: t.EntityRecord) !SemanticSnapshot {
        const prior = self.service.context;
        self.service.context.admin = false;
        self.service.context.principal = record.owner;
        defer self.service.context = prior;
        // Eligibility and rendering share the entity's audience. Immutable
        // private support owned by another principal is not a current input
        // for this audience, even if the maintenance administrator can read it.
        if (!try self.entityVisible(record)) return .{};
        const selected = try self.canonical(record.entity.id);
        if (selected == null or !eql(u8, selected.?, record.entity.id)) return .{ .canonical = false };
        return .{ .view = try self.entityView(record.entity.id, .{}) };
    }
    fn semanticView(self: *Run, record: t.EntityRecord) !t.EntityView {
        // A projection can never acquire the maintenance principal's wider
        // administrative visibility. Its read audience belongs to the entity.
        const prior = self.service.context;
        self.service.context.admin = false;
        self.service.context.principal = record.owner;
        defer self.service.context = prior;
        return self.entityView(record.entity.id, .{});
    }
    fn semanticNames(self: *Run, value: t.Entity) ![]const u8 {
        var text: std.ArrayList(u8) = .empty;
        try text.appendSlice(self.a, value.name);
        for (value.aliases) |alias| {
            try text.append(self.a, '\n');
            try text.appendSlice(self.a, alias);
        }
        return text.items;
    }
    fn inheritedSemanticProjection(self: *Run, record: t.EntityRecord, kind: t.ProjectionKind, query: bool) !?t.EntityProjection {
        const head = self.lineage_head orelse return null;
        if (self.service.context.branch.len == 0) return null;
        const parent = head.parent orelse return null;
        const branch = self.service.context.branch;
        self.service.context.branch = parent;
        defer self.service.context.branch = branch;
        return self.read(t.EntityProjection, if (query) try self.semanticQueryKey(record, kind) else try self.semanticProjectionKey(record, kind));
    }
    fn reusableSemanticVector(self: *Run, prior: t.EntityProjection, record: t.EntityRecord, kind: t.ProjectionKind, text: []const u8) bool {
        const model = self.state.config.models.embedder.?;
        return prior.kind == kind and eql(u8, prior.entity_id, record.entity.id) and eql(u8, prior.owner, record.owner) and prior.visibility == record.entity.visibility and prior.generation == self.state.config.embedding_generation and prior.embedding.len == model.dimensions.? and valueEqual(t.Model, prior.model, model) and eql(u8, prior.text, text);
    }
    fn semanticCurrent(self: *Run, projection: t.EntityProjection, record: t.EntityRecord, view: t.EntityView) !bool {
        if (!eql(u8, projection.branch, self.service.context.branch)) return false;
        if (projection.embedding.len != self.state.config.models.embedder.?.dimensions.? or projection.entity_revision != record.revision or projection.generation != self.state.config.embedding_generation or projection.config_generation != self.state.config.generation or !eql(u8, projection.entity_id, record.entity.id) or !eql(u8, projection.owner, record.owner) or projection.visibility != record.entity.visibility or !valueEqual(t.Model, projection.model, self.state.config.models.embedder.?)) return false;
        if (!eql(u8, projection.fingerprint, view.fingerprint) or !valueEqual([]const t.DerivedDependency, projection.dependencies, view.dependencies)) return false;
        if (projection.kind == .view) return eql(u8, projection.text, view.description);
        var remaining = projection.text;
        if (!std.mem.startsWith(u8, remaining, record.entity.name)) return false;
        remaining = remaining[record.entity.name.len..];
        for (record.entity.aliases) |alias| {
            if (remaining.len == 0 or remaining[0] != '\n') return false;
            remaining = remaining[1..];
            if (!std.mem.startsWith(u8, remaining, alias)) return false;
            remaining = remaining[alias.len..];
        }
        return remaining.len == 0;
    }
    // A per-entity manifest makes serving retirement and source fanout point
    // operations. Resource identities include every evaluated support branch,
    // including currently redacted branches and transitive parents.
    const SemanticBinding = struct { id: []const u8, projections: []const []const u8 = &.{}, resources: []const []const u8 = &.{}, assertions: []const []const u8 = &.{} };
    fn semanticResourceId(self: *Run, reference: prov.Resource) ![]const u8 {
        return hash(self.a, reference.identity());
    }
    fn semanticInvalidateAssertion(self: *Run, id: []const u8) anyerror!void {
        if (!self.state.config.semantic_discovery.enabled) return;
        const input = try hash(self.a, id);
        const prefix = try std.fmt.allocPrint(self.a, "dependency/semantic-assertion/{s}/", .{try hash(self.a, input)});
        // A fresh import with no derived consumers creates no empty work queue.
        // Existing consumers include the full evaluated transitive read closure.
        if ((try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, null, 1)).rows.len != 0) try self.invalidate("semantic-assertion", input);
    }
    fn semanticBindings(self: *Run, id: []const u8, resources: ResourceSet, projections: []const []const u8, assertion_inputs: ResourceSet) !void {
        const path = try self.semanticKey("entity-semantic-binding", id);
        const prior = (try self.read(SemanticBinding, path)) orelse SemanticBinding{ .id = id };
        var current: std.StringHashMapUnmanaged(void) = .empty;
        var inputs = resources.keyIterator();
        while (inputs.next()) |input| try current.put(self.a, try self.semanticResourceId(self.resource_states.get(input.*).?.reference), {});
        for (prior.projections) |projection| {
            var keep = false;
            for (projections) |desired| if (eql(u8, projection, desired)) {
                keep = true;
                break;
            };
            if (!keep) try self.remove(projection);
        }
        for (prior.resources) |resource| if (!current.contains(resource)) try self.remove(try self.semanticChildKey("semantic-resource", resource, id));
        const ids = try self.a.alloc([]const u8, current.count());
        var iterator = current.keyIterator();
        var i: usize = 0;
        while (iterator.next()) |resource| : (i += 1) {
            ids[i] = resource.*;
            try self.put(try self.semanticChildKey("semantic-resource", resource.*, id), .{ .child = id, .branch = self.service.context.branch });
        }
        std.mem.sort([]const u8, ids, {}, stringLess);
        for (prior.assertions) |assertion_id| if (!assertion_inputs.contains(assertion_id)) try self.remove(try self.semanticChildKey("semantic-assertion", assertion_id, id));
        const assertion_ids = try self.a.alloc([]const u8, assertion_inputs.count());
        var assertion_iterator = assertion_inputs.keyIterator();
        i = 0;
        while (assertion_iterator.next()) |assertion_id| : (i += 1) {
            assertion_ids[i] = assertion_id.*;
            try self.put(try self.semanticChildKey("semantic-assertion", assertion_id.*, id), .{ .child = id, .branch = self.service.context.branch });
        }
        std.mem.sort([]const u8, assertion_ids, {}, stringLess);
        if (ids.len == 0 and projections.len == 0 and assertion_ids.len == 0) try self.remove(path) else try self.put(path, SemanticBinding{ .id = id, .resources = ids, .projections = projections, .assertions = assertion_ids });
    }
    fn retireSemantic(self: *Run, record: t.EntityRecord) !void {
        // Also cover projections predating the manifest's bounded backfill.
        inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }) |kind| {
            const path = try self.semanticProjectionKey(record, kind);
            if (try self.read(t.EntityProjection, path) != null) try self.remove(path);
            const query_path = try self.semanticQueryKey(record, kind);
            if (try self.read(t.EntityProjection, query_path) != null) try self.remove(query_path);
        }
    }
    const SemanticTimer = struct { branch: []const u8 = "", id: []const u8, at: i64 };
    fn semanticTimerKey(self: *Run, timer: SemanticTimer) ![]const u8 {
        const ordered = @as(u64, @bitCast(timer.at)) ^ (@as(u64, 1) << 63);
        return std.fmt.allocPrint(self.a, "entity-semantic-due/{d:0>20}/{s}", .{ ordered, if (timer.branch.len == 0) try hash(self.a, timer.id) else try hash(self.a, .{ timer.branch, timer.id }) });
    }
    fn scheduleSemanticTimer(self: *Run, id: []const u8, at: ?i64) !void {
        const path = try self.semanticKey("entity-semantic-timer", id);
        if (try self.read(SemanticTimer, path)) |prior| try self.remove(try self.semanticTimerKey(prior));
        if (at) |time| {
            const timer: SemanticTimer = .{ .branch = self.service.context.branch, .id = id, .at = time };
            try self.put(path, timer);
            try self.put(try self.semanticTimerKey(timer), timer);
        } else try self.remove(path);
    }
    fn semanticTimers(self: *Run) !void {
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "entity-semantic-due/", null, 8);
        for (page.rows) |row| {
            const timer = try parse(SemanticTimer, self.a, row.json);
            if (timer.at > self.service.context.now_ms) break;
            const old_branch = self.service.context.branch;
            self.service.context.branch = timer.branch;
            defer self.service.context.branch = old_branch;
            try self.remove(row.key);
            const path = try self.semanticKey("entity-semantic-timer", timer.id);
            if (try self.read(SemanticTimer, path)) |current| if (current.at == timer.at) {
                try self.remove(path);
                if (self.state.config.semantic_discovery.enabled) try self.semanticDirty(timer.id);
            };
        }
    }
    const SemanticFailure = struct { id: []const u8, generation: u64, attempts: u16, error_code: []const u8, next_retry_at: i64 };
    fn deferSemanticFailure(self: *Run, record: t.EntityRecord, failure: anyerror, inputs: ResourceSet) !void {
        switch (failure) {
            error.OutOfMemory, error.Canceled, error.Forbidden, error.Unauthorized, error.MemorySourceAccessDenied, error.MemoryResourceChanged, error.MemoryRevisionConflict, error.MemorySpaceBindingChanged, error.NotLeader, error.MemoryCorruptState => return failure,
            else => {},
        }
        if (isProcessingTimeout(failure)) try self.service.context.beginCheckpoint();
        // A failed invocation can release an embedded lease too. Restore the
        // complete source/control/authority fence before publishing retry debt.
        try self.inferenceCompleted(inputs);
        const path = try self.semanticKey("entity-semantic-failure", record.entity.id);
        const previous = try self.read(SemanticFailure, path);
        const attempts = if (previous != null and previous.?.generation == self.state.config.generation) previous.?.attempts +| 1 else @as(u16, 1);
        const delay: i64 = @min(300000, @as(i64, 1000) << @intCast(@min(attempts, 9)));
        const retry_at = self.service.context.currentTime() +| delay;
        try self.put(path, SemanticFailure{ .id = record.entity.id, .generation = self.state.config.generation, .attempts = attempts, .error_code = @errorName(failure), .next_retry_at = retry_at });
        try self.put(try self.semanticKey("entity-semantic-blocked", record.entity.id), .{ .id = record.entity.id });
        const timer = try self.read(SemanticTimer, try self.semanticKey("entity-semantic-timer", record.entity.id));
        try self.scheduleSemanticTimer(record.entity.id, if (timer) |current| @min(current.at, retry_at) else retry_at);
        try self.remove(try self.semanticKey("entity-semantic-work", record.entity.id));
        return error.MemorySemanticDeferred;
    }
    fn semanticEmbedding(self: *Run, record: t.EntityRecord, model: t.Model, text: []const u8, inputs: ResourceSet, progressed: bool, comptime query: bool) ![]const f32 {
        const compute = if (query) self.service.backend.embed.? else self.service.backend.embed_document.?;
        const computed = compute(self.service.backend.ptr, self.a, model, text) catch |err| {
            // Yield a cumulative budget after useful work. If the first
            // unfinished entity fails again on its own quantum, defer it and
            // advance the scan so it cannot pin unrelated healthy entities.
            if (isProcessingTimeout(err) and progressed) return err;
            try self.deferSemanticFailure(record, err, inputs);
            unreachable;
        };
        try self.inferenceCompleted(inputs);
        if (computed.len != model.dimensions.?) {
            try self.deferSemanticFailure(record, error.MemoryEmbeddingMismatch, inputs);
            unreachable;
        }
        for (computed) |value| if (!std.math.isFinite(value)) {
            try self.deferSemanticFailure(record, error.MemoryEmbeddingMismatch, inputs);
            unreachable;
        };
        return computed;
    }
    fn semanticBatch(self: *Run, record: t.EntityRecord, model: t.Model, texts: []const []const u8, role: EmbeddingRole, inputs: ResourceSet, progressed: bool) ![]const []const f32 {
        const vectors = self.service.backend.embed_batch.?(self.service.backend.ptr, self.a, model, texts, role) catch |err| {
            if (isProcessingTimeout(err) and progressed) return err;
            try self.deferSemanticFailure(record, err, inputs);
            unreachable;
        };
        try self.inferenceCompleted(inputs);
        var valid = vectors.len == texts.len;
        for (vectors) |vector| {
            if (vector.len != model.dimensions.?) valid = false;
            for (vector) |value| if (!std.math.isFinite(value)) {
                valid = false;
            };
        }
        if (!valid) {
            try self.deferSemanticFailure(record, error.MemoryEmbeddingMismatch, inputs);
            unreachable;
        }
        return vectors;
    }
    // An entity is one authorization/dependency partition. Batch its names and
    // rendered view by role, then checkpoint documents before building queries.
    // Both provider roles stay outside writer leases and share publication fences.
    fn semanticEntityBatched(self: *Run, record: t.EntityRecord, view: t.EntityView, inputs: ResourceSet, progressed: *bool) !void {
        const model = self.state.config.models.embedder.?;
        var documents: [2]t.EntityProjection = undefined;
        var old_documents: [2]?t.EntityProjection = undefined;
        var old_queries: [2]?t.EntityProjection = undefined;
        var queries: [2]?[]const f32 = .{ null, null };
        var paths: [2][]const u8 = undefined;
        var query_paths: [2][]const u8 = undefined;
        var missing_texts: std.ArrayList([]const u8) = .empty;
        var missing_slots: std.ArrayList(usize) = .empty;
        inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }, 0..) |kind, slot| {
            paths[slot] = try self.semanticProjectionKey(record, kind);
            query_paths[slot] = try self.semanticQueryKey(record, kind);
            const old = try self.read(t.EntityProjection, paths[slot]);
            const old_query = try self.read(t.EntityProjection, query_paths[slot]);
            old_documents[slot] = old;
            old_queries[slot] = old_query;
            const text = if (kind == .names) try self.semanticNames(record.entity) else view.description;
            const cached_document = old orelse try self.inheritedSemanticProjection(record, kind, false);
            const cached_query = old_query orelse try self.inheritedSemanticProjection(record, kind, true);
            const reusable = if (cached_document) |prior| self.reusableSemanticVector(prior, record, kind, text) else false;
            documents[slot] = .{ .branch = self.service.context.branch, .entity_id = record.entity.id, .entity_revision = record.revision, .model = model, .generation = self.state.config.embedding_generation, .config_generation = self.state.config.generation, .owner = record.owner, .visibility = record.entity.visibility, .kind = kind, .fingerprint = view.fingerprint, .dependencies = view.dependencies, .text = text, .embedding = if (reusable) cached_document.?.embedding else &.{} };
            if (!reusable) {
                try missing_slots.append(self.a, slot);
                try missing_texts.append(self.a, text);
            }
            if (reusable and cached_document.?.query_embedding != null and cached_document.?.query_embedding.?.len == model.dimensions.?) queries[slot] = cached_document.?.query_embedding else if (cached_query) |prior| {
                if (prior.generation == self.state.config.embedding_generation and prior.embedding.len == model.dimensions.? and valueEqual(t.Model, prior.model, model) and eql(u8, prior.text, text)) queries[slot] = prior.embedding;
            }
        }
        if (missing_texts.items.len != 0) {
            const vectors = try self.semanticBatch(record, model, missing_texts.items, .document, inputs, progressed.*);
            for (missing_slots.items, vectors) |slot, vector| documents[slot].embedding = vector;
        }
        inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }, 0..) |kind, slot| {
            if (old_documents[slot] == null or old_documents[slot].?.query_embedding != null or !try self.semanticCurrent(old_documents[slot].?, record, view)) {
                try self.ensureMemoryIndex(try self.semanticIndexName(record, kind), .{ .kind = .semantic, .generation = self.state.config.embedding_generation }, model.dimensions.?);
                try self.put(paths[slot], documents[slot]);
                progressed.* = true;
            }
        }
        missing_texts = .empty;
        missing_slots = .empty;
        for (queries, 0..) |query, slot| if (query == null) {
            try missing_slots.append(self.a, slot);
            try missing_texts.append(self.a, documents[slot].text);
        };
        if (missing_texts.items.len != 0) {
            const vectors = try self.semanticBatch(record, model, missing_texts.items, .query, inputs, progressed.*);
            for (missing_slots.items, vectors) |slot, vector| queries[slot] = vector;
        }
        for (queries, 0..) |query, slot| {
            if (old_queries[slot] == null or !try self.semanticCurrent(old_queries[slot].?, record, view)) {
                var projection = documents[slot];
                projection.embedding = query.?;
                try self.put(query_paths[slot], projection);
                progressed.* = true;
            }
        }
    }
    fn semanticMaintenanceScope(self: *Run) !void {
        if (!self.state.config.semantic_discovery.enabled) return;
        try self.requireAdmin();
        var index = (try self.read(t.SemanticIndex, try self.semanticPath("entity-semantic-index"))) orelse t.SemanticIndex{};
        if (index.version != t.semantic_index_version or index.generation != self.state.config.generation) index = .{ .version = t.semantic_index_version, .generation = self.state.config.generation };
        const initial = !index.backfill_complete;
        // A qualified generation stays serving during incremental refresh.
        // Every returned projection is independently dependency-fenced below.
        if (initial) index.complete = false;
        const page = try self.semanticEntityPage(initial, if (initial) index.cursor else index.work_cursor);
        var position = if (initial) index.cursor else index.work_cursor;
        var progressed = false;
        var yielded = false;
        for (page.rows) |row| {
            self.semanticEntity(row, initial, &progressed) catch |err| {
                if (err == error.MemorySemanticDeferred) {
                    position = row.key;
                    yielded = true;
                    break;
                }
                if ((!isProcessingTimeout(err) and err != error.MemoryWorkLimit) or !progressed) return err;
                try self.service.context.beginCheckpoint();
                yielded = true;
                break;
            };
            position = row.key;
            progressed = true;
        }
        if (initial) {
            index.cursor = if (yielded) position else page.next;
            index.backfill_complete = !yielded and page.next == null;
        } else index.work_cursor = if (yielded) position else page.next;
        self.semantic_ready_after_commit = index.backfill_complete and !yielded;
        try self.put(try self.semanticPath("entity-semantic-index"), index);
    }
    fn semanticEntity(self: *Run, row: Row, initial: bool, progressed: *bool) !void {
        const record = if (initial) try parse(t.EntityRecord, self.a, row.json) else blk: {
            const work = try parse(struct { id: []const u8 }, self.a, row.json);
            break :blk (try self.read(t.EntityRecord, try key(self.a, "entity", work.id))) orelse {
                try self.semanticBindings(work.id, .empty, &.{}, .empty);
                try self.scheduleSemanticTimer(work.id, null);
                try self.remove(try self.semanticKey("entity-semantic-blocked", work.id));
                try self.remove(try self.semanticKey("entity-semantic-failure", work.id));
                try self.remove(row.key);
                return;
            };
        };
        self.temporal_tracking = true;
        self.semantic_assertions.clearRetainingCapacity();
        self.temporal_boundary = null;
        self.temporal_denied = false;
        defer self.temporal_tracking = false;
        var resources: ResourceSet = .empty;
        const snapshot = self.captureResourcesInto(semanticSnapshot, .{record}, &resources) catch |err| {
            // Rendering shares the same fair scheduling contract as embedding.
            // Yield cumulative exhaustion after progress; an entity that also
            // exhausts a fresh quantum becomes durable, timed readiness debt.
            if (!isProcessingTimeout(err) and err != error.MemoryWorkLimit) return err;
            if (progressed.*) return err;
            try self.retireSemantic(record);
            try self.deferSemanticFailure(record, err, resources);
            unreachable;
        };
        const captured = .{ .result = snapshot, .resources = resources };
        self.temporal_tracking = false;
        // Eligibility, dependency bindings and serving retirement are also
        // derived publications, including a currently unavailable branch.
        self.inferred = true;
        if (captured.result.view) |view| {
            const paths = try self.a.alloc([]const u8, 4);
            inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }, 0..) |kind, i| {
                paths[2 * i] = try self.semanticProjectionKey(record, kind);
                paths[2 * i + 1] = try self.semanticQueryKey(record, kind);
            }
            try self.semanticBindings(record.entity.id, captured.resources, paths, self.semantic_assertions);
            // A partially materialized entity retains its dependency and
            // temporal fences, but cannot certify generation readiness.
            try self.scheduleSemanticTimer(record.entity.id, self.temporal_boundary);
            if (self.service.backend.embed_batch != null) {
                try self.semanticEntityBatched(record, view, captured.resources, progressed);
            } else {
                inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }) |kind| {
                    const path = try self.semanticProjectionKey(record, kind);
                    const existing = try self.read(t.EntityProjection, path);
                    const query_path = try self.semanticQueryKey(record, kind);
                    const query_prior = try self.read(t.EntityProjection, query_path);
                    if (existing == null or existing.?.query_embedding != null or !try self.semanticCurrent(existing.?, record, view) or query_prior == null or !try self.semanticCurrent(query_prior.?, record, view)) {
                        const text = if (kind == .names) try self.semanticNames(record.entity) else view.description;
                        const model = self.state.config.models.embedder.?;
                        const index_name = try self.semanticIndexName(record, kind);
                        try self.ensureMemoryIndex(index_name, .{ .kind = .semantic, .generation = self.state.config.embedding_generation }, model.dimensions.?);
                        const cached_document = existing orelse try self.inheritedSemanticProjection(record, kind, false);
                        const cached_query = query_prior orelse try self.inheritedSemanticProjection(record, kind, true);
                        const reusable = if (cached_document) |prior| self.reusableSemanticVector(prior, record, kind, text) else false;
                        const embedding = if (reusable) cached_document.?.embedding else blk: {
                            break :blk try self.semanticEmbedding(record, model, text, captured.resources, progressed.*, false);
                        };
                        if (embedding.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
                        for (embedding) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
                        const projection: t.EntityProjection = .{ .branch = self.service.context.branch, .entity_id = record.entity.id, .entity_revision = record.revision, .model = model, .generation = self.state.config.embedding_generation, .config_generation = self.state.config.generation, .owner = record.owner, .visibility = record.entity.visibility, .kind = kind, .fingerprint = view.fingerprint, .dependencies = view.dependencies, .text = text, .embedding = embedding };
                        // Checkpoint the document role before another provider call.
                        // Legacy backfills and retries never recompute that vector.
                        if (existing == null or existing.?.query_embedding != null or !try self.semanticCurrent(existing.?, record, view)) {
                            try self.put(path, projection);
                            progressed.* = true;
                        }
                        var query_projection = projection;
                        query_projection.embedding = if (reusable and cached_document.?.query_embedding != null and cached_document.?.query_embedding.?.len == model.dimensions.?)
                            cached_document.?.query_embedding.?
                        else if (cached_query != null and self.reusableSemanticVector(cached_query.?, record, kind, text))
                            cached_query.?.embedding
                        else
                            try self.semanticEmbedding(record, model, text, captured.resources, progressed.*, true);
                        if (query_projection.embedding.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
                        for (query_projection.embedding) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
                        try self.put(query_path, query_projection);
                        progressed.* = true;
                    }
                }
            }
        } else {
            try self.retireSemantic(record);
            const retained_entity = try self.entityRetained(record);
            const revisit = retained_entity and captured.result.canonical;
            try self.semanticBindings(record.entity.id, if (revisit) captured.resources else .empty, &.{}, if (revisit) self.semantic_assertions else .empty);
            if (revisit) {
                if (self.temporal_denied) {
                    // Preserve readiness debt separately from runnable work.
                    // The durable deadline retries denial at the poll cadence,
                    // allowing both healthy jobs and retention work to advance.
                    try self.put(try self.semanticKey("entity-semantic-blocked", record.entity.id), .{ .id = record.entity.id });
                    try self.scheduleSemanticTimer(record.entity.id, self.temporal_boundary orelse (self.service.context.now_ms +| resource_check_interval_ms));
                    try self.remove(try self.semanticKey("entity-semantic-work", record.entity.id));
                    return;
                }
                // A changed pinned source remains historical, rather than an
                // eligible current projection. Revisit it if the pin is restored.
                try self.scheduleSemanticTimer(record.entity.id, self.temporal_boundary);
            } else try self.scheduleSemanticTimer(record.entity.id, null);
        }
        try self.remove(try self.semanticKey("entity-semantic-work", record.entity.id));
        try self.remove(try self.semanticKey("entity-semantic-blocked", record.entity.id));
        try self.remove(try self.semanticKey("entity-semantic-failure", record.entity.id));
    }
    fn finishSemanticPublication(self: *Run) !void {
        if (!self.semantic_ready_after_commit) return;
        const old_branch = self.service.context.branch;
        self.service.context.branch = self.semantic_publication_branch orelse old_branch;
        defer self.service.context.branch = old_branch;
        // Phase one committed its durable cursor with complete=false and
        // waited for full index publication. Only its acknowledged success
        // authorizes this second CAS. A crash or ambiguous phase-one outcome
        // leaves a resumable, unready generation, never a false certificate.
        const backend = self.service.backend;
        const control = (try backend.get(backend.ptr, self.a, "control")) orelse return error.MemorySpaceNotFound;
        if (!eql(u8, control.json, try self.encode(self.state))) return;
        if ((try backend.scan(backend.ptr, self.a, try self.semanticPath("entity-semantic-work/"), null, 1)).rows.len != 0 or
            (try backend.scan(backend.ptr, self.a, try self.semanticPath("entity-semantic-blocked/"), null, 1)).rows.len != 0) return;
        const row = (try backend.get(backend.ptr, self.a, try self.semanticPath("entity-semantic-index"))) orelse return;
        var index = try parse(t.SemanticIndex, self.a, row.json);
        if (index.version != t.semantic_index_version or !index.backfill_complete or index.generation != self.state.config.generation) return;
        index.complete = true;
        try self.service.context.requireGrant(.admin);
        try self.service.context.finish();
        if (self.inferred) try self.validateResources();
        try backend.commit(backend.ptr, self.a, control, &.{ .{ .key = try self.semanticPath("entity-semantic-index"), .json = try self.encode(index) }, .{ .key = "control", .json = control.json } });
    }
    const candidate_pending_limit = 3 * 64;
    const candidate_semantic_limit = 2 * 64;
    // Lexical positions contain only fixed prefixes, hashes and a term number.
    const candidate_lexical_limit = 512;
    const candidate_json_limit = candidate_pending_limit * (2 * t.max_id_bytes + 32) + candidate_semantic_limit * (2 * t.max_id_bytes + 3) + 2 * candidate_lexical_limit + 128;
    const candidate_cursor_limit = std.base64.url_safe_no_pad.Encoder.calcSize(candidate_json_limit) + 256;
    const CandidateRef = struct { id: []const u8, semantic: bool = false };
    const CandidateContinuation = struct { pending: []const CandidateRef = &.{}, lexical: ?[]const u8 = null, semantic: []const []const u8 = &.{} };
    fn candidateView(self: *Run, rendered: *std.StringHashMapUnmanaged(t.EntityView), id: []const u8) !t.EntityView {
        if (rendered.get(id)) |view| return view;
        const view = try self.entityView(id, .{});
        try rendered.put(self.a, try self.a.dupe(u8, id), view);
        return view;
    }
    fn candidateCurrent(self: *Run, verified: *std.StringHashMapUnmanaged(bool), path: []const u8, projection: t.EntityProjection, record: t.EntityRecord, view: t.EntityView) !bool {
        if (verified.get(path)) |current| return current;
        const current = try self.semanticCurrent(projection, record, view);
        try verified.put(self.a, path, current);
        return current;
    }
    fn candidateMetadataEligible(self: *Run, seed: t.EntityRecord, candidate: t.EntityRecord, kind: @FieldType(t.EntityCandidateRequest, "kind")) bool {
        if (eql(u8, seed.entity.id, candidate.entity.id)) return false;
        if (candidate.entity.visibility != seed.entity.visibility or (candidate.entity.visibility == .private and !eql(u8, candidate.owner, seed.owner))) return false;
        if (kind == .identity) {
            if (!eql(u8, candidate.entity.type, seed.entity.type) or !sameNameNumbers(seed.entity.name, candidate.entity.name)) return false;
            for (seed.entity.distinct_from) |id| if (eql(u8, id, candidate.entity.id)) return false;
            for (candidate.entity.distinct_from) |id| if (eql(u8, id, seed.entity.id)) return false;
        }
        if (kind == .relationship) {
            if (self.state.config.ontology_pack == .software_team_v1 and eql(u8, seed.entity.type, "concept") and eql(u8, candidate.entity.type, "concept")) return true;
            for (self.state.config.predicates) |predicate| {
                if (predicate.object_type == null) continue;
                if (predicate.subject_type != null and !eql(u8, predicate.subject_type.?, seed.entity.type)) continue;
                if (eql(u8, predicate.object_type.?, candidate.entity.type)) return true;
            }
            return false;
        }
        return true;
    }
    fn semanticCandidates(self: *Run, input_seed: t.EntityRecord, request: t.EntityCandidateRequest, pending: []const CandidateRef, rendered: *std.StringHashMapUnmanaged(t.EntityView), verified: *std.StringHashMapUnmanaged(bool)) !SemanticMatches {
        if (!self.state.config.semantic_discovery.enabled) return .{};
        var result: SemanticMatches = .{ .state = .not_ready };
        const selected = (try self.canonical(input_seed.entity.id)) orelse return result;
        const seed = (try self.read(t.EntityRecord, try key(self.a, "entity", selected))) orelse return result;
        const index = (try self.read(t.SemanticIndex, try self.semanticPath("entity-semantic-index"))) orelse return result;
        if (index.version != t.semantic_index_version or !index.complete or !index.backfill_complete or index.generation != self.state.config.generation) return result;
        const view = try self.candidateView(rendered, seed.entity.id);
        var matches: std.StringHashMapUnmanaged(SemanticMatch) = .empty;
        inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }) |kind| {
            const projection = (try self.read(t.EntityProjection, try self.semanticProjectionKey(seed, kind))) orelse return result;
            if (!try self.candidateCurrent(verified, try self.semanticProjectionKey(seed, kind), projection, seed, view)) return result;
            // Both roles were built under the same source/model fences. Ready
            // pages never call a provider, including lexical continuations.
            const query_projection = (try self.read(t.EntityProjection, try self.semanticQueryKey(seed, kind))) orelse return result;
            if (query_projection.kind != kind or !try self.candidateCurrent(verified, try self.semanticQueryKey(seed, kind), query_projection, seed, view)) return result;
            const query = query_projection.embedding;
            if (query.len != projection.embedding.len) return error.MemoryCorruptState;
            const self_score = try cosine(query, projection.embedding);
            if (!std.math.isFinite(self_score) or self_score <= 0) return result;
            result.queries[@backingInt(kind)] = query;
            result.self_scores[@backingInt(kind)] = self_score;
            // ANN runs once per projection on the first lexical page. Later
            // pages continue lexical postings without repeating semantic hits.
            if (request.cursor == null or pending.len != 0) {
                const neighbors = if (request.cursor != null) blk: {
                    var carried: std.ArrayList(Neighbor) = .empty;
                    for (pending) |ref| {
                        if (!ref.semantic) continue;
                        const record = (try self.read(t.EntityRecord, try key(self.a, "entity", ref.id))) orelse continue;
                        const projection_row = (try self.read(t.EntityProjection, try self.semanticProjectionKey(record, kind))) orelse continue;
                        try carried.append(self.a, .{ .key = try self.semanticProjectionKey(record, kind), .similarity = try cosine(query, projection_row.embedding) });
                    }
                    break :blk carried.items;
                } else self.service.backend.neighbors.?(self.service.backend.ptr, self.a, .{ .index = try self.semanticIndexName(seed, kind), .vector = query, .limit = self.state.config.semantic_discovery.neighbors }) catch |err| switch (err) {
                    error.IndexNotFound, error.IndexRebuilding, error.Timeout, error.EnrichmentWaitTimeout => return result,
                    else => return err,
                };
                if (request.cursor == null and neighbors.len > self.state.config.semantic_discovery.neighbors) return error.MemoryCorruptState;
                var paths: std.ArrayList([]const u8) = .empty;
                for (neighbors) |neighbor| {
                    const normalized = neighbor.similarity / self_score;
                    const threshold = if (kind == .names) self.state.config.semantic_discovery.name_threshold else self.state.config.semantic_discovery.view_threshold;
                    if (std.math.isFinite(normalized) and normalized >= threshold) try paths.append(self.a, neighbor.key);
                }
                try self.prefetch(paths.items);
                const projection_paths = paths.items;
                paths = .empty;
                for (projection_paths) |path| if (try self.read(t.EntityProjection, path)) |other| {
                    if (other.kind == kind) try paths.append(self.a, try key(self.a, "entity", other.entity_id));
                };
                try self.prefetch(paths.items);
                var supports: std.ArrayList([]const u8) = .empty;
                for (paths.items) |path| if (try self.read(t.EntityRecord, path)) |record| {
                    if (!self.candidateMetadataEligible(input_seed, record, request.kind)) continue;
                    for (record.entity.evidence) |ref| try supports.append(self.a, try key(self.a, "evidence", ref.id));
                };
                try self.prefetch(supports.items);
                for (neighbors) |neighbor| {
                    if (!std.math.isFinite(neighbor.similarity)) continue;
                    const similarity = neighbor.similarity / self_score;
                    const threshold = if (kind == .names) self.state.config.semantic_discovery.name_threshold else self.state.config.semantic_discovery.view_threshold;
                    if (similarity < threshold) continue;
                    const other = (try self.read(t.EntityProjection, neighbor.key)) orelse continue;
                    if (other.kind != kind) continue;
                    const record = (try self.read(t.EntityRecord, try key(self.a, "entity", other.entity_id))) orelse continue;
                    if (!self.candidateMetadataEligible(input_seed, record, request.kind)) continue;
                    self.authorize(record.owner, record.entity.visibility) catch continue;
                    if (record.entity.visibility != seed.entity.visibility or (record.entity.visibility == .private and !eql(u8, seed.owner, record.owner)) or !try self.entityVisible(record)) continue;
                    const canonical_id = (try self.canonical(record.entity.id)) orelse continue;
                    if (!eql(u8, canonical_id, record.entity.id)) continue;
                    const candidate_view = try self.candidateView(rendered, record.entity.id);
                    if (!try self.candidateCurrent(verified, neighbor.key, other, record, candidate_view)) continue;
                    const entry = try matches.getOrPut(self.a, record.entity.id);
                    if (!entry.found_existing) entry.value_ptr.* = .{ .id = record.entity.id };
                    if (kind == .names) entry.value_ptr.names = similarity else entry.value_ptr.view = similarity;
                }
            }
        }
        const items = try self.a.alloc(SemanticMatch, matches.count());
        var values = matches.valueIterator();
        var count: usize = 0;
        while (values.next()) |value| : (count += 1) items[count] = value.*;
        result.items = items;
        result.state = .indexed;
        return result;
    }
    fn entityCandidates(self: *Run, id: []const u8, request: t.EntityCandidateRequest) ![]const u8 {
        if (request.limit == 0 or request.limit > 64) return error.InvalidMemoryRequest;
        const seed = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return error.MemoryNotFound;
        try self.authorize(seed.owner, seed.entity.visibility);
        if (!try self.entityVisible(seed)) return error.MemoryNotFound;
        const canonical_seed = (try self.canonical(id)) orelse return error.MemoryEntitySuppressed;
        const lexical_scope = try self.candidateScope(id, request);
        const union_scope = try std.fmt.allocPrint(self.a, "{s}union/v2/", .{lexical_scope});
        var continuation: CandidateContinuation = .{};
        if (request.cursor) |cursor_value| {
            if (cursor_value.len > candidate_cursor_limit) return error.InvalidMemoryCursor;
            const position = (try self.decodeCursor(union_scope, cursor_value)).?;
            const bytes = position[union_scope.len..];
            const decoder = std.base64.url_safe_no_pad.Decoder;
            const length = decoder.calcSizeForSlice(bytes) catch return error.InvalidMemoryCursor;
            if (length > candidate_json_limit) return error.InvalidMemoryCursor;
            const json = try self.a.alloc(u8, length);
            decoder.decode(json, bytes) catch return error.InvalidMemoryCursor;
            continuation = parse(CandidateContinuation, self.a, json) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidMemoryCursor,
            };
            if (continuation.pending.len > candidate_pending_limit or continuation.semantic.len > candidate_semantic_limit) return error.InvalidMemoryCursor;
            var semantic_seen: std.StringHashMapUnmanaged(void) = .empty;
            for (continuation.semantic) |semantic_id| {
                t.validateId(semantic_id) catch return error.InvalidMemoryCursor;
                if (semantic_seen.contains(semantic_id)) return error.InvalidMemoryCursor;
                try semantic_seen.put(self.a, semantic_id, {});
            }
            var seen: std.StringHashMapUnmanaged(void) = .empty;
            for (continuation.pending) |ref| {
                t.validateId(ref.id) catch return error.InvalidMemoryCursor;
                if (seen.contains(ref.id)) return error.InvalidMemoryCursor;
                try seen.put(self.a, ref.id, {});
            }
            if (continuation.lexical) |position_value| if (position_value.len > candidate_lexical_limit or !std.mem.startsWith(u8, position_value, lexical_scope)) return error.InvalidMemoryCursor;
        }
        var lexical_request = request;
        lexical_request.cursor = try self.encodeCursor(lexical_scope, continuation.lexical);
        const page = if (continuation.pending.len != 0) blk: {
            var paths: std.ArrayList([]const u8) = .empty;
            for (continuation.pending) |ref| try paths.append(self.a, try key(self.a, "entity", ref.id));
            try self.prefetch(paths.items);
            break :blk Page{ .rows = try self.cachedRows(paths.items), .next = continuation.lexical };
        } else try self.candidatePage(seed, lexical_request);
        var rendered: std.StringHashMapUnmanaged(t.EntityView) = .empty;
        var verified: std.StringHashMapUnmanaged(bool) = .empty;
        const semantic = try self.semanticCandidates(seed, request, continuation.pending, &rendered, &verified);
        var candidate_records: std.ArrayList(t.EntityRecord) = .empty;
        var candidate_ids: ResourceSet = .empty;
        for (page.rows) |row| {
            const record = try self.parseRead(t.EntityRecord, row.key, row.json);
            if (candidate_ids.contains(record.entity.id)) continue;
            try candidate_ids.put(self.a, record.entity.id, {});
            try candidate_records.append(self.a, record);
        }
        for (semantic.items) |item| {
            if (candidate_ids.contains(item.id)) continue;
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", item.id))) orelse continue;
            try candidate_ids.put(self.a, record.entity.id, {});
            try candidate_records.append(self.a, record);
        }
        var candidate_paths: std.ArrayList([]const u8) = .empty;
        for (candidate_records.items) |candidate| {
            for (candidate.entity.evidence) |ref| try candidate_paths.append(self.a, try key(self.a, "evidence", ref.id));
            if (semantic.state == .indexed) inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }) |kind| {
                try candidate_paths.append(self.a, try self.semanticProjectionKey(candidate, kind));
            };
        }
        try self.prefetch(candidate_paths.items);
        const seed_distinctions = try self.declaredDistinct(id);
        const seed_view = try self.candidateView(&rendered, id);
        const seed_terms = try self.candidateTerms(seed.entity);
        var views_complete = seed_view.complete;
        const Candidate = t.EntityCandidate;
        var description_query = try lexical.PreparedQuery.init(self.lexical_allocator, seed_view.description);
        defer description_query.deinit();
        var matches: std.ArrayList(Candidate) = .empty;
        for (candidate_records.items) |candidate| {
            try self.service.context.check();
            if (request.cursor != null and continuation.pending.len == 0) {
                var emitted = false;
                for (continuation.semantic) |semantic_id| if (eql(u8, semantic_id, candidate.entity.id)) {
                    emitted = true;
                    break;
                };
                if (emitted) continue;
            }
            if (!self.candidateMetadataEligible(seed, candidate, request.kind)) continue;
            self.authorize(candidate.owner, candidate.entity.visibility) catch continue;
            if (!try self.entityVisible(candidate)) continue;
            const canonical_id = (try self.canonical(candidate.entity.id)) orelse continue;
            if (!eql(u8, canonical_id, candidate.entity.id) or eql(u8, canonical_id, canonical_seed)) continue;
            if (request.kind == .identity and !eql(u8, candidate.entity.type, seed.entity.type)) continue;
            const predicates = if (request.kind == .relationship) try self.relationshipPredicates(seed.entity.type, candidate.entity.type) else &.{};
            if (request.kind == .relationship and predicates.len == 0) continue;
            if (candidate.entity.visibility != seed.entity.visibility or (candidate.entity.visibility == .private and !eql(u8, candidate.owner, seed.owner))) continue;
            var distinct = false;
            for (seed.entity.distinct_from) |other| if (eql(u8, other, candidate.entity.id)) {
                distinct = true;
            };
            for (candidate.entity.distinct_from) |other| if (eql(u8, other, id)) {
                distinct = true;
            };
            if (request.kind == .identity) {
                if (distinct or seed_distinctions.contains(canonical_id) or try self.identityPinnedDistinct(id, candidate.entity.id)) continue;
                const candidate_distinctions = try self.declaredDistinct(candidate.entity.id);
                if (candidate_distinctions.contains(canonical_seed)) continue;
            }
            if (request.kind == .identity and !sameNameNumbers(seed.entity.name, candidate.entity.name)) continue;
            const score = candidateNameScore(seed_terms, try self.candidateTerms(candidate.entity));
            var name_similarity: ?f64 = null;
            var view_similarity: ?f64 = null;
            var semantic_discovered = false;
            for (semantic.items) |item| if (eql(u8, item.id, candidate.entity.id)) {
                name_similarity = item.names;
                view_similarity = item.view;
                semantic_discovered = true;
                break;
            };
            if (score <= 0 and name_similarity == null and view_similarity == null) continue;
            const candidate_view = try self.candidateView(&rendered, candidate.entity.id);
            views_complete = views_complete and candidate_view.complete;
            if (semantic.state == .indexed) inline for (.{ t.ProjectionKind.names, t.ProjectionKind.view }) |kind| {
                if (try self.read(t.EntityProjection, try self.semanticProjectionKey(candidate, kind))) |projection| {
                    if (try self.candidateCurrent(&verified, try self.semanticProjectionKey(candidate, kind), projection, candidate, candidate_view)) {
                        const similarity = (try cosine(semantic.queries[@backingInt(kind)].?, projection.embedding)) / semantic.self_scores[@backingInt(kind)];
                        if (kind == .names) name_similarity = similarity else view_similarity = similarity;
                    }
                }
            };
            try matches.append(self.a, .{ .entity = candidate, .view = candidate_view, .match_score = score, .description_score = try description_query.overlap(candidate_view.description), .name_similarity = name_similarity, .view_similarity = view_similarity, .semantic_discovered = semantic_discovered, .predicates = predicates });
        }
        std.mem.sort(Candidate, matches.items, {}, struct {
            fn less(_: void, lhs: Candidate, rhs: Candidate) bool {
                if (lhs.match_score != rhs.match_score) return lhs.match_score > rhs.match_score;
                if (lhs.description_score != rhs.description_score) return lhs.description_score > rhs.description_score;
                return std.mem.lessThan(u8, lhs.entity.entity.id, rhs.entity.entity.id);
            }
        }.less);
        const count = @min(matches.items.len, request.limit);
        var semantic_ids: std.ArrayList([]const u8) = .empty;
        if (request.cursor != null) try semantic_ids.appendSlice(self.a, continuation.semantic) else {
            for (matches.items) |item| if (item.semantic_discovered) try semantic_ids.append(self.a, item.entity.entity.id);
        }
        const pending = try self.a.alloc(CandidateRef, matches.items.len - count);
        for (matches.items[count..], pending) |item, *ref| ref.* = .{ .id = item.entity.entity.id, .semantic = item.semantic_discovered };
        var next: ?[]const u8 = null;
        if (pending.len != 0 or page.next != null) {
            const json = try self.encode(CandidateContinuation{ .pending = pending, .lexical = page.next, .semantic = semantic_ids.items });
            if (json.len > candidate_json_limit) return error.MemoryCorruptState;
            const encoder = std.base64.url_safe_no_pad.Encoder;
            const encoded = try self.a.alloc(u8, encoder.calcSize(json.len));
            _ = encoder.encode(encoded, json);
            next = try self.encodeCursor(union_scope, try std.fmt.allocPrint(self.a, "{s}{s}", .{ union_scope, encoded }));
            if (next.?.len > candidate_cursor_limit) return error.MemoryCorruptState;
        }
        return self.encode(t.EntityCandidatePage{ .seed = seed, .seed_view = seed_view, .kind = request.kind, .relationship_predicates = if (self.state.config.ontology_pack == .software_team_v1) &[_][]const u8{ "is_a", "part_of", "alternative_to" } else &[_][]const u8{}, .items = matches.items[0..count], .revision = self.state.revision, .config_generation = self.state.config.generation, .next_cursor = next, .complete = views_complete and next == null, .omitted = 0, .semantic_discovery = semantic.state, .retrieval = if (!((try self.read(CandidateIndex, "entity-candidate-index")) orelse CandidateIndex{}).complete) .bounded_primary_fallback else if (seed_terms.len > 128) .bounded_audience_fallback else .indexed_lexical });
    }
    fn proposalOperation(self: *Run, proposal: curation.Proposal, binding: ?ProposalBinding) !prov.Resource {
        if (binding) |value| if (value.operation) |operation| return operation;
        // Recover the operation reference for bindings written before attempt
        // snapshots existed, using the native entity audit index.
        const revision = if (binding) |value| value.verdict_revision orelse proposal.origin.revision else proposal.origin.revision;
        var decision = (try self.read(prov.Resource, try std.fmt.allocPrint(self.a, "audit-event/entity/{s}/{d:0>20}", .{ try hash(self.a, proposal.key), revision }))) orelse return error.MemoryCorruptState;
        decision.kind = .operation;
        return decision;
    }
    fn archiveProposal(self: *Run, proposal: curation.Proposal, binding: ProposalBinding, operation: prov.Resource) !void {
        const path = try key(self.a, "proposal-attempt", operation.id);
        if (try self.read(ProposalAttempt, path) != null or !try self.proposalInputsRetained(binding)) return;
        var inputs = binding;
        if (inputs.revision == 0) inputs.revision = proposal.origin.revision;
        inputs.operation = operation;
        try self.put(path, ProposalAttempt{ .proposal = proposal, .inputs = inputs });
    }
    fn storeProposalAttempt(self: *Run, id: []const u8, binding: ProposalBinding, receipt: t.Operation, previous: ?prov.Resource) !void {
        const proposal = (try self.identityProposal(id)) orelse return error.MemoryCorruptState;
        try self.put(try key(self.a, "proposal-input", id), binding);
        try self.put(try key(self.a, "proposal-attempt", receipt.id), ProposalAttempt{ .proposal = proposal, .inputs = binding, .previous = previous });
    }
    fn proposeEntity(self: *Run, request: t.EntityProposalRequest, receipt: *t.Operation) !void {
        try self.service.context.requireGrant(.curator);
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        if (try self.read(bool, "forgotten-all") != null) return error.MemoryForgotten;
        if (request.actor.len > 0) try t.validateId(request.actor);
        try self.reviewEvidence(request.evidence);
        if (request.actor.len > 0) receipt.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
        const ids = if (request.to) |target| &[_][]const u8{ request.from, target } else &[_][]const u8{request.from};
        var entities: std.ArrayList(std.meta.Child(@FieldType(ProposalBinding, "entities"))) = .empty;
        for (ids) |id| {
            const entity_record = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return error.MemoryNotFound;
            try self.authorize(entity_record.owner, entity_record.entity.visibility);
            if (!try self.entityVisible(entity_record)) return error.MemoryForgotten;
            try entities.append(self.a, .{ .id = id, .revision = entity_record.revision });
        }
        const input: curation.ProposalRequest = .{ .action = std.meta.stringToEnum(curation.EntityAction, @tagName(request.action)).?, .key = request.from, .into = request.to, .options = .{ .replace = request.replace, .pin_distinct = request.pin_distinct, .absorbed = std.meta.stringToEnum(curation.Absorbed, @tagName(request.absorbed)).? } };
        const proposal_id = try curation.proposalId(self.a, input);
        try self.ensureIdentityAuthority();
        const stored = try self.identityPolicy(ids, proposal_id);
        const prior = for (stored.state.proposals) |value| {
            if (eql(u8, value.id, proposal_id)) break @as(?curation.Proposal, value);
        } else null;
        const previous_binding = try self.read(ProposalBinding, try key(self.a, "proposal-input", proposal_id));
        const binding: ProposalBinding = .{ .views = try self.captureEntityViews(ids, request.views), .revision = self.state.revision, .operation = try self.resourceRef(.operation, receipt.id, receipt.revision), .owner = self.service.context.principal, .actor = request.actor, .config_generation = self.state.config.generation, .entities = entities.items, .evidence = request.evidence };
        var previous: ?prov.Resource = null;
        if (prior) |proposal| {
            if (proposal.state != .open) {
                // Replacing open inputs never reopens a terminal decision.
                if (previous_binding == null) return error.MemoryForgotten;
                receipt.proposal_id = proposal_id;
                return;
            }
            if (previous_binding) |old| {
                if (eql(u8, old.owner, binding.owner) and eql(u8, old.actor, binding.actor) and old.config_generation == binding.config_generation and
                    eql(u8, try self.encode(old.entities), try self.encode(binding.entities)) and eql(u8, try self.encode(old.views), try self.encode(binding.views)) and eql(u8, try self.encode(old.evidence), try self.encode(binding.evidence)) and
                    eql(u8, try self.encode(proposal.options), try self.encode(input.options)))
                {
                    receipt.proposal_id = proposal_id;
                    return;
                }
            }
            previous = try self.proposalOperation(proposal, previous_binding);
            if (previous_binding) |old| try self.archiveProposal(proposal, old, previous.?);
        } else if (previous_binding != null) return error.MemoryCorruptState;
        const stamp: curation.Origin = .{ .actor = self.service.context.principal, .created_at_ms = @intCast(@max(0, self.service.context.now_ms)), .revision = self.state.revision };
        const plan = if (prior != null) try curation.refreshProposal(self.a, stored.state, stored.redirects, input, stamp) else try curation.propose(self.a, stored.state, stored.redirects, input, stamp);
        try self.writeIdentityPolicy(stored, .{ .state = plan.state, .redirects = plan.merges }, ids);
        try self.storeProposalAttempt(proposal_id, binding, receipt.*, previous);
        for (entities.items) |entity_ref| try self.decision_targets.append(self.a, try self.resourceRef(.entity, entity_ref.id, entity_ref.revision));
        receipt.proposal_id = proposal_id;
    }
    fn entityProposals(self: *Run, request: operations.Page) ![]const u8 {
        try self.service.context.requireGrant(.curator);
        if (request.limit == 0 or request.limit > t.max_items) return error.InvalidMemoryRequest;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "proposal-input/", try self.decodeCursor("proposal-input/", request.cursor), request.limit);
        const native = try self.identityStore().head(identity_template) != null;
        const legacy = if (!native) ((try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{}) else EntityPolicy{};
        var proposals: std.ArrayList(curation.Proposal) = .empty;
        for (page.rows) |row| if (try self.proposalInputsRetained(try parse(ProposalBinding, self.a, row.json))) {
            if (native) {
                const id = (try self.read([]const u8, try std.fmt.allocPrint(self.a, "proposal-binding-id/{s}", .{row.key["proposal-input/".len..]}))) orelse return error.MemoryCorruptState;
                if (try self.identityProposal(id)) |proposal| try proposals.append(self.a, proposal);
            } else for (legacy.state.proposals) |proposal| if (eql(u8, try key(self.a, "proposal-input", proposal.id), row.key)) {
                try proposals.append(self.a, proposal);
                break;
            };
        };
        return self.encode(t.EntityProposalPage{ .items = proposals.items, .next_cursor = try self.encodeCursor("proposal-input/", page.next), .complete = page.next == null, .revision = self.state.revision });
    }
    fn entityProposal(self: *Run, id: []const u8) ![]const u8 {
        try self.service.context.requireGrant(.curator);
        const binding = (try self.read(ProposalBinding, try key(self.a, "proposal-input", id))) orelse return error.MemoryNotFound;
        if (!try self.proposalInputsRetained(binding)) return error.MemoryNotFound;
        const proposal = (try self.identityProposal(id)) orelse return error.MemoryNotFound;
        const reason = if (binding.verdict_revision) |revision| blk: {
            const provenance = (try self.read(EntityCurationProvenance, try std.fmt.allocPrint(self.a, "entity-curation/{d:0>20}", .{revision}))) orelse break :blk @as(?[]const u8, null);
            break :blk if (try self.curationProvenanceRetained(provenance)) provenance.reason else null;
        } else null;
        return self.encode(t.EntityProposalDetail{ .proposal = proposal, .inputs = try self.publicProposalBinding(binding), .reason = reason, .stale_configuration = binding.config_generation != self.state.config.generation, .revision = self.state.revision });
    }
    fn reviewEntity(self: *Run, id: []const u8, request: t.EntityVerdict, receipt: *t.Operation) !void {
        try self.service.context.requireGrant(.curator);
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        if (request.actor.len > 0) try t.validateId(request.actor);
        if (request.reason.len > t.max_text_bytes or request.pin_distinct and request.accept != false) return error.InvalidMemoryRequest;
        if (request.model) |model| {
            try t.validateId(model.provider);
            try t.validateId(model.model);
            try t.validateId(model.revision);
        }
        try self.reviewEvidence(request.evidence);
        if (request.actor.len > 0) receipt.origin = .{ .agent = .{ .id = request.actor, .kind = .agent } };
        var binding = (try self.read(ProposalBinding, try key(self.a, "proposal-input", id))) orelse return error.MemoryNotFound;
        if (!try self.proposalInputsRetained(binding)) return error.MemoryForgotten;
        if (binding.config_generation != self.state.config.generation) return error.MemoryConfigurationChanged;
        // Retention permits historical audit reads; applying a judgment also
        // requires every original context source to remain current. Space
        // revision fences do not cover edits in ordinary source tables.
        try self.reviewEvidence(binding.evidence);
        try self.ensureIdentityAuthority();
        const proposal = (try self.identityProposal(id)) orelse return error.MemoryNotFound;
        const policy_seeds = if (proposal.into) |target| &[_][]const u8{ proposal.key, target } else &[_][]const u8{proposal.key};
        const stored = try self.identityPolicy(policy_seeds, id);
        if (proposal.state != .open) return error.MemoryEntityConflict;
        try self.bindJudgedViews(binding.views, request.views);
        const previous = try self.proposalOperation(proposal, binding);
        try self.archiveProposal(proposal, binding, previous);
        const inputs = try self.a.alloc(t.EvidenceRef, binding.evidence.len + request.evidence.len);
        if (inputs.len > t.max_items) return error.MemoryInputTooLarge;
        @memcpy(inputs[0..binding.evidence.len], binding.evidence);
        @memcpy(inputs[binding.evidence.len..], request.evidence);
        const artifact = try self.storeJudgment(binding.views orelse return error.MemoryResourceChanged, inputs, null, .private, request.reason, receipt.*, null);
        if (request.accept) |accept| {
            const plan = try curation.decide(self.a, stored.state, stored.redirects, stored.redirects, id, .{ .accept = accept, .pin_distinct = request.pin_distinct }, .{ .actor = self.service.context.principal, .created_at_ms = @intCast(@max(0, self.service.context.now_ms)), .revision = self.state.revision });
            const previous_review = self.reviewing_entity;
            self.reviewing_entity = id;
            defer self.reviewing_entity = previous_review;
            if (accept or request.pin_distinct) try self.curateEntity(.{ .action = if (!accept) .distinct else std.meta.stringToEnum(@FieldType(t.EntityCorrection, "action"), @tagName(proposal.action)).?, .from = proposal.key, .to = proposal.into, .expected_revision = request.expected_revision, .replace = if (accept) proposal.options.replace else false, .pin_distinct = if (accept) proposal.options.pin_distinct else false, .absorbed = if (accept) std.meta.stringToEnum(@FieldType(t.EntityCorrection, "absorbed"), @tagName(proposal.options.absorbed)).? else .reject });
            try self.writeIdentityPolicy(stored, .{ .state = plan.state, .redirects = plan.merges }, policy_seeds);
            self.clearIdentityCaches();
        }
        const ids = if (proposal.into) |target| &[_][]const u8{ proposal.key, target } else &[_][]const u8{proposal.key};
        if (request.reason.len > 0) try self.put(try std.fmt.allocPrint(self.a, "entity-curation/{d:0>20}", .{self.state.revision}), EntityCurationProvenance{ .owner = self.service.context.principal, .recorded_at = self.service.context.now_ms, .revision = self.state.revision, .entity_ids = ids, .reason = request.reason, .evidence = inputs, .judgment = artifact.operation });
        binding.verdict_revision = self.state.revision;
        binding.model = request.model;
        binding.verdict_actor = request.actor;
        if (binding.revision == 0) binding.revision = proposal.origin.revision;
        binding.operation = try self.resourceRef(.operation, receipt.id, receipt.revision);
        try self.storeProposalAttempt(id, binding, receipt.*, previous);
        for (binding.entities) |entity_ref| try self.decision_targets.append(self.a, try self.resourceRef(.entity, entity_ref.id, entity_ref.revision));
        receipt.proposal_id = id;
    }

    fn curateEntity(self: *Run, request: t.EntityCorrection) !void {
        try self.service.context.requireGrant(.curator);
        if (try self.read(bool, "forgotten-all") != null) return error.MemoryForgotten;
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        try self.ensureIdentityAuthority();
        const policy_seeds = if (request.to) |target| &[_][]const u8{ request.from, target } else &[_][]const u8{request.from};
        const stored = try self.identityPolicy(policy_seeds, null);
        const from = (try self.read(t.EntityRecord, try key(self.a, "entity", request.from))) orelse return error.MemoryNotFound;
        try self.authorize(from.owner, from.entity.visibility);
        if (!try self.entityVisible(from)) return error.MemoryForgotten;
        if (request.to) |target| {
            const to = (try self.read(t.EntityRecord, try key(self.a, "entity", target))) orelse return error.MemoryNotFound;
            try self.authorize(to.owner, to.entity.visibility);
            if (!try self.entityVisible(to)) return error.MemoryForgotten;
            if (request.action == .distinct and eql(u8, (try self.canonical(request.from)) orelse return error.MemoryEntitySuppressed, (try self.canonical(target)) orelse return error.MemoryEntitySuppressed)) return error.MemoryEntityConflict;
            if (request.action == .merge) {
                if (from.entity.visibility != to.entity.visibility or (from.entity.visibility == .private and !eql(u8, from.owner, to.owner))) return error.MemoryVisibilityConflict;
            }
        }
        var affected_names: std.StringHashMapUnmanaged(void) = .empty;
        for (try self.identityMembers(request.from)) |name| try affected_names.put(self.a, name, {});
        if (request.to) |target| for (try self.identityMembers(target)) |name| {
            try affected_names.put(self.a, name, {});
        };
        try self.decision_targets.append(self.a, try self.resourceRef(.entity, from.entity.id, from.revision));
        if (request.to) |target| {
            const to = (try self.read(t.EntityRecord, try key(self.a, "entity", target))).?;
            try self.decision_targets.append(self.a, try self.resourceRef(.entity, to.entity.id, to.revision));
        }
        if (request.reason.len > t.max_text_bytes) return error.InvalidMemoryRequest;
        const plan = try curation.planWithOrigin(self.a, try self.encode(stored.state), stored.redirects, stored.redirects, .{
            .action = switch (request.action) {
                .merge => .merge,
                .unmerge => .unmerge,
                .distinct => .distinct,
                .suppress => .suppress,
                .unsuppress => .unsuppress,
            },
            .from = if (request.action == .merge or request.action == .unmerge or request.action == .distinct) request.from else null,
            .to = request.to,
            .keys = if (request.action == .suppress or request.action == .unsuppress) &.{request.from} else &.{},
            .absorbed = switch (request.absorbed) {
                .reject => .reject,
                .release => .release,
                .suppress => .suppress,
            },
            .replace = request.replace,
            .pin_distinct = request.pin_distinct,
        }, .{
            .actor = self.service.context.principal,
            .created_at_ms = @intCast(@max(0, self.service.context.now_ms)),
            .revision = self.state.revision,
        });
        if (request.reason.len > 0) {
            const ids = if (request.to) |target| &[_][]const u8{ request.from, target } else &[_][]const u8{request.from};
            try self.put(try std.fmt.allocPrint(self.a, "entity-curation/{d:0>20}", .{self.state.revision}), EntityCurationProvenance{
                .owner = self.service.context.principal,
                .recorded_at = self.service.context.now_ms,
                .revision = self.state.revision,
                .entity_ids = ids,
                .reason = request.reason,
            });
        }
        const redirects = try curation.RedirectIndex.init(self.a, plan.merges);
        // Check all members against the proposed graph. Re-pointing one alias
        // need not move its old component, but no declared distinction may fold.
        try self.requireDistinctIdentities(&affected_names, redirects);
        var index = try curation.Index.init(self.a, "", try self.encode(plan.state));
        try index.bindRedirects(self.a, redirects);
        try self.writeIdentityPolicy(stored, .{ .state = plan.state, .redirects = plan.merges }, policy_seeds);
        self.clearIdentityCaches();
        if (self.state.config.semantic_discovery.enabled) {
            var names = affected_names.keyIterator();
            while (names.next()) |name| try self.semanticDirty(name.*);
        }
        // Reconcile affected belief groups in the redirect transaction.
        // A work-limit rejection publishes neither identity nor partial facts.
        if (request.action != .distinct) {
            try self.rebuildIdentityGroups(&affected_names);
            try self.invalidate("entity", request.from);
        }
    }
    fn resolveEntity(self: *Run, id: []const u8) ![]const u8 {
        const requested = (try self.read(t.EntityRecord, try key(self.a, "entity", id))) orelse return error.MemoryNotFound;
        try self.authorize(requested.owner, requested.entity.visibility);
        if (!try self.entityVisible(requested)) return error.MemoryNotFound;
        const selected = (try self.canonical(id)) orelse return error.MemoryEntitySuppressed;
        const record = (try self.read(t.EntityRecord, try key(self.a, "entity", selected))) orelse return error.MemoryNotFound;
        try self.authorize(record.owner, record.entity.visibility);
        if (!try self.entityVisible(record)) return error.MemoryNotFound;
        return self.encode(.{ .entity = record.entity, .resource_ref = try self.resourceRef(.entity, record.entity.id, record.revision), .canonical_id = selected, .requested_id = id, .revision = self.state.revision, .identity = "explicit", .uncertainty = "Names and aliases alone do not prove entity identity" });
    }
    fn entity(self: *Run, value: t.Entity) !void {
        try t.validateId(value.id);
        if (try self.read(bool, try key(self.a, "forgotten-entity", value.id)) != null) return error.MemoryForgotten;
        try t.validateId(value.name);
        if (value.evidence.len > t.max_items) return error.MemoryInputTooLarge;
        if (!try self.supportVisible(value.evidence, &.{}, 0)) return error.MemoryEvidenceRequired;
        for (value.evidence) |ref| {
            const source = (try self.evidence(ref.id)).?;
            if (value.visibility == .team and source.evidence.visibility == .private) return error.MemoryVisibilityConflict;
            if (ref.span != null and self.intake_redacted_evidence.contains(ref.id)) return error.InvalidMemorySpan;
            if (ref.span) |span| if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
        }
        if (value.aliases.len > t.max_items or value.distinct_from.len > t.max_items) return error.MemoryInputTooLarge;
        for (value.aliases) |alias| try t.validateId(alias);
        for (value.distinct_from) |other| {
            try t.validateId(other);
            if (eql(u8, other, value.id)) return error.MemoryEntityConflict;
        }
        const path = try key(self.a, "entity", value.id);
        if (self.lineage_head != null) if (try self.rawRead(t.EntityRecord, path)) |existing| {
            try self.authorize(existing.owner, .private);
            if (!valueEqual(t.Entity, existing.entity, value)) return error.MemoryEntityConflict;
        };
        if (try self.read(t.EntityRecord, path)) |old| {
            try self.authorize(old.owner, .private);
            if (!eql(u8, try self.encode(old.entity), try self.encode(value))) return error.MemoryEntityConflict;
            return;
        }
        // Identity is explicit. Names/aliases are discoverable candidates and
        // never constitute authority to merge two canonical IDs.
        const record: t.EntityRecord = .{ .entity = value, .owner = self.service.context.principal, .revision = self.state.revision, .recorded_at = self.service.context.now_ms };
        try self.put(path, record);
        try self.indexCandidate(record, false);
        if (value.evidence.len > 0) try self.mention(.{ .id = try std.fmt.allocPrint(self.a, "entity/{s}", .{try hash(self.a, value.id)}), .entity_id = value.id, .evidence = value.evidence, .visibility = value.visibility });
    }
    fn putAssertion(self: *Run, value: t.StoredAssertion) !void {
        return self.putAssertionWithReasons(value, null);
    }
    fn putAssertionWithReasons(self: *Run, value: t.StoredAssertion, override: ?ReasonBindings) !void {
        const reasons = override orelse (try self.read(ReasonBindings, try self.reasonKey(value.assertion.id, value.revision))) orelse inherit: {
            const previous = (try self.assertion(value.assertion.id)) orelse break :inherit ReasonBindings{};
            break :inherit try self.reasonBindings(previous);
        };
        try self.put(try self.reasonKey(value.assertion.id, value.revision), reasons);
        var record = try self.retainedRecord(value);
        record.recorded_at = self.service.context.now_ms;
        try self.put(try key(self.a, "assertion", record.assertion.id), record);
        try self.put(try historyKey(self.a, record.assertion.id, self.state.revision), record);
        try self.put(try groupKey(self.a, record.assertion, record.owner), .{ .id = record.assertion.id });
        try self.put(try dependencyKey(self.a, "identity", record.assertion.subject, record.assertion.id), .{ .child = record.assertion.id });
        try self.put(try dependencyKey(self.a, "identity", record.assertion.value, record.assertion.id), .{ .child = record.assertion.id });
    }
    // Include the transaction's own writes: reconciliation within an import
    // must have the same semantics as the same observations committed singly.
    fn scanOverlay(self: *Run, prefix: []const u8, cursor: ?[]const u8, limit: u32) !Page {
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, cursor, limit);
        var rows: std.ArrayList(Row) = .empty;
        var present: ResourceSet = .empty;
        for (page.rows) |row| {
            if (self.change_positions.get(row.key)) |position| {
                if (self.changes.items[position].json) |json| try rows.append(self.a, .{ .key = row.key, .json = json });
            } else try rows.append(self.a, row);
            try present.put(self.a, row.key, {});
        }
        // Seek directly to the requested range; unrelated import writes never
        // participate in a prefix scan. The backend boundary still determines
        // pagination, so pending writes cannot be skipped between pages.
        const lower = cursor orelse prefix;
        var search = self.change_ranges.root;
        var first: ?*ChangeTree.Node = null;
        while (search) |node| {
            if (std.mem.order(u8, node.key, lower) == .lt or (cursor != null and eql(u8, node.key, lower))) {
                search = node.children[1];
            } else {
                first = node;
                search = node.children[0];
            }
        }
        var added: usize = 0;
        var node = first;
        while (node) |current| : (node = current.next()) {
            if (!std.mem.startsWith(u8, current.key, prefix)) break;
            if (page.next) |end| if (std.mem.lessThan(u8, end, current.key)) break;
            const change = self.changes.items[self.change_positions.get(current.key).?];
            if (!present.contains(change.key)) if (change.json) |json| {
                try rows.append(self.a, .{ .key = change.key, .json = json });
                added += 1;
                if (added > limit) break;
            };
        }
        std.mem.sort(Row, rows.items, {}, struct {
            fn less(_: void, left: Row, right: Row) bool {
                return std.mem.lessThan(u8, left.key, right.key);
            }
        }.less);
        const count = @min(rows.items.len, limit);
        return .{ .rows = rows.items[0..count], .next = if (rows.items.len > count) rows.items[count - 1].key else page.next };
    }
    // A complete live branch can justify authority; an unavailable branch
    // cannot lend its strongest citation to an independently supported fact.
    // Null means no eligible support, distinct from untrusted support at rank 0.
    fn authority(self: *Run, claim: t.Assertion, valid: ?i64) !?u16 {
        var rank = try self.supportAuthority(claim.evidence, claim.depends_on, valid);
        for (claim.support_sets) |support| if (try self.supportAuthority(support.evidence, support.depends_on, valid)) |candidate| {
            rank = @max(rank orelse 0, candidate);
        };
        return rank;
    }
    fn supportAuthority(self: *Run, refs: []const t.EvidenceRef, parents: []const []const u8, valid: ?i64) !?u16 {
        if (!try self.supportVisibleTime(refs, parents, null, valid, 0)) return null;
        var rank: u16 = 0;
        for (refs) |ref| {
            const source = (try self.evidence(ref.id)) orelse return error.MemoryCorruptState;
            for (self.state.config.source_rules) |rule| if (eql(u8, rule.source_id, source.evidence.source_id)) {
                if (rule.principal) |principal| if (!eql(u8, principal, source.owner)) continue;
                if (rule.producer) |producer| if (!eql(u8, producer, source.evidence.producer)) continue;
                rank = @max(rank, rule.authority);
            };
        }
        return rank;
    }
    fn singlePredicate(self: *Run, name: []const u8) bool {
        for (self.state.config.predicates) |predicate| if (eql(u8, predicate.name, name)) return predicate.cardinality == .one;
        return false;
    }
    fn conflict(self: *Run, record: *t.StoredAssertion, other: []const u8, reason: []const u8) !void {
        record.lifecycle = .disputed;
        record.reason = reason;
        for (record.conflicts_with) |id| if (eql(u8, id, other)) return;
        if (record.conflicts_with.len >= t.max_items) return error.MemoryWorkLimit;
        const ids = try self.a.alloc([]const u8, record.conflicts_with.len + 1);
        @memcpy(ids[0..record.conflicts_with.len], record.conflicts_with);
        ids[record.conflicts_with.len] = other;
        record.conflicts_with = ids;
    }
    fn successor(self: *Run, record: *t.StoredAssertion, next: t.StoredAssertion) !void {
        var replace = record.superseded_by == null;
        if (record.superseded_by) |id| {
            const previous = try self.assertion(id);
            replace = previous == null or previous.?.assertion.valid_from == null or next.assertion.valid_from.? < previous.?.assertion.valid_from.?;
        }
        if (replace) {
            record.superseded_by = next.assertion.id;
            record.superseded_valid_at = next.assertion.valid_from;
        }
        if (record.lifecycle != .disputed) record.lifecycle = .superseded;
        if (record.assertion.valid_to == null or record.assertion.valid_to.? > next.assertion.valid_from.?) record.assertion.valid_to = next.assertion.valid_from;
    }
    fn reconcile(self: *Run, incoming: t.StoredAssertion) !void {
        const claim = incoming.assertion;
        if (try self.canonical(claim.subject) == null) return;
        if (self.objectTyped(claim.predicate) and try self.canonical(claim.value) == null) return;
        if (incoming.lifecycle != .active or incoming.stale or claim.modality != .actual or claim.role == .hypothesized or claim.stability != .settled or !self.singlePredicate(claim.predicate)) return;
        if (!try self.retained(incoming, 0)) return;
        for (try self.canonicalGroup(claim, incoming.owner)) |candidate| {
            try self.service.context.check();
            if (eql(u8, candidate.assertion.id, claim.id)) continue;
            var old = (try self.assertion(candidate.assertion.id)) orelse continue;
            if (old.explicit_disposition) |baseline| if (baseline.lifecycle == .superseded or baseline.lifecycle == .retracted) continue;
            if (old.lifecycle == .retracted or old.stale or old.assertion.modality != .actual or old.assertion.role == .hypothesized or old.assertion.stability != .settled) continue;
            if (try self.canonical(old.assertion.subject) == null) continue;
            if (self.objectTyped(old.assertion.predicate) and try self.canonical(old.assertion.value) == null) continue;
            if (!try self.retained(old, 0)) continue;
            if (!try self.sameCanonicalGroup(claim, old.assertion)) return error.MemoryCorruptState;
            self.authorize(old.owner, old.assertion.visibility) catch continue;
            if (try self.sameCanonicalValue(claim, old.assertion) or !intervalsOverlap(claim, old.assertion)) continue;
            // Evaluate both supports at the start of their overlapping business
            // interval. Historical superseded premises remain eligible there;
            // retracted, forgotten or expired premises never confer authority.
            var valid = claim.valid_from;
            if (old.assertion.valid_from) |start| if (valid == null or start > valid.?) {
                valid = start;
            };
            const incoming_authority = (try self.authority(claim, valid)) orelse continue;
            const previous_authority = (try self.authority(old.assertion, valid)) orelse continue;
            // Unknown business validity is never inferred from ingestion
            // order. Competing unknown/simultaneous assertions are disputed.
            var current = (try self.assertion(claim.id)).?;
            if (self.state.config.features.reconciliation == .deterministic and claim.valid_from != null and old.assertion.valid_from != null and claim.valid_from.? != old.assertion.valid_from.?) {
                const newer = claim.valid_from.? > old.assertion.valid_from.?;
                if ((newer and incoming_authority < previous_authority) or (!newer and previous_authority < incoming_authority)) {
                    try self.conflict(&old, claim.id, "Overlapping assertions have conflicting source authority");
                    try self.conflict(&current, old.assertion.id, "Overlapping assertions have conflicting source authority");
                } else if (newer) {
                    try self.successor(&old, current);
                } else {
                    try self.successor(&current, old);
                }
            } else {
                try self.conflict(&old, claim.id, "Competing actual values have unknown or concurrent business validity");
                try self.conflict(&current, old.assertion.id, "Competing actual values have unknown or concurrent business validity");
            }
            old.revision = self.state.revision;
            current.revision = self.state.revision;
            try self.putAssertion(old);
            try self.putAssertion(current);
            try self.invalidate("assertion", old.assertion.id);
        }
    }
    fn correct(self: *Run, request: t.Correction) !void {
        var record = (try self.assertion(request.id)) orelse return error.MemoryNotFound;
        try self.authorize(record.owner, .private);
        if (!try self.retained(record, 0)) return error.MemoryForgotten;
        if (record.revision != request.expected_revision) return error.MemoryRevisionConflict;
        if (request.reason.len == 0 or request.reason.len > t.max_text_bytes) return error.InvalidMemoryRequest;
        if (request.evidence.len > t.max_items) return error.MemoryInputTooLarge;
        for (request.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (!try self.supportVisible(&.{ref}, &.{}, 0)) return error.MemoryForgotten;
            if (record.assertion.visibility == .team and source.evidence.visibility == .private) return error.MemoryVisibilityConflict;
            if (ref.span != null and self.intake_redacted_evidence.contains(ref.id)) return error.InvalidMemorySpan;
            if (ref.span) |span| if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
        }
        record.lifecycle = request.lifecycle;
        if (request.lifecycle == .active) {
            record.recorded_to = null;
            record.superseded_by = null;
            record.superseded_valid_at = null;
            record.conflicts_with = &.{};
            record.assertion.valid_to = record.asserted_valid_to;
        }
        if (request.stability) |stability| record.assertion.stability = stability;
        record.reason = request.reason;
        record.revision = self.state.revision;
        if (request.lifecycle == .retracted) record.recorded_to = self.service.context.now_ms;
        record.explicit_disposition = explicitDisposition(record);
        record.explicit_disposition.?.provenance = .{ .principal = self.service.context.principal, .recorded_at = self.service.context.now_ms, .evidence = request.evidence };
        var reasons = try self.reasonBindings((try self.assertion(request.id)).?);
        reasons.disposition = null;
        try self.putAssertionWithReasons(record, reasons);
        if (self.state.config.features.reconciliation == .deterministic and record.lifecycle == .active) try self.reconcile(record);
        try self.invalidate("assertion", request.id);
    }
    fn invalidate(self: *Run, kind: []const u8, id: []const u8) !void {
        // Durable work is independent of conversation replay horizons.
        try self.put(try dependencyKey(self.a, "work", kind, id), .{ .kind = kind, .id = id, .revision = self.state.revision });
    }
    fn forget(self: *Run, request: t.Forget) !void {
        if (request.all) try self.requireAdmin();
        if (request.evidence_ids.len + request.assertion_ids.len + request.source_ids.len > t.max_items) return error.MemoryInputTooLarge;
        if (request.all) {
            try self.put("forgotten-all", true);
            // Forget-all permanently closes evidence/identity admission. Its
            // durable fence replaces every projection-level identity guard,
            // so discard the entire curation payload, including free-text
            // provenance, without retaining another copy in audit metadata.
            try self.remove("entity-policy");
            self.clearIdentityCaches();
        }
        for (request.source_ids) |id| {
            try t.validateId(id);
            try self.requireAdmin();
            try self.put(try key(self.a, "forgotten-source", id), true);
            try self.invalidate("source", id);
        }
        for (request.evidence_ids) |id| {
            const source = (try self.evidence(id)) orelse return error.MemoryNotFound;
            try self.authorize(source.owner, .private);
            try self.retireEvidence(source);
            try self.invalidate("evidence", id);
        }
        for (request.assertion_ids) |id| {
            const record = (try self.assertion(id)) orelse return error.MemoryNotFound;
            try self.authorize(record.owner, .private);
            try self.retireAssertion(id);
        }
        // Cleanup is bounded and resumable. Recall checks the durable fences
        // immediately, before asynchronous physical cleanup has completed.
        try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
    }
    fn outcome(self: *Run, request: t.Outcome) !void {
        if (try self.read(bool, "forgotten-all") != null) return error.MemoryForgotten;
        try t.validateId(request.id);
        try t.validateId(request.task_id);
        if (request.used_memory_ids.len > t.max_items or request.detail.len > t.max_text_bytes) return error.MemoryInputTooLarge;
        if (!std.unicode.utf8ValidateSlice(request.detail)) return error.InvalidMemoryRequest;
        if (try self.read(bool, try key(self.a, "forgotten-outcome", request.id)) != null) return error.MemoryForgotten;
        const path = try key(self.a, "outcome", request.id);
        if (try self.read(StoredOutcome, path)) |old| {
            try self.authorize(old.owner, .private);
            if (!try self.outcomeRetained(old)) return error.MemoryForgotten;
            if (!eql(u8, try self.encode(old.outcome), try self.encode(request))) return error.MemoryOutcomeConflict;
            return;
        }
        for (request.used_memory_ids) |id| {
            const record = (try self.assertion(id)) orelse return error.MemoryNotFound;
            try self.authorize(record.owner, record.assertion.visibility);
            if (!try self.evidenceVisible(record)) return error.MemoryNotFound;
        }
        try self.put(path, StoredOutcome{ .outcome = request, .owner = self.service.context.principal, .recorded_at = self.service.context.now_ms, .revision = self.state.revision });
    }
    fn advance(self: *Run, id: []const u8, receipt: *t.Operation) !void {
        if (id.len == 0) return self.maintenance();
        const path = try key(self.a, "operation", id);
        var pending = (try self.read(t.Operation, path)) orelse return error.MemoryNotFound;
        receipt.origin = pending.origin;
        receipt.config_generation = pending.config_generation;
        defer projectProgress(pending, receipt);
        try self.service.context.requireGrant(operationGrant(pending));
        try self.authorize(pending.owner, .private);
        if (pending.phase == .succeeded or pending.phase == .cancelled) return;
        if (pending.phase == .failed) return error.MemoryRetryExhausted;
        if (pending.next_retry_at) |time| if (time > self.service.context.now_ms) return error.MemoryRetryNotReady;
        const caller = self.service.context.principal;
        self.service.context.principal = pending.owner;
        defer self.service.context.principal = caller;
        if (eql(u8, pending.action, "configure")) {
            const processed: anyerror!void = if (self.processing_failure) |failure| failure else self.reindex(path, &pending);
            processed catch |err| {
                // Source authorization is a publication prerequisite, not a
                // producer failure. Keep the durable cursor and retry budget.
                if (err == error.MemorySourceAccessDenied) return err;
                if (err != error.MemoryExternalVectorRequired) pending.attempts += 1;
                try self.processingFailure(path, &pending, err);
                if (pending.phase == .failed or pending.phase == .cancelled) {
                    self.state.pending_config = null;
                    try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
                }
            };
            if (pending.phase == .succeeded) try self.decision_targets.append(self.a, try self.resourceRef(.configuration, try std.fmt.allocPrint(self.a, "{d}", .{pending.config_generation}), pending.config_generation));
            return;
        }
        if (eql(u8, pending.action, "start_evaluation")) {
            self.evaluationQuantum(path, &pending, receipt) catch |err| {
                if (err == error.MemorySourceAccessDenied or err == error.Forbidden) return err;
                pending.attempts += 1;
                try self.processingFailure(path, &pending, err);
                if (pending.phase == .failed or pending.phase == .cancelled) {
                    var evaluation_run = try self.evaluationRun(pending.evaluation_id.?);
                    evaluation_run.phase = .cancelled;
                    evaluation_run.error_code = @errorName(err);
                    evaluation_run.revision = self.state.revision;
                    try self.put(try key(self.a, "evaluation-run", evaluation_run.id), evaluation_run);
                }
            };
            return;
        }
        if (pending.config_generation != self.state.config.generation) {
            try self.cancelOperation(path, &pending, "MemoryConfigurationChanged");
            return;
        }
        if (pending.attempts >= self.state.config.max_attempts) return error.MemoryRetryExhausted;
        if (pending.next_retry_at) |time| if (time > self.service.context.now_ms) return error.MemoryRetryNotReady;
        // Failure finalization publishes only outcome metadata. Do not repeat
        // source acquisition under its short budget or expose any input to a
        // model. The original revision fence and live destination grants guard
        // this update; the next processing attempt revalidates its input plan.
        if (self.processing_failure) |failure| {
            pending.attempts += 1;
            return self.processingFailure(path, &pending, failure);
        }
        var sources: std.ArrayList(t.StoredEvidence) = .empty;
        for (pending.inputs) |input| {
            const source = (try self.evidence(input)) orelse {
                try self.cancelOperation(path, &pending, "MemoryForgotten");
                return;
            };
            try self.authorize(source.owner, source.evidence.visibility);
            if (!try self.processingSourceAvailable(source)) {
                try self.cancelOperation(path, &pending, "MemoryForgotten");
                return;
            }
            try sources.append(self.a, source);
        }
        for (pending.context_inputs) |input| {
            var source = (try self.evidence(input)) orelse return self.processingFailure(path, &pending, error.MemoryForgotten);
            try self.authorize(source.owner, source.evidence.visibility);
            if (!try self.processingSourceAvailable(source)) return self.processingFailure(path, &pending, error.MemoryForgotten);
            source.context_only = true;
            try sources.append(self.a, source);
        }
        pending.attempts += 1;
        const inputs = try self.captureResources(generationInputs, .{sources.items});
        const generate = self.service.backend.generate orelse return error.MemoryCapabilityUnavailable;
        const Invocation = struct {
            run: *Run,
            resources: ResourceSet,
            fn complete(ptr: *anyopaque) !void {
                const invocation: *@This() = @ptrCast(@alignCast(ptr));
                try invocation.run.inferenceCompleted(invocation.resources);
            }
        };
        var invocation: Invocation = .{ .run = self, .resources = inputs.resources };
        const generated = generate(self.service.backend.ptr, self.a, self.state.config, sources.items, .{ .ptr = &invocation, .complete_fn = Invocation.complete }) catch |err| return self.processingFailure(path, &pending, err);
        self.inferenceCompleted(inputs.resources) catch |err| return self.processingFailure(path, &pending, err);
        self.publishGenerated(pending, generated, sources.items) catch |err| return self.processingFailure(path, &pending, err);
        pending.phase = .succeeded;
        pending.assertions_ready = true;
        pending.error_code = null;
        pending.next_retry_at = null;
        try self.put(path, pending);
        try self.remove(try key(self.a, "pending", pending.id));
    }
    fn cancelOperation(self: *Run, path: []const u8, pending: *t.Operation, code: []const u8) !void {
        pending.phase = .cancelled;
        pending.assertions_ready = false;
        pending.error_code = code;
        pending.next_retry_at = null;
        try self.put(path, pending.*);
        try self.remove(try key(self.a, "pending", pending.id));
    }
    fn projectProgress(pending: t.Operation, receipt: *t.Operation) void {
        receipt.phase = pending.phase;
        receipt.evidence_accepted = pending.evidence_accepted;
        receipt.assertions_ready = pending.phase == .succeeded and pending.assertions_ready;
        receipt.attempts = pending.attempts;
        receipt.error_code = pending.error_code;
        receipt.next_retry_at = pending.next_retry_at;
    }
    fn processingSourceAvailable(self: *Run, source: t.StoredEvidence) !bool {
        // Forgetting fences, expiry, deletion and version drift are terminal;
        // an inaccessible current source remains a durable work obligation.
        const previous = self.retention_maintenance;
        self.retention_maintenance = true;
        const durable = self.evidenceRetained(source);
        self.retention_maintenance = previous;
        if (!try durable) return false;
        if (source.evidence.resource) |reference| {
            const snapshot = try self.resourceSnapshot(reference);
            if (snapshot.state == .denied) return error.MemorySourceAccessDenied;
            return snapshot.state == .current;
        }
        return true;
    }
    fn processingFailure(self: *Run, path: []const u8, pending: *t.Operation, err: anyerror) !void {
        // Worker timeouts are finalized under a fresh bounded context even if
        // the outer deadline has grace left. Backoff starts at completion, not
        // at the beginning of a potentially long model call.
        if (self.defer_timeouts and isProcessingTimeout(err)) return err;
        // Authorization failure rolls back the whole attempt, preserving both
        // the durable obligation and its retry budget.
        if (err == error.MemorySourceAccessDenied) return err;
        self.inferred = false;
        self.judged_views.clearRetainingCapacity();
        self.decision_targets.clearRetainingCapacity();
        // A rejected model output publishes no partial assertions or entities.
        try self.resetChanges(&.{});
        // No source-derived changes survive this rollback. Retire its read set
        // as well, so a terminal source-change cancellation can be committed.
        // Any subsequent work captures and fences its own live dependencies.
        self.resource_states.clearRetainingCapacity();
        pending.phase = if (err == error.MemoryForgotten or err == error.MemoryResourceChanged) .cancelled else if (pending.attempts >= self.state.config.max_attempts) .failed else .pending;
        pending.error_code = @errorName(err);
        pending.assertions_ready = false;
        pending.next_retry_at = if (pending.phase == .pending) self.service.context.now_ms +| (@as(i64, 1000) * (@as(i64, 1) << @intCast(@min(pending.attempts, 10)))) else null;
        try self.put(path, pending.*);
        if (pending.phase == .failed or pending.phase == .cancelled) try self.remove(try key(self.a, "pending", pending.id));
    }
    fn publishGenerated(self: *Run, pending: t.Operation, raw: t.Generated, sources: []const t.StoredEvidence) !void {
        const generated = try self.screenTyped(t.Generated, raw);
        try checkScreenedEmbeddings(raw.assertions, generated.assertions);
        // Reconstruct ownership from immutable admitted evidence; never trust
        // a model's producer label, kind, citations or branch ordering.
        try (generation_policy.Plan{ .config = self.state.config, .sources = sources }).validatePublication(generated);
        var private_input = false;
        for ([_][]const []const u8{ pending.inputs, pending.context_inputs }) |inputs| for (inputs) |id| {
            const source = (try self.evidence(id)) orelse return error.MemoryForgotten;
            if (source.evidence.visibility == .private) private_input = true;
        };
        // A model's citations do not prove that uncited private context did
        // not influence its text. Taint every output with all admitted input.
        if (private_input) {
            for (generated.assertions) |claim| if (claim.visibility == .team) return error.MemoryVisibilityConflict;
            for (generated.entities) |generated_entity| if (generated_entity.visibility == .team) return error.MemoryVisibilityConflict;
            for (generated.mentions) |generated_mention| if (generated_mention.visibility == .team) return error.MemoryVisibilityConflict;
        }
        const input_count = pending.inputs.len + pending.context_inputs.len;
        const inputs = try self.a.alloc([]const u8, input_count);
        @memcpy(inputs[0..pending.inputs.len], pending.inputs);
        @memcpy(inputs[pending.inputs.len..], pending.context_inputs);
        const provenance: GenerationProvenance = .{ .operation_id = pending.id, .config_generation = pending.config_generation, .evidence_ids = inputs };
        for (generated.entities) |value| {
            if (try self.read(t.EntityRecord, try key(self.a, "entity", value.id)) == null) try self.put(try self.generationKey("entity", value.id), provenance);
            try self.entity(value);
        }
        for (generated.mentions) |value| {
            if (try self.read(t.StoredMention, try key(self.a, "mention", value.id)) == null) try self.put(try self.generationKey("mention", value.id), provenance);
            try self.mention(value);
        }
        for (generated.assertions) |claim| if (try self.assertion(claim.id) == null) {
            try self.put(try self.generationKey("assertion", claim.id), provenance);
            for (inputs) |input| try self.put(try dependencyKey(self.a, "evidence", input, claim.id), .{ .child = claim.id });
        };
        try self.assertions(generated.assertions, true);
    }
    const VectorProjection = struct { role: ?EmbeddingRole = null, assertion_id: []const u8, generation: u64, model: t.Model, statement_hash: []const u8, embedding: []const f32 };
    fn recallScope(self: *Run, config: t.Config, audience: []const u8) ![]const u8 {
        return hash(self.a, .{ "recall/v2", config.generation, config.embedding_generation, config.models.embedder, audience });
    }
    fn recallProjectionKey(self: *Run, config: t.Config, id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "assertion-recall/{d:0>20}/{s}", .{ config.generation, try hash(self.a, id) });
    }
    fn recallAudience(self: *Run, record: t.StoredAssertion) ![]const u8 {
        return if (record.assertion.visibility == .team) "team" else try std.fmt.allocPrint(self.a, "private/{s}", .{record.owner});
    }
    fn recallIndexName(self: *Run, config: t.Config, audience: []const u8, vector: bool) ![]const u8 {
        return std.fmt.allocPrint(self.a, "memory_recall_{s}_{s}", .{ if (vector) "vector" else "text", if (vector) try self.recallScope(config, audience) else try hash(self.a, .{ "lexical/v2", audience }) });
    }
    fn recallAudienceKey(self: *Run, config: t.Config, audience: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "assertion-recall-audience-v2/{s}", .{try self.recallScope(config, audience)});
    }
    const IndexLease = struct { kind: enum { lexical, recall, semantic }, generation: u64 };
    fn ensureMemoryIndex(self: *Run, name: []const u8, lease: IndexLease, dimensions: ?u32) !void {
        if (self.service.backend.drop_index != null) {
            const path = try std.fmt.allocPrint(self.a, "memory-index/{s}", .{name});
            if (try self.read(IndexLease, path) == null) {
                // Publish the intent before DDL. An interrupted creation is
                // discoverable even when its projection transaction aborts.
                try self.requireAdmin();
                try self.service.context.finish();
                const encoded = try self.encode(lease);
                try self.service.backend.commit(self.service.backend.ptr, self.a, self.state_row, &.{.{ .key = path, .json = encoded }});
                try self.backend_rows.put(self.a, path, encoded);
            }
        }
        if (dimensions) |dims| try self.service.backend.ensure_vector_index.?(self.service.backend.ptr, self.a, name, dims) else try self.service.backend.ensure_text_index.?(self.service.backend.ptr, self.a, name);
    }
    fn indexLeaseLive(config: t.Config, lease: IndexLease) bool {
        return switch (lease.kind) {
            .lexical => true,
            .recall => config.features.semantic_recall and lease.generation == config.generation,
            .semantic => (config.semantic_discovery.enabled or config.features.semantic_recall) and lease.generation == config.embedding_generation,
        };
    }
    fn dropMemoryIndex(self: *Run, name: []const u8) !void {
        const drop = self.service.backend.drop_index orelse return;
        try self.requireAdmin();
        try self.service.context.finish();
        const latest = (try self.service.backend.get(self.service.backend.ptr, self.a, "control")) orelse return error.MemorySpaceNotFound;
        const expected = self.state_row orelse return error.MemorySpaceNotFound;
        if (latest.version != expected.version or !eql(u8, latest.json, expected.json)) return error.MemoryRevisionConflict;
        try drop(self.service.backend.ptr, self.a, name);
    }
    fn ensureRecallAudience(self: *Run, config: t.Config, audience: []const u8) !void {
        const path = try self.recallAudienceKey(config, audience);
        if (try self.read(bool, path) != null) return;
        try self.requireAdmin();
        try self.ensureMemoryIndex(try self.recallIndexName(config, audience, false), .{ .kind = .lexical, .generation = 0 }, null);
        if (config.features.semantic_recall) try self.ensureMemoryIndex(try self.recallIndexName(config, audience, true), .{ .kind = .recall, .generation = config.generation }, config.models.embedder.?.dimensions.?);
        try self.put(path, true);
    }
    fn updateRecallProjection(self: *Run, config: t.Config, record: t.StoredAssertion) anyerror!void {
        const audience = try self.recallAudience(record);
        if (try self.read(bool, try self.recallAudienceKey(config, audience)) == null or try self.read(bool, try self.recallAudienceKey(config, "all")) == null) {
            try self.put(try key(self.a, "assertion-recall-work", record.assertion.id), .{ .id = record.assertion.id });
            return;
        }
        const path = try self.recallProjectionKey(config, record.assertion.id);
        const embedding = if (config.features.semantic_recall) try self.vectorFor(config, record.assertion) else null;
        const statement_hash = try hash(self.a, record.assertion.statement);
        if (try self.read(t.RecallProjection, path)) |prior| {
            if (prior.version == t.recall_index_version and prior.generation == config.generation and eql(u8, prior.owner, record.owner) and prior.visibility == record.assertion.visibility and eql(u8, prior.statement_hash, statement_hash) and valueEqual(?[]const f32, prior.embedding, embedding)) {
                try self.remove(try key(self.a, "assertion-recall-work", record.assertion.id));
                try self.remove(try key(self.a, "assertion-recall-blocked", record.assertion.id));
                return;
            }
        }
        const captured = try self.captureResources(publicationCandidate, .{record});
        if (!captured.result) {
            try self.remove(try self.recallProjectionKey(config, record.assertion.id));
            try self.remove(try key(self.a, "assertion-recall-work", record.assertion.id));
            try self.remove(try key(self.a, "assertion-recall-blocked", record.assertion.id));
            return;
        }
        self.inferred = true;
        var text_indexes: [2][]const u8 = undefined;
        var vector_indexes: [2][]const u8 = undefined;
        for ([_][]const u8{ "all", audience }, 0..) |scope, i| {
            text_indexes[i] = try self.recallIndexName(config, scope, false);
            vector_indexes[i] = try self.recallIndexName(config, scope, true);
        }
        try self.put(path, t.RecallProjection{ .version = t.recall_index_version, .assertion_id = record.assertion.id, .assertion_key = try key(self.a, "assertion", record.assertion.id), .generation = config.generation, .statement_hash = statement_hash, .owner = record.owner, .visibility = record.assertion.visibility, .text = record.assertion.statement, .filter_terms = try recall_filters.terms(self.a, record.assertion), .text_indexes = &text_indexes, .vector_indexes = if (embedding != null) &vector_indexes else &.{}, .embedding = embedding });
        const binding_key = try key(self.a, "assertion-recall-binding", record.assertion.id);
        const Binding = struct { paths: []const []const u8 = &.{} };
        const old = (try self.read(Binding, binding_key)) orelse Binding{};
        var paths: std.ArrayList([]const u8) = .empty;
        for (old.paths) |prior| {
            if (eql(u8, prior, path)) continue;
            // During cutover retain the active generation until the new model serves.
            if (config.generation != self.state.config.generation or (self.state.pending_config != null and std.mem.startsWith(u8, prior, try std.fmt.allocPrint(self.a, "assertion-recall/{d:0>20}/", .{self.state.pending_config.?.generation})))) try paths.append(self.a, prior) else try self.remove(prior);
        }
        try paths.append(self.a, path);
        try self.put(binding_key, .{ .paths = paths.items });
        try self.remove(try key(self.a, "assertion-recall-work", record.assertion.id));
        try self.remove(try key(self.a, "assertion-recall-blocked", record.assertion.id));
    }
    fn recallMaintenance(self: *Run) !void {
        const previous = self.retention_maintenance;
        self.retention_maintenance = false;
        defer self.retention_maintenance = previous;
        if (self.service.backend.search_assertions == null) return;
        try self.requireAdmin();
        var index = (try self.read(t.RecallIndex, "assertion-recall-index")) orelse t.RecallIndex{};
        if (index.version != t.recall_index_version or index.generation != self.state.config.generation) {
            if (index.version != t.recall_index_version) try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
            index = .{ .generation = self.state.config.generation };
        }
        index.complete = false;
        const initial = !index.backfill_complete;
        const blocked = !initial and (try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion-recall-work/", null, 1)).rows.len == 0;
        if (blocked and index.next_retry_at > self.service.context.now_ms) return;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, if (initial) "assertion/" else if (blocked) "assertion-recall-blocked/" else "assertion-recall-work/", if (initial or blocked) index.cursor else null, 8);
        var position = if (initial or blocked) index.cursor else null;
        var progressed = false;
        var yielded = false;
        for (page.rows) |row| {
            const checkpoint = try self.a.dupe(Change, self.changes.items);
            const record = if (initial) try parse(t.StoredAssertion, self.a, row.json) else blk: {
                const work = try parse(struct { id: []const u8 }, self.a, row.json);
                break :blk (try self.assertion(work.id)) orelse {
                    try self.remove(row.key);
                    progressed = true;
                    continue;
                };
            };
            self.recallRecord(record) catch |err| {
                if (err == error.MemorySourceAccessDenied) {
                    try self.resetChanges(checkpoint);
                    try self.put(try key(self.a, "assertion-recall-blocked", record.assertion.id), .{ .id = record.assertion.id });
                    try self.remove(try key(self.a, "assertion-recall-work", record.assertion.id));
                } else if (isProcessingTimeout(err) or err == error.MemoryWorkLimit) {
                    try self.resetChanges(checkpoint);
                    if (isProcessingTimeout(err)) try self.service.context.beginCheckpoint();
                    if (!progressed) {
                        // A record that cannot fit a fresh quantum is durable,
                        // timed debt. Advance past it so other records can run.
                        try self.put(try key(self.a, "assertion-recall-blocked", record.assertion.id), .{ .id = record.assertion.id });
                        try self.remove(try key(self.a, "assertion-recall-work", record.assertion.id));
                        position = row.key;
                    }
                    yielded = true;
                    break;
                } else return err;
            };
            position = row.key;
            progressed = true;
        }
        if (initial) {
            index.cursor = if (yielded) position else page.next;
            index.backfill_complete = !yielded and page.next == null;
        } else if (blocked) index.cursor = if (yielded) position else page.next;
        index.next_retry_at = self.service.context.now_ms +| (if (blocked and page.next != null) @as(i64, 0) else resource_check_interval_ms);
        index.yield_to_semantic = true;
        self.recall_ready_after_commit = index.backfill_complete and !yielded;
        try self.put("assertion-recall-index", index);
    }
    fn recallRecord(self: *Run, record: t.StoredAssertion) !void {
        try self.ensureRecallAudience(self.state.config, "all");
        try self.ensureRecallAudience(self.state.config, try self.recallAudience(record));
        try self.updateRecallProjection(self.state.config, record);
    }
    fn finishRecallPublication(self: *Run) !void {
        if (!self.recall_ready_after_commit) return;
        const backend = self.service.backend;
        const control = (try backend.get(backend.ptr, self.a, "control")) orelse return error.MemorySpaceNotFound;
        if (!eql(u8, control.json, try self.encode(self.state))) return;
        if ((try backend.scan(backend.ptr, self.a, "assertion-recall-work/", null, 1)).rows.len != 0 or (try backend.scan(backend.ptr, self.a, "assertion-recall-blocked/", null, 1)).rows.len != 0) return;
        const row = (try backend.get(backend.ptr, self.a, "assertion-recall-index")) orelse return;
        var index = try parse(t.RecallIndex, self.a, row.json);
        if (!index.backfill_complete or index.generation != self.state.config.generation) return;
        index.complete = true;
        try self.requireAdmin();
        try self.service.context.finish();
        if (self.inferred) try self.validateResources();
        try backend.commit(backend.ptr, self.a, control, &.{ .{ .key = "assertion-recall-index", .json = try self.encode(index) }, .{ .key = "control", .json = control.json } });
    }
    fn vectorKey(self: *Run, generation: u64, id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "vector/{d:0>20}/{s}", .{ generation, try hash(self.a, id) });
    }
    fn putVector(self: *Run, config: t.Config, claim: t.Assertion, embedding: []const f32) !void {
        const model = config.models.embedder orelse return error.MemoryModelRequired;
        if (embedding.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
        for (embedding) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        try self.put(try self.vectorKey(config.embedding_generation, claim.id), VectorProjection{ .role = .document, .assertion_id = claim.id, .generation = config.embedding_generation, .model = model, .statement_hash = try hash(self.a, claim.statement), .embedding = embedding });
        // Pending vectors are already durable reindex progress. Their serving
        // projection backfill starts at cutover; they must not dirty the active
        // generation's recall queue while uploads or inference are in progress.
        if (self.service.backend.search_assertions != null and config.generation == self.state.config.generation) if (try self.assertion(claim.id)) |record| try self.updateRecallProjection(config, record);
    }
    fn vectorFor(self: *Run, config: t.Config, claim: t.Assertion) !?[]const f32 {
        const projection = (try self.read(VectorProjection, try self.vectorKey(config.embedding_generation, claim.id))) orelse return null;
        if (projection.generation != config.embedding_generation or !eql(u8, projection.assertion_id, claim.id) or !eql(u8, projection.statement_hash, try hash(self.a, claim.statement)) or !valueEqual(?t.Model, projection.model, config.models.embedder)) return error.MemoryEmbeddingMismatch;
        if (projection.role != .document and !eql(u8, projection.model.provider, "external")) return null;
        return projection.embedding;
    }
    fn stageEmbeddings(self: *Run, request: t.EmbeddingBatch) !void {
        try self.requireAdmin();
        const config = self.state.pending_config orelse return error.MemoryConfigurationChanged;
        if (request.generation != config.embedding_generation or config.models.embedder == null or !valueEqual(t.Model, request.model, config.models.embedder.?)) return error.MemoryEmbeddingMismatch;
        if (request.items.len == 0 or request.items.len > 64) return error.InvalidMemoryRequest;
        for (request.items) |item| {
            const record = (try self.assertion(item.id)) orelse return error.MemoryNotFound;
            if (!try self.retained(record, 0)) return error.MemoryNotFound;
            try self.putVector(config, record.assertion, item.embedding);
        }
    }
    fn reindex(self: *Run, path: []const u8, pending: *t.Operation) !void {
        try self.requireAdmin();
        const config = self.state.pending_config orelse return error.MemoryConfigurationChanged;
        if (config.generation != pending.config_generation) return error.MemoryConfigurationChanged;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion/", pending.cursor, 8);
        var position = pending.cursor;
        var progressed = false;
        var yielded = false;
        for (page.rows) |row| {
            self.reindexAssertion(config, row) catch |err| {
                if ((!isProcessingTimeout(err) and err != error.MemoryWorkLimit) or !progressed) return err;
                try self.service.context.beginCheckpoint();
                yielded = true;
                break;
            };
            position = row.key;
            progressed = true;
        }
        // Checkpoint completed vectors without cutting over the serving model.
        // The unfinished assertion remains after the durable cursor.
        if (!yielded and page.next == null) {
            try self.applyConfiguration(config);
            self.state.pending_config = null;
            try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
            pending.phase = .succeeded;
            pending.assertions_ready = true;
            try self.remove(try key(self.a, "pending", pending.id));
        }
        pending.cursor = if (yielded) position else page.next;
        pending.attempts = 0;
        pending.error_code = null;
        pending.next_retry_at = null;
        try self.put(path, pending.*);
    }
    fn reindexAssertion(self: *Run, config: t.Config, row: Row) !void {
        const record = try parse(t.StoredAssertion, self.a, row.json);
        const inputs = try self.captureResources(publicationCandidate, .{record});
        if (!inputs.result) return;
        self.inferred = true;
        if (try self.vectorFor(config, record.assertion) != null) return;
        if (eql(u8, config.models.embedder.?.provider, "external")) return error.MemoryExternalVectorRequired;
        const embedding = try self.embedClaimDocument(config.models.embedder.?, record.assertion.statement, inputs.resources);
        try self.putVector(config, record.assertion, embedding);
    }
    fn retireAssertion(self: *Run, id: []const u8) !void {
        try self.semanticInvalidateAssertion(id);
        if (try self.read(struct { paths: []const []const u8 }, try key(self.a, "assertion-recall-binding", id))) |binding| for (binding.paths) |path| try self.remove(path);
        try self.remove(try key(self.a, "assertion-recall-binding", id));
        try self.remove(try key(self.a, "assertion-recall-work", id));
        try self.remove(try key(self.a, "assertion-recall-blocked", id));
        // Retirement is irreversible physical cleanup, not a belief revision.
        // Fence identity reuse before dropping payload and generation metadata.
        try self.put(try key(self.a, "forgotten-assertion", id), true);
        try self.remove(try self.generationKey("assertion", id));
        try self.remove(try key(self.a, "assertion", id));
        try self.invalidate("assertion", id);
    }
    fn retireEvidence(self: *Run, source: t.StoredEvidence) !void {
        try self.remove(try key(self.a, "evidence", source.evidence.id));
        try self.put(try key(self.a, "forgotten-evidence", source.evidence.id), true);
        try self.retireEpisode(source);
        if (source.evidence.resource) |reference| {
            try self.remove(try std.fmt.allocPrint(self.a, "resource-evidence/{s}/{s}", .{ try hash(self.a, reference.identity()), try hash(self.a, source.evidence.id) }));
            try self.remove(try key(self.a, "resource-watch", source.evidence.id));
        }
    }
    // Admission arms an idle poller or brings a pending deadline forward. It
    // never postpones existing work, including an unfinished bounded round.
    fn scheduleResourceCheck(self: *Run) void {
        const due = if (self.state.resource_cursor != null) self.service.context.now_ms else self.service.context.now_ms +| resource_check_interval_ms;
        if (self.state.next_resource_check_at == null or due < self.state.next_resource_check_at.?) self.state.next_resource_check_at = due;
    }
    fn pollResources(self: *Run) !void {
        const due = self.state.next_resource_check_at orelse return;
        if (due > self.service.context.now_ms) return;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "resource-watch/", self.state.resource_cursor, 16);
        for (page.rows) |row| {
            var watch = try parse(struct { evidence_id: []const u8, last_state: prov.ResourceState }, self.a, row.json);
            const source = (try self.evidence(watch.evidence_id)) orelse {
                try self.remove(row.key);
                continue;
            };
            const reference = source.evidence.resource orelse return error.MemoryCorruptState;
            const state = (try self.resourceSnapshot(reference)).state;
            if (state != watch.last_state) {
                watch.last_state = state;
                try self.put(row.key, watch);
                if (self.state.config.semantic_discovery.enabled) try self.invalidate("semantic-resource", try self.semanticResourceId(reference));
                if (state == .changed) try self.invalidate("resource", watch.evidence_id);
                if (state == .missing) try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
            }
        }
        self.state.resource_cursor = page.next;
        self.state.next_resource_check_at = if (page.next != null) self.service.context.now_ms else if (page.rows.len == 0) null else self.service.context.now_ms +| resource_check_interval_ms;
    }
    fn maintenance(self: *Run) !void {
        if (!self.service.context.admin) return error.Forbidden;
        // A worker credential's denied read is not proof of global source
        // retirement. Foreground authorization still hides it immediately.
        self.retention_maintenance = true;
        defer self.retention_maintenance = false;
        const kind = try @import("scheduling.zig").maintenanceKind(self.service.backend, self.a, self.service.context.now_ms, self.state);
        if (kind == .recall) return self.recallMaintenance();
        if (kind != .retention) {
            if (self.service.backend.search_assertions != null) if (try self.read(t.RecallIndex, "assertion-recall-index")) |prior| {
                var index = prior;
                index.yield_to_semantic = false;
                try self.put("assertion-recall-index", index);
            };
            return self.semanticMaintenance();
        }
        try self.pollResources();
        try self.semanticTimers();
        var candidate_index = (try self.read(CandidateIndex, "entity-candidate-index")) orelse CandidateIndex{};
        if (!candidate_index.complete) {
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "entity/", candidate_index.cursor, 16);
            for (page.rows) |row| try self.indexCandidate(try parse(t.EntityRecord, self.a, row.json), false);
            candidate_index.cursor = page.next;
            candidate_index.complete = page.next == null;
            try self.put("entity-candidate-index", candidate_index);
        }
        if (self.state.next_expiry_at) |time| if (time <= self.service.context.now_ms) {
            try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
            self.state.next_expiry_at = null;
        };
        // One dependency page per transaction. Retention loss propagates a
        // durable retirement frontier. Current inapplicability is evaluated by
        // the shared temporal support traversal; it is never global staleness.
        const work = try self.service.backend.scan(self.service.backend.ptr, self.a, "dependency/work/", null, 1);
        if (work.rows.len > 0) {
            const item = try parse(DependencyWork, self.a, work.rows[0].json);
            const prefix = try std.fmt.allocPrint(self.a, "dependency/{s}/{s}/", .{ if (eql(u8, item.kind, "resource")) "evidence" else item.kind, try hash(self.a, item.id) });
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, item.cursor, 64);
            var position = item.cursor;
            for (page.rows) |row| {
                if (!try self.maintenanceStep(maintainChild, .{ row, item })) {
                    try self.put(work.rows[0].key, DependencyWork{ .kind = item.kind, .id = item.id, .revision = item.revision, .cursor = position });
                    return;
                }
                position = row.key;
            }
            if (page.next) |cursor| try self.put(work.rows[0].key, DependencyWork{ .kind = item.kind, .id = item.id, .revision = item.revision, .cursor = cursor }) else try self.remove(work.rows[0].key);
        }
        if (try self.read(struct { cursor: ?[]const u8 = null }, "cleanup")) |cleanup| {
            // Include history appended by this quantum's invalidation work;
            // completing cleanup cannot leave newly written forgotten text.
            const page = try self.scanOverlay("", cleanup.cursor, 64);
            var continuation = page.next;
            var position = cleanup.cursor;
            var deferred = false;
            for (page.rows) |row| {
                if (!try self.maintenanceStep(maintainRow, .{row})) {
                    continuation = position;
                    deferred = true;
                    break;
                }
                position = row.key;
            }
            if (deferred or continuation != null) try self.put("cleanup", .{ .cursor = continuation }) else try self.remove("cleanup");
        }
    }
    // Each artifact is atomic, while a quantum commits all completed artifacts.
    // Work exhaustion on a later artifact yields its cursor instead of rolling
    // back earlier progress. Restore overlay/state in case admission wrote before
    // evaluating a later provenance branch; cache entries cannot survive rollback.
    fn maintenanceStep(self: *Run, comptime handler: anytype, args: anytype) !bool {
        const work_before = self.support_work;
        const checkpoint = try self.a.dupe(Change, self.changes.items);
        const state_before = self.state;
        @call(.auto, handler, .{self} ++ args) catch |err| {
            if (err != error.MemoryWorkLimit or work_before == 0) return err;
            try self.resetChanges(checkpoint);
            self.state = state_before;
            self.views.clearRetainingCapacity();
            self.historical.clearRetainingCapacity();
            return false;
        };
        return true;
    }
    fn maintainChild(self: *Run, row: Row, item: DependencyWork) anyerror!void {
        const child = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
        if (eql(u8, item.kind, "semantic-resource") or eql(u8, item.kind, "semantic-assertion")) {
            const old_branch = self.service.context.branch;
            self.service.context.branch = child.branch;
            defer self.service.context.branch = old_branch;
            if (self.state.config.semantic_discovery.enabled and try self.read(t.EntityRecord, try key(self.a, "entity", child.child)) != null) try self.semanticDirty(child.child) else try self.remove(row.key);
            return;
        }
        if (try self.assertion(child.child)) |value| {
            if (self.state.config.semantic_discovery.enabled) {
                try self.semanticDirty(value.assertion.subject);
                if (self.objectTyped(value.assertion.predicate)) try self.semanticDirty(value.assertion.value);
            }
            var record = value;
            if (!try self.retained(record, 0)) {
                try self.retireAssertion(child.child);
            } else if (!record.stale and record.revision <= item.revision and (eql(u8, item.kind, "entity") or (eql(u8, item.kind, "resource") and (record.assertion.kind == .summary or record.assertion.kind == .procedural)))) {
                // Identity changes invalidate opaque derived artifacts;
                // ordinary support changes do not rewrite factual history.
                // A delayed event cannot invalidate a regenerated artifact.
                record.stale = true;
                record.revision = self.state.revision;
                try self.putAssertion(record);
                try self.invalidate("assertion", child.child);
            }
        }
    }
    fn maintainRow(self: *Run, row: Row) anyerror!void {
        if (std.mem.startsWith(u8, row.key, "entity-authority/") or std.mem.startsWith(u8, row.key, "proposal-binding-id/")) {
            if (try self.read(bool, "forgotten-all") != null) try self.remove(row.key);
        } else if (eql(u8, row.key, "entity-policy")) {
            // Also scrub policy rows left by earlier implementations
            // or a restored pre-cleanup snapshot of a forgotten space.
            if (try self.read(bool, "forgotten-all") != null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "evaluation-case/")) {
            var value = try parse(t.evaluation.StoredCase, self.a, row.json);
            if (!value.forgotten and !try self.evaluationCaseAvailable(value)) {
                value.forgotten = true;
                value.value = .{ .id = try hash(self.a, value.value.id), .task = value.value.task, .input = .null };
                value.fingerprint = "";
                try self.put(row.key, value);
            }
        } else if (std.mem.startsWith(u8, row.key, "evaluation-result/")) {
            var result = try parse(t.evaluation.Result, self.a, row.json);
            if (!result.scrubbed) {
                const run = (try self.read(t.evaluation.Run, try key(self.a, "evaluation-run", result.run))) orelse return error.MemoryCorruptState;
                const value = self.evaluationCase(run, result.case_id) catch |err| switch (err) {
                    error.MemoryNotFound => null,
                    else => return err,
                };
                if (value == null or !try self.evaluationCaseAvailable(value.?)) {
                    result.prediction = null;
                    result.unavailable = true;
                    result.scrubbed = true;
                    result.case_id = try hash(self.a, result.case_id);
                    result.input_fingerprint = "";
                    try self.put(row.key, result);
                }
            }
        } else if (std.mem.startsWith(u8, row.key, "lineage-entity/")) {
            const entity_record = try parse(t.EntityRecord, self.a, row.json);
            if (!try self.entityRetained(entity_record)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "supersession-proposal/")) {
            const proposal = try parse(t.SupersessionProposal, self.a, row.json);
            if (!try self.supersessionProposalRetained(proposal)) {
                try self.remove(row.key);
                try self.remove(try std.fmt.allocPrint(self.a, "supersession-open/{s}/{s}", .{ try hash(self.a, proposal.branch), try hash(self.a, proposal.id) }));
                try self.put(try key(self.a, "forgotten-supersession-proposal", proposal.id), true);
            }
        } else if (std.mem.startsWith(u8, row.key, "evidence/")) {
            const source = try parse(t.StoredEvidence, self.a, row.json);
            const expires = self.evidenceExpiry(source);
            if (expires) |time| if (time > self.service.context.now_ms and (self.state.next_expiry_at == null or time < self.state.next_expiry_at.?)) {
                self.state.next_expiry_at = time;
            };
            if (!try self.evidenceRetained(source)) try self.retireEvidence(source);
        } else if (std.mem.startsWith(u8, row.key, "resource-evidence/")) {
            // Reclaim orphaned bindings as well as bindings written before
            // evidence retirement removed its indexes atomically.
            const binding = try parse(struct { evidence_id: []const u8 }, self.a, row.json);
            const source = try self.evidence(binding.evidence_id);
            if (source == null or !try self.evidenceRetained(source.?)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "mention/")) {
            const stored = try parse(t.StoredMention, self.a, row.json);
            const entity_record = try self.read(t.EntityRecord, try key(self.a, "entity", stored.mention.entity_id));
            if (entity_record == null or !try self.entityRetained(entity_record.?) or !try self.retainedSupport(stored.mention.evidence, &.{}, 0) or !try self.generationRetained("mention", stored.mention.id)) {
                try self.put(try key(self.a, "forgotten-mention", stored.mention.id), true);
                try self.remove(try self.generationKey("mention", stored.mention.id));
                // Remove reverse bindings in the retirement transaction. The
                // cleanup cursor visits dependency/ before mention/, so
                // deferring these would strand dead adjacency until another
                // erasure and eventually crowd live mentions out of recall.
                try self.remove(try dependencyKey(self.a, "mention-entity", stored.mention.entity_id, stored.mention.id));
                for (stored.mention.evidence) |ref| try self.remove(try dependencyKey(self.a, "mention-evidence", ref.id, stored.mention.id));
                try self.remove(row.key);
            }
        } else if (std.mem.startsWith(u8, row.key, "dependency/mention-")) {
            const binding = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
            if (try self.read(t.StoredMention, try key(self.a, "mention", binding.child)) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "episode-turn/")) {
            const binding = try parse(struct { id: []const u8 }, self.a, row.json);
            if (try self.evidence(binding.id) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "judgment/")) {
            const artifact = try parse(JudgmentArtifact, self.a, row.json);
            if (!try self.judgmentAvailable(artifact, .retained)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "relationship-judgment/")) {
            const ref = try parse(prov.Resource, self.a, row.json);
            if (try self.read(JudgmentArtifact, try key(self.a, "judgment", ref.id)) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "relationship-proposal/")) {
            var proposal = try parse(t.RelationshipProposal, self.a, row.json);
            if (!try self.relationshipRetained(proposal)) {
                try self.put(try key(self.a, "forgotten-relationship-proposal", proposal.id), true);
                try self.remove(row.key);
            } else if (proposal.verdict_revision != null) {
                const judgment = try self.relationshipJudgment(proposal);
                if (judgment == null or !try self.judgmentAvailable(judgment.?, .retained) or !try self.retainedSupport(proposal.verdict_evidence, &.{}, 0)) {
                    redactVerdict(&proposal);
                    try self.put(row.key, proposal);
                }
            }
        } else if (std.mem.startsWith(u8, row.key, "proposal-input/")) {
            if (!try self.proposalInputsRetained(try parse(ProposalBinding, self.a, row.json))) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "proposal-attempt/")) {
            const attempt = try parse(ProposalAttempt, self.a, row.json);
            if (!try self.proposalInputsRetained(attempt.inputs)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "entity-curation/")) {
            const provenance = try parse(EntityCurationProvenance, self.a, row.json);
            if (!try self.curationProvenanceRetained(provenance)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "entity/")) {
            const stored = try parse(t.EntityRecord, self.a, row.json);
            if (try self.read(bool, "forgotten-all") != null or !try self.entityRetained(stored)) {
                try self.indexCandidate(stored, true);
                try self.put(try key(self.a, "forgotten-entity", stored.entity.id), true);
                try self.remove(try self.generationKey("entity", stored.entity.id));
                try self.remove(row.key);
            }
        } else if (std.mem.startsWith(u8, row.key, "decision/")) {
            const decision = try parse(DecisionProvenance, self.a, row.json);
            if (try self.assertion(decision.assertion_id) == null or !try self.decisionAvailable(row.key, 0, .retained)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "reason-binding/")) {
            var parts = std.mem.splitScalar(u8, row.key, '/');
            _ = parts.next();
            const id_hash = parts.next() orelse return error.MemoryCorruptState;
            if (try self.read(std.json.Value, try std.fmt.allocPrint(self.a, "assertion/{s}", .{id_hash})) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "generation-input/")) {
            const owner_key = row.key["generation-input/".len..];
            if (try self.read(std.json.Value, owner_key) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "memory-index/")) {
            const lease = try parse(IndexLease, self.a, row.json);
            if (!indexLeaseLive(self.state.config, lease) and (self.state.pending_config == null or !indexLeaseLive(self.state.pending_config.?, lease))) {
                try self.dropMemoryIndex(row.key["memory-index/".len..]);
                try self.remove(row.key);
            }
        } else if (std.mem.startsWith(u8, row.key, "assertion-recall-audience/")) {
            const scope = row.key["assertion-recall-audience/".len..];
            if (scope.len != 64) return error.MemoryCorruptState;
            try self.dropMemoryIndex(try std.fmt.allocPrint(self.a, "memory_recall_text_{s}", .{scope}));
            try self.dropMemoryIndex(try std.fmt.allocPrint(self.a, "memory_recall_vector_{s}", .{scope}));
            try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "assertion-recall/")) {
            const projection = try parse(t.RecallProjection, self.a, row.json);
            if (projection.version < t.recall_index_version or (projection.generation != self.state.config.generation and (self.state.pending_config == null or projection.generation != self.state.pending_config.?.generation)) or try self.assertion(projection.assertion_id) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic/") or std.mem.startsWith(u8, row.key, "entity-semantic-query/")) {
            const query_role = std.mem.startsWith(u8, row.key, "entity-semantic-query/");
            const projection = try parse(t.EntityProjection, self.a, row.json);
            const saved_branch = self.service.context.branch;
            const saved_head = self.lineage_head;
            const saved_raw = self.lineage_raw;
            defer {
                self.service.context.branch = saved_branch;
                self.lineage_head = saved_head;
                self.lineage_raw = saved_raw;
                self.views.clearRetainingCapacity();
            }
            self.service.context.branch = projection.branch;
            if (self.state.config.features.lineage) {
                self.lineage_head = try self.rawRead(t.LineageHead, try key(self.a, "lineage-head", if (projection.branch.len == 0) "main" else projection.branch));
                self.lineage_raw = false;
            }
            self.views.clearRetainingCapacity();
            const record = try self.read(t.EntityRecord, try key(self.a, "entity", projection.entity_id));
            const lease: IndexLease = .{ .kind = .semantic, .generation = projection.generation };
            if (!query_role and !indexLeaseLive(self.state.config, lease) and (self.state.pending_config == null or !indexLeaseLive(self.state.pending_config.?, lease))) {
                const rest = row.key["entity-semantic/".len..];
                const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.MemoryCorruptState;
                try self.put(try std.fmt.allocPrint(self.a, "memory-index/{s}", .{rest[0..slash]}), lease);
            }
            if (!self.state.config.semantic_discovery.enabled or record == null or !try self.entityRetained(record.?) or !eql(u8, row.key, if (query_role) try self.semanticQueryKey(record.?, projection.kind) else try self.semanticProjectionKey(record.?, projection.kind))) {
                try self.remove(row.key);
            } else {
                const selected = try self.canonical(record.?.entity.id);
                if (selected == null or !eql(u8, selected.?, record.?.entity.id)) {
                    try self.remove(row.key);
                } else {
                    const view = self.semanticView(record.?) catch |err| switch (err) {
                        error.MemoryNotFound, error.MemoryEntitySuppressed => return,
                        else => return err,
                    };
                    if (!try self.semanticCurrent(projection, record.?, view)) {
                        try self.remove(row.key);
                        try self.semanticDirty(record.?.entity.id);
                    }
                }
            }
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic-branch/")) {
            const target = try std.json.parseFromSliceLeaky(struct { id: ?[]const u8 = null }, self.a, row.json, .{ .ignore_unknown_fields = true });
            if (try self.read(bool, "forgotten-all") != null) {
                try self.remove(row.key);
            } else if (target.id) |id| {
                const record = try self.rawRead(t.EntityRecord, try key(self.a, "entity", id));
                if (!self.state.config.semantic_discovery.enabled or record == null or !try self.entityRetained(record.?)) try self.remove(row.key);
            }
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic-binding/")) {
            const binding = try parse(SemanticBinding, self.a, row.json);
            const record = try self.read(t.EntityRecord, try key(self.a, "entity", binding.id));
            if (!self.state.config.semantic_discovery.enabled or record == null or !try self.entityRetained(record.?)) try self.semanticBindings(binding.id, .empty, &.{}, .empty);
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic-timer/")) {
            const timer = try parse(SemanticTimer, self.a, row.json);
            const record = try self.read(t.EntityRecord, try key(self.a, "entity", timer.id));
            if (!self.state.config.semantic_discovery.enabled or record == null or !try self.entityRetained(record.?)) try self.scheduleSemanticTimer(timer.id, null);
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic-due/")) {
            const timer = try parse(SemanticTimer, self.a, row.json);
            const saved_branch = self.service.context.branch;
            self.service.context.branch = timer.branch;
            defer self.service.context.branch = saved_branch;
            const current = try self.read(SemanticTimer, try self.semanticKey("entity-semantic-timer", timer.id));
            if (current == null or current.?.at != timer.at) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic-failure/")) {
            const failure = try parse(SemanticFailure, self.a, row.json);
            const record = try self.read(t.EntityRecord, try key(self.a, "entity", failure.id));
            if (!self.state.config.semantic_discovery.enabled or failure.generation != self.state.config.generation or record == null or !try self.entityRetained(record.?)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "entity-semantic-work/") or std.mem.startsWith(u8, row.key, "entity-semantic-blocked/")) {
            const work = try parse(struct { id: []const u8 }, self.a, row.json);
            const record = try self.read(t.EntityRecord, try key(self.a, "entity", work.id));
            if (!self.state.config.semantic_discovery.enabled or record == null or !try self.entityRetained(record.?)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "vector/")) {
            const projection = try parse(VectorProjection, self.a, row.json);
            const record = try self.assertion(projection.assertion_id);
            const serving = self.state.config.features.semantic_recall and projection.generation == self.state.config.embedding_generation;
            const staging = if (self.state.pending_config) |pending| projection.generation == pending.embedding_generation else false;
            if ((!serving and !staging) or record == null or !try self.retained(record.?, 0)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "outcome/")) {
            const stored = try parse(StoredOutcome, self.a, row.json);
            if (!try self.outcomeRetained(stored)) {
                try self.put(try key(self.a, "forgotten-outcome", stored.outcome.id), true);
                try self.remove(row.key);
            }
        } else if (std.mem.startsWith(u8, row.key, "assertion/") or std.mem.startsWith(u8, row.key, "history/")) {
            const record = (try self.read(t.StoredAssertion, row.key)) orelse return;
            if (!try self.retained(record, 0)) {
                if (std.mem.startsWith(u8, row.key, "assertion/")) try self.retireAssertion(record.assertion.id) else try self.remove(row.key);
            } else try self.put(row.key, try self.retainedRecord(record));
        }
    }

    fn resourceRef(self: *Run, kind: prov.Kind, id: []const u8, revision: u64) !prov.Resource {
        return .{ .database = self.state.database, .namespace = self.state.namespace, .table = try std.fmt.allocPrint(self.a, "memory_{s}", .{self.state.space}), .table_id = self.state.table_id, .kind = kind, .id = id, .revision = revision };
    }
    fn auditIndex(self: *Run, resource: prov.Resource) ![]const u8 {
        return std.fmt.allocPrint(self.a, "provenance/{s}/{s}/{d:0>20}", .{ @tagName(resource.kind), try hash(self.a, resource.id), resource.revision });
    }
    fn recordTarget(self: *Run, decision: prov.Resource, target: prov.Resource) !void {
        try self.put(try std.fmt.allocPrint(self.a, "audit-target/{s}/{s}/{s}/{d:0>20}", .{ try hash(self.a, decision.id), @tagName(target.kind), try hash(self.a, target.id), target.revision }), target);
        try self.put(try std.fmt.allocPrint(self.a, "audit-event/{s}/{s}/{d:0>20}", .{ @tagName(target.kind), try hash(self.a, target.id), decision.revision }), decision);
    }
    const OperationVersion = struct { record: t.OperationStatus, recorded_at: i64, decision: ?prov.Resource = null };
    fn operationVersionKey(self: *Run, id: []const u8, revision: u64) ![]const u8 {
        return std.fmt.allocPrint(self.a, "operation-history/{s}/{d:0>20}", .{ try hash(self.a, id), revision });
    }
    fn recordOperationVersions(self: *Run, receipt: t.Operation) !void {
        const decision = try self.read(prov.Decision, try key(self.a, "audit", receipt.id));
        const changes = try self.a.dupe(Change, self.changes.items);
        for (changes) |change| if (change.json) |json| {
            if (!std.mem.startsWith(u8, change.key, "operation/")) continue;
            const record = try parse(t.Operation, self.a, json);
            try self.put(try self.operationVersionKey(record.id, self.state.revision), OperationVersion{ .record = publicOperation(record), .recorded_at = self.service.context.now_ms, .decision = if (decision) |value| value.resource else null });
        };
    }
    fn recordDecision(self: *Run, operation: t.Operation) !void {
        // Polling and physical cursor movement are not new knowledge decisions.
        // Preserve semantic changes and managed publication/failure transitions.
        if (eql(u8, operation.action, "advance")) {
            var consequential = false;
            for (self.changes.items) |change| if (std.mem.startsWith(u8, change.key, "assertion/") or std.mem.startsWith(u8, change.key, "operation/") or std.mem.startsWith(u8, change.key, "configuration/")) {
                consequential = true;
                break;
            };
            if (!consequential) return;
        }
        const ref = try self.resourceRef(.decision, operation.id, operation.revision);
        const governed_config = (try self.read(t.Config, try configKey(self.a, operation.config_generation))) orelse self.state.config;
        var status: @FieldType(prov.Decision, "status") = switch (operation.phase) {
            .pending, .running => .proposed,
            .failed => .rejected,
            .cancelled => .cancelled,
            else => .applied,
        };
        if (operation.proposal_id) |proposal_id| {
            if (eql(u8, operation.action, "propose_supersession") or eql(u8, operation.action, "review_supersession")) {
                const proposal = (try self.read(t.SupersessionProposal, try key(self.a, "supersession-proposal", proposal_id))) orelse return error.MemoryCorruptState;
                status = if (proposal.verdict == null) .proposed else .applied;
            } else if (eql(u8, operation.action, "propose_relationship") or eql(u8, operation.action, "review_relationship")) {
                const proposal = (try self.read(t.RelationshipProposal, try key(self.a, "relationship-proposal", proposal_id))) orelse return error.MemoryCorruptState;
                status = switch (proposal.state) {
                    .open => .proposed,
                    .accepted => .applied,
                    .rejected => .rejected,
                };
            } else {
                if (try self.identityProposal(proposal_id)) |proposal| {
                    status = switch (proposal.state) {
                        .open => .proposed,
                        .accepted => .applied,
                        .rejected => .rejected,
                        .superseded => .cancelled,
                    };
                }
            }
        }
        const decision: prov.Decision = .{
            .resource = ref,
            .operation = try self.resourceRef(.operation, operation.id, operation.revision),
            .action = operation.action,
            .status = status,
            .initiator = .{ .id = self.service.context.principal, .assurance = .authenticated },
            .executor = .{ .id = if (self.service.context.execution_principal.len > 0) self.service.context.execution_principal else self.service.context.principal, .kind = .service, .assurance = .authenticated },
            .origin = operation.origin,
            .decided_at = self.service.context.now_ms,
            .applied_at = if (status == .applied) self.service.context.now_ms else null,
            .recorded_at = self.service.context.now_ms,
            .configuration = try self.resourceRef(.configuration, try std.fmt.allocPrint(self.a, "{d}", .{operation.config_generation}), operation.config_generation),
            .ontology_version = governed_config.ontology_version,
            .policy_version = governed_config.policy_version,
        };
        try self.put(try key(self.a, "audit", operation.id), decision);
        const changes = try self.a.dupe(Change, self.changes.items);
        for (changes) |change| if (change.json) |json| {
            const resource: ?prov.Resource = if (std.mem.startsWith(u8, change.key, "assertion/")) value: {
                const record = try parse(t.StoredAssertion, self.a, json);
                if (record.revision != operation.revision) continue;
                break :value try self.resourceRef(.assertion, record.assertion.id, record.revision);
            } else if (std.mem.startsWith(u8, change.key, "evidence/")) value: {
                const record = try parse(t.StoredEvidence, self.a, json);
                break :value try self.resourceRef(.evidence, record.evidence.id, record.revision);
            } else if (std.mem.startsWith(u8, change.key, "entity/")) value: {
                const record = try parse(t.EntityRecord, self.a, json);
                break :value try self.resourceRef(.entity, record.entity.id, record.revision);
            } else if (std.mem.startsWith(u8, change.key, "outcome/")) value: {
                const record = try parse(StoredOutcome, self.a, json);
                break :value try self.resourceRef(.outcome, record.outcome.id, record.revision);
            } else null;
            if (resource) |target| {
                if (try self.read(prov.Resource, try self.auditIndex(target)) == null) try self.put(try self.auditIndex(target), ref);
                try self.recordTarget(ref, target);
            }
        };
        for (self.decision_targets.items) |target| try self.recordTarget(ref, target);
        if (eql(u8, operation.action, "create") or eql(u8, operation.action, "configure")) {
            const configuration = try self.resourceRef(.configuration, try std.fmt.allocPrint(self.a, "{d}", .{operation.config_generation}), operation.config_generation);
            if (try self.read(prov.Resource, try self.auditIndex(configuration)) == null) try self.put(try self.auditIndex(configuration), ref);
            try self.recordTarget(ref, configuration);
        }
        if (eql(u8, operation.action, "create")) {
            const target = try self.resourceRef(.table, ref.table, operation.revision);
            try self.put(try self.auditIndex(target), ref);
            try self.recordTarget(ref, target);
        }
    }
    fn explain(self: *Run, id: []const u8, request: prov.ExplainRequest) ![]const u8 {
        self.historical_resource_reads = request.known_at != null or request.revision != null or request.resource != null;
        if (request.max_nodes == 0 or request.max_nodes > 256 or request.max_depth > 16 or request.max_bytes < 1024 or request.max_bytes > 4 * 1024 * 1024) return error.InvalidMemoryRequest;
        const root = request.resource orelse try self.resourceRef(.assertion, id, request.revision orelse 0);
        if (request.resource != null and request.revision != null and request.revision.? != root.revision) return error.InvalidMemoryRequest;
        if (request.resource != null) try root.validate();
        const Pending = struct { resource: prov.Resource, depth: u8 };
        var queue: std.ArrayList(Pending) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        var nodes: std.ArrayList(prov.ExplanationNode) = .empty;
        var truncated = false;
        var missing = false;
        try queue.append(self.a, .{ .resource = root, .depth = 0 });
        var position: usize = 0;
        while (position < queue.items.len and nodes.items.len < request.max_nodes) : (position += 1) {
            const pending = queue.items[position];
            const visit = try hash(self.a, pending.resource);
            if (seen.contains(visit)) continue;
            try seen.put(self.a, visit, {});
            var ref = pending.resource;
            var links: std.ArrayList(prov.Link) = .empty;
            var record = self.explanationRecord(&ref, request, &links) catch |err| switch (err) {
                error.MemoryNotKnownAt => {
                    if (position == 0) return error.MemoryNotFound;
                    continue;
                },
                error.Forbidden, error.MemoryNotFound, error.MemoryForgotten, error.MemoryEntityRequired => {
                    if (position == 0) return err;
                    missing = true;
                    continue;
                },
                else => return err,
            };
            try seen.put(self.a, try hash(self.a, ref), {});
            var public_links: std.ArrayList(prov.ExplainedLink) = .empty;
            for (links.items) |link| {
                if (public_links.items.len >= request.max_nodes) {
                    truncated = true;
                    break;
                }
                var target = link.target;
                var ignored: std.ArrayList(prov.Link) = .empty;
                _ = self.explanationRecord(&target, request, &ignored) catch |err| switch (err) {
                    error.MemoryNotKnownAt => continue,
                    error.MemoryResourceChanged => {
                        missing = true;
                        try public_links.append(self.a, .{ .relation = link.relation, .target = target, .status = .changed });
                        continue;
                    },
                    error.Forbidden, error.MemoryNotFound, error.MemoryForgotten, error.MemoryEntityRequired => {
                        missing = true;
                        try public_links.append(self.a, .{ .relation = link.relation, .target = null, .status = .missing_or_denied });
                        continue;
                    },
                    else => return err,
                };
                const already_seen = seen.contains(try hash(self.a, target));
                const bounded = !already_seen and (pending.depth >= request.max_depth or queue.items.len >= request.max_nodes * 2);
                try public_links.append(self.a, .{ .relation = link.relation, .target = target, .status = if (bounded) .truncated else if (target.external() and (try self.resourceSnapshot(target)).state == .changed) .changed else .available });
                if (bounded) truncated = true else if (!already_seen) try queue.append(self.a, .{ .resource = target, .depth = pending.depth + 1 });
            }
            const payload_truncated = try encodedSize(record) > request.max_bytes / 2;
            if (payload_truncated) {
                record = .null;
                truncated = true;
            }
            const source_truncated = if (ref.external()) source: {
                const snapshot = try self.resourceSnapshot(ref);
                break :source snapshot.state == .current and snapshot.payload_truncated;
            } else false;
            try nodes.append(self.a, .{ .resource = ref, .record = record, .payload_truncated = payload_truncated or source_truncated, .links = public_links.items });
        }
        truncated = truncated or position < queue.items.len or self.explanation_truncated;
        return self.renderExplanation(&nodes, request.max_bytes, truncated, missing);
    }
    // Measure each node once without allocating JSON. Trimming updates an
    // additive wire-size total, then emits the retained graph into one exact
    // buffer. Discarded suffixes never accumulate serialized graph copies.
    fn renderExplanation(self: *Run, nodes: *std.ArrayList(prov.ExplanationNode), max_bytes: usize, was_truncated: bool, missing: bool) ![]const u8 {
        var sizes: [256]usize = undefined;
        std.debug.assert(nodes.items.len <= sizes.len);
        var node_bytes: usize = 0;
        for (nodes.items, 0..) |node, i| {
            try self.service.context.check();
            sizes[i] = try encodedSize(node);
            node_bytes += sizes[i];
        }
        var result: prov.Explanation = .{ .nodes = &.{}, .complete = !was_truncated and !missing and !self.explanation_incomplete_history, .truncated = was_truncated, .missing_evidence = missing, .incomplete_history = self.explanation_incomplete_history };
        var envelope_bytes = try encodedSize(result);
        while (true) {
            try self.service.context.check();
            const total = envelope_bytes + node_bytes + (nodes.items.len -| 1);
            if (total <= max_bytes) {
                result.nodes = nodes.items;
                const output = try self.a.alloc(u8, total);
                var writer: std.Io.Writer = .fixed(output);
                try std.json.Stringify.value(result, .{}, &writer);
                std.debug.assert(writer.end == total);
                return output;
            }
            if (!result.truncated) {
                result.truncated = true;
                result.complete = false;
                envelope_bytes = try encodedSize(result);
                continue;
            }
            if (nodes.items.len > 1) {
                node_bytes -= sizes[nodes.items.len - 1];
                _ = nodes.pop();
                continue;
            }
            if (nodes.items.len == 1) {
                const root = &nodes.items[0];
                if (root.record != .null) {
                    root.record = .null;
                    root.payload_truncated = true;
                } else if (root.links.len > 0) {
                    root.links = &.{};
                } else {
                    _ = nodes.pop();
                    node_bytes = 0;
                    continue;
                }
                node_bytes = try encodedSize(root.*);
                continue;
            }
            return error.InvalidMemoryRequest;
        }
    }
    fn explanationRecord(self: *Run, ref: *prov.Resource, request: prov.ExplainRequest, links: *std.ArrayList(prov.Link)) anyerror!std.json.Value {
        const cache_key = try hash(self.a, ref.*);
        if (self.explanation_records.get(cache_key)) |cached| {
            ref.* = cached.resource;
            try links.appendSlice(self.a, cached.links);
            return cached.record;
        }
        var captured: std.ArrayList(prov.Link) = .empty;
        const record = try self.loadExplanationRecord(ref, request, &captured);
        const cached: ExplainedRecord = .{ .resource = ref.*, .record = record, .links = captured.items };
        try self.explanation_records.put(self.a, cache_key, cached);
        try self.explanation_records.put(self.a, try hash(self.a, ref.*), cached);
        try links.appendSlice(self.a, captured.items);
        return record;
    }
    fn requireKnownAt(self: *Run, ref: prov.Resource, recorded_at: ?i64, request: prov.ExplainRequest) !void {
        const known = request.known_at orelse return;
        const observed = recorded_at orelse historical: {
            if (ref.kind == .decision) {
                if (try self.read(prov.Decision, try key(self.a, "audit", ref.id))) |decision| break :historical decision.recorded_at;
            }
            if (try self.read(prov.Resource, try self.auditIndex(ref))) |decision_ref| {
                if (try self.read(prov.Decision, try key(self.a, "audit", decision_ref.id))) |decision| break :historical decision.recorded_at;
            }
            self.explanation_incomplete_history = true;
            return error.MemoryNotKnownAt;
        };
        if (observed > known) return error.MemoryNotKnownAt;
    }
    const CapturedSource = union(enum) { found: t.StoredEvidence, missing, exhausted };
    fn capturedSource(self: *Run, ref: prov.Resource, request: prov.ExplainRequest) !CapturedSource {
        const prefix = try std.fmt.allocPrint(self.a, "resource-evidence/{s}/", .{try hash(self.a, ref.identity())});
        var after: ?[]const u8 = null;
        while (true) {
            try self.service.context.check();
            if (self.explanation_snapshot_candidates == max_explanation_snapshot_candidates) return .exhausted;
            _ = self.explanation_snapshot_scratch.reset(.retain_capacity);
            const page_a = self.explanation_snapshot_scratch.allocator();
            const snapshots = try self.service.backend.scan(self.service.backend.ptr, page_a, prefix, after, @intCast(@min(64, max_explanation_snapshot_candidates - self.explanation_snapshot_candidates)));
            for (snapshots.rows) |row| {
                try self.service.context.check();
                self.explanation_snapshot_candidates += 1;
                const binding = try parse(struct { evidence_id: []const u8 }, page_a, row.json);
                // Rejected snapshot payloads must not accumulate in the run's
                // row cache. Only the selected immutable capture escapes this page.
                const raw = (try self.service.backend.get(self.service.backend.ptr, page_a, try key(page_a, "evidence", binding.evidence_id))) orelse continue;
                const source = try parse(t.StoredEvidence, page_a, raw.json);
                self.authorize(source.owner, source.evidence.visibility) catch continue;
                const actual = source.evidence.resource orelse return error.MemoryCorruptState;
                if (!ref.sameVersion(actual)) continue;
                if (ref.digest) |expected| {
                    if (actual.digest == null or !std.ascii.eqlIgnoreCase(expected, actual.digest.?)) continue;
                }
                if (request.known_at) |known| if (source.recorded_at > known) continue;
                if (!try self.evidenceRetained(source)) continue;
                return .{ .found = try parse(t.StoredEvidence, self.a, raw.json) };
            }
            after = if (snapshots.next) |cursor| try self.a.dupe(u8, cursor) else return .missing;
        }
    }
    fn appendProvenanceLinks(self: *Run, inputs: GenerationProvenance, links: *std.ArrayList(prov.Link)) !void {
        var seen: ResourceSet = .empty;
        for (links.items) |link| try seen.put(self.a, try hash(self.a, .{ link.relation, link.target }), {});
        for (inputs.evidence_ids) |id| if (try self.evidence(id)) |source| try self.appendProvenanceLink(links, &seen, .supported_by, try self.resourceRef(.evidence, id, source.revision));
        for (inputs.context) |input| try self.appendProvenanceLink(links, &seen, .depends_on, input.resource);
    }
    fn appendProvenanceLink(self: *Run, links: *std.ArrayList(prov.Link), seen: *ResourceSet, relation: prov.Relation, target: prov.Resource) !void {
        const identity = try hash(self.a, .{ relation, target });
        if (seen.contains(identity)) return;
        try seen.put(self.a, identity, {});
        try links.append(self.a, .{ .relation = relation, .target = target });
    }
    fn loadExplanationRecord(self: *Run, ref: *prov.Resource, request: prov.ExplainRequest, links: *std.ArrayList(prov.Link)) anyerror!std.json.Value {
        if (ref.external()) {
            const snapshot = try self.resourceSnapshot(ref.*);
            if (snapshot.state == .denied) return error.Forbidden;
            if (snapshot.state == .missing) return error.MemoryNotFound;
            if (snapshot.state == .current and snapshot.payload_truncated and request.known_at == null) {
                self.explanation_truncated = true;
                self.explanation_incomplete_history = true;
                return parse(std.json.Value, self.a, try self.encode(.{ .snapshot_json = @as(?[]const u8, null), .revision = ref.revision, .digest = ref.digest, .source_state = snapshot.state, .payload_truncated = true }));
            }
            var json = snapshot.json;
            var payload_redacted = false;
            if (snapshot.state == .changed or request.known_at != null) {
                // Look up immutable identity, not mutable routing/display hints.
                const source = switch (try self.capturedSource(ref.*, request)) {
                    .found => |source| source,
                    .exhausted => {
                        self.explanation_truncated = true;
                        self.explanation_incomplete_history = true;
                        return parse(std.json.Value, self.a, try self.encode(.{ .snapshot_json = @as(?[]const u8, null), .revision = ref.revision, .digest = ref.digest, .source_state = snapshot.state, .payload_truncated = true }));
                    },
                    .missing => {
                        if (request.known_at != null) {
                            self.explanation_incomplete_history = true;
                            return error.MemoryNotKnownAt;
                        }
                        return error.MemoryResourceChanged;
                    },
                };
                json = source.evidence.text;
                if (source.materialization) |materialization| payload_redacted = materialization.text_redacted;
            }
            var source_screening = self.state.config.intake_screening;
            source_screening.mode = .redact;
            const sanitized = try screening.json(self.a, json, source_screening);
            payload_redacted = payload_redacted or sanitized.report.redactions > 0;
            json = sanitized.value;
            const DeclaredEnvelope = struct { provenance: ?prov.Envelope = null };
            const declaration: DeclaredEnvelope = std.json.parseFromSliceLeaky(DeclaredEnvelope, self.a, json, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => invalid: {
                    self.explanation_incomplete_history = true;
                    break :invalid .{ .provenance = @as(?prov.Envelope, null) };
                },
            };
            // A resource snapshot is not an attestation of the ordinary row's
            // creation decision. Report absent lineage instead of inventing it.
            if (declaration.provenance == null) self.explanation_incomplete_history = true;
            if (declaration.provenance) |envelope| {
                if (envelope.schema_version != 1) {
                    self.explanation_incomplete_history = true;
                } else {
                    if (envelope.links.len > t.max_items) self.explanation_truncated = true;
                    for (envelope.links[0..@min(envelope.links.len, t.max_items)]) |link| {
                        link.target.validate() catch {
                            self.explanation_incomplete_history = true;
                            continue;
                        };
                        try links.append(self.a, link);
                    }
                }
            }
            return parse(std.json.Value, self.a, try self.encode(.{ .snapshot_json = json, .payload_redacted = payload_redacted, .payload_sha256 = try textDigest(self.a, json), .revision = ref.revision, .digest = ref.digest, .source_state = snapshot.state, .metadata_assurance = "source_declared" }));
        }
        if (ref.storage_name != null) return error.InvalidResourceReference;
        const own_table = try std.fmt.allocPrint(self.a, "memory_{s}", .{self.state.space});
        if (!eql(u8, ref.database, self.state.database) or !eql(u8, ref.namespace, self.state.namespace) or !eql(u8, ref.table, own_table) or ref.table_id != self.state.table_id) return error.Forbidden;
        var value: std.json.Value = undefined;
        switch (ref.kind) {
            .assertion => {
                const record = (if (ref.revision > 0) try self.read(t.StoredAssertion, try historyKey(self.a, ref.id, ref.revision)) else if (request.known_at) |known| try self.asOf(ref.id, known) else try self.assertion(ref.id)) orelse return error.MemoryNotFound;
                try self.authorize(record.owner, record.assertion.visibility);
                try self.requireKnownAt(ref.*, record.recorded_at, request);
                if (!try self.retained(record, 0)) return error.MemoryForgotten;
                ref.revision = record.revision;
                value = try parse(std.json.Value, self.a, try self.encode(try self.publicRecord(record)));
                if (request.valid_at) |at| {
                    try value.object.put(self.a, "applicable", .{ .bool = matchesTime(.{ .valid_at = at, .include_history = true }, record, self.service.context.now_ms) });
                }
                for (record.assertion.evidence) |citation| if (try self.evidence(citation.id)) |source| try links.append(self.a, .{ .relation = .supported_by, .target = try self.resourceRef(.evidence, citation.id, source.revision) });
                for (record.assertion.support_sets) |support| {
                    for (support.evidence) |citation| if (try self.evidence(citation.id)) |source| try links.append(self.a, .{ .relation = .supported_by, .target = try self.resourceRef(.evidence, citation.id, source.revision) });
                    for (support.depends_on) |target| if (try self.asOf(target, record.recorded_at)) |parent| try links.append(self.a, .{ .relation = .depends_on, .target = try self.resourceRef(.assertion, target, parent.revision) });
                }
                inline for (.{ .{ "depends_on", prov.Relation.depends_on }, .{ "supersedes", prov.Relation.supersedes }, .{ "motivates", prov.Relation.motivates }, .{ "rejected_in_favor_of", prov.Relation.rejected_in_favor_of } }) |relationship| {
                    for (@field(record.assertion, relationship[0])) |target| if (try self.asOf(target, record.recorded_at)) |parent| try links.append(self.a, .{ .relation = relationship[1], .target = try self.resourceRef(.assertion, target, parent.revision) });
                }
            },
            .evidence => {
                const record = (try self.evidence(ref.id)) orelse return error.MemoryNotFound;
                try self.authorize(record.owner, record.evidence.visibility);
                if (!try self.evidenceRetained(record) or (ref.revision > 0 and ref.revision != record.revision)) return error.MemoryForgotten;
                ref.revision = record.revision;
                try self.requireKnownAt(ref.*, record.recorded_at, request);
                value = try parse(std.json.Value, self.a, try self.encode(record));
                if (record.evidence.resource) |source| try links.append(self.a, .{ .relation = .supported_by, .target = source });
            },
            .entity => {
                const record = (try self.read(t.EntityRecord, try key(self.a, "entity", ref.id))) orelse return error.MemoryNotFound;
                try self.authorize(record.owner, record.entity.visibility);
                if (!try self.entityRetained(record)) return error.MemoryForgotten;
                if (ref.revision > 0 and ref.revision != record.revision) return error.MemoryNotFound;
                ref.revision = record.revision;
                try self.requireKnownAt(ref.*, record.recorded_at, request);
                value = try parse(std.json.Value, self.a, try self.encode(record));
                for (record.entity.evidence) |citation| if (try self.evidence(citation.id)) |source| try links.append(self.a, .{ .relation = .supported_by, .target = try self.resourceRef(.evidence, citation.id, source.revision) });
            },
            .decision => {
                var record = (try self.read(prov.Decision, try key(self.a, "audit", ref.id))) orelse return error.MemoryNotFound;
                if (ref.revision != record.resource.revision) return error.MemoryNotFound;
                try self.requireKnownAt(ref.*, record.recorded_at, request);
                // Decision metadata may survive payload retention, but access
                // still follows the operation's owner and current table grant.
                if (!self.explanation_decisions.contains(try hash(self.a, ref.*))) try self.authorize(record.initiator.id, .private);
                if (record.origin) |*origin| {
                    for ([_]?prov.Resource{ origin.trace, origin.span }) |source| if (source) |target| try links.append(self.a, .{ .relation = .originated_in, .target = target });
                    origin.trace = null;
                    origin.span = null;
                }
                const target_prefix = try std.fmt.allocPrint(self.a, "audit-target/{s}/", .{try hash(self.a, ref.id)});
                const targets = try self.service.backend.scan(self.service.backend.ptr, self.a, target_prefix, null, request.max_nodes);
                for (targets.rows) |target| try links.append(self.a, .{ .relation = .applied_to, .target = try parse(prov.Resource, self.a, target.json) });
                if (targets.next != null) self.explanation_truncated = true;
                try links.append(self.a, .{ .relation = .governed_by, .target = record.configuration });
                value = try parse(std.json.Value, self.a, try self.encode(record));
                if (eql(u8, record.action, "review_entity") or eql(u8, record.action, "review_relationship")) {
                    if (try self.read(JudgmentArtifact, try key(self.a, "judgment", record.operation.id))) |artifact| {
                        if (artifact.operation.revision == record.operation.revision and try self.judgmentAvailable(artifact, .retained)) {
                            try self.appendProvenanceLinks(artifact.inputs, links);
                            // Minimal admission decisions can be shared with an
                            // output's readers; private review text cannot.
                            if ((self.service.context.admin or eql(u8, record.initiator.id, self.service.context.principal)) and artifact.reason.len > 0) {
                                if (!eql(u8, record.initiator.id, self.service.context.principal)) try self.requireAdmin();
                                try value.object.put(self.a, "recorded_rationale", .{ .string = artifact.reason });
                            }
                        } else self.explanation_incomplete_history = true;
                    } else self.explanation_incomplete_history = true;
                }
                if (eql(u8, record.action, "curate_entity")) {
                    const reason_path = try std.fmt.allocPrint(self.a, "entity-curation/{d:0>20}", .{record.resource.revision});
                    if (try self.read(EntityCurationProvenance, reason_path)) |reason| {
                        self.authorize(reason.owner, .private) catch return value;
                        if (try self.curationProvenanceRetained(reason)) try value.object.put(self.a, "recorded_rationale", .{ .string = reason.reason });
                    }
                }
            },
            .operation => {
                const snapshot = try self.read(OperationVersion, try self.operationVersionKey(ref.id, ref.revision));
                const record = if (snapshot) |version| version.record else publicOperation((try self.read(t.Operation, try key(self.a, "operation", ref.id))) orelse return error.MemoryNotFound);
                try self.authorize(record.owner, .private);
                if (snapshot == null and ref.revision != record.revision) return error.MemoryNotFound;
                try self.requireKnownAt(ref.*, if (snapshot) |version| version.recorded_at else null, request);
                if (snapshot == null) self.explanation_incomplete_history = true;
                value = try parse(std.json.Value, self.a, try self.encode(record));
                if (try self.read(JudgmentArtifact, try key(self.a, "judgment", ref.id))) |artifact| {
                    if (artifact.operation.revision == ref.revision and try self.judgmentAvailable(artifact, .retained)) {
                        try self.appendProvenanceLinks(artifact.inputs, links);
                        if (artifact.reason.len > 0) try value.object.put(self.a, "recorded_rationale", .{ .string = artifact.reason });
                    } else self.explanation_incomplete_history = true;
                } else if (eql(u8, record.action, "review_relationship") or eql(u8, record.action, "review_entity")) self.explanation_incomplete_history = true;
                if (self.service.context.admin) try self.requireAdmin();
                if (self.service.context.admin) if (try self.read(ProposalAttempt, try key(self.a, "proposal-attempt", record.id))) |attempt| {
                    if (attempt.inputs.operation) |operation| if (operation.revision == ref.revision and try self.proposalInputsRetained(attempt.inputs)) {
                        var projected = attempt;
                        projected.inputs = try self.publicProposalBinding(attempt.inputs);
                        try value.object.put(self.a, "entity_proposal", try parse(std.json.Value, self.a, try self.encode(projected)));
                        if (attempt.previous) |previous| try links.append(self.a, .{ .relation = .supersedes, .target = previous });
                        for (attempt.inputs.evidence) |evidence_ref| if (try self.evidence(evidence_ref.id)) |source| try links.append(self.a, .{ .relation = .supported_by, .target = try self.resourceRef(.evidence, source.evidence.id, source.revision) });
                        for (attempt.inputs.entities) |entity_ref| try links.append(self.a, .{ .relation = .depends_on, .target = try self.resourceRef(.entity, entity_ref.id, entity_ref.revision) });
                        for (attempt.inputs.views orelse &.{}) |view| for (view.dependencies) |dependency| try links.append(self.a, .{ .relation = .depends_on, .target = dependency.resource });
                    };
                };
                const decision_ref = if (snapshot) |version| version.decision else if (try self.read(prov.Decision, try key(self.a, "audit", record.id))) |decision| decision.resource else null;
                if (decision_ref) |decision| {
                    try self.explanation_decisions.put(self.a, try hash(self.a, decision), {});
                    try links.append(self.a, .{ .relation = .applied_to, .target = decision });
                }
            },
            .configuration => {
                if ((std.fmt.parseInt(u64, ref.id, 10) catch return error.InvalidResourceReference) != ref.revision) return error.InvalidResourceReference;
                const record = (try self.read(t.Config, try configKey(self.a, ref.revision))) orelse return error.MemoryNotFound;
                try self.requireKnownAt(ref.*, null, request);
                value = try parse(std.json.Value, self.a, try self.encode(record));
            },
            .table => {
                if (!eql(u8, ref.id, own_table)) return error.InvalidResourceReference;
                try self.requireKnownAt(ref.*, self.state.created_at, request);
                value = try parse(std.json.Value, self.a, try self.encode(.{ .owner = self.state.owner, .created_at = self.state.created_at, .space = self.state.space, .revision = ref.revision, .serving_revision = self.state.revision }));
            },
            .outcome => {
                const record = (try self.read(StoredOutcome, try key(self.a, "outcome", ref.id))) orelse return error.MemoryNotFound;
                try self.authorize(record.owner, .private);
                if (!try self.outcomeRetained(record)) return error.MemoryForgotten;
                if (ref.revision > 0 and ref.revision != record.revision) return error.MemoryNotFound;
                ref.revision = record.revision;
                try self.requireKnownAt(ref.*, record.recorded_at, request);
                value = try parse(std.json.Value, self.a, try self.encode(record));
                for (record.outcome.used_memory_ids) |used| if (try self.asOf(used, record.recorded_at)) |parent| try links.append(self.a, .{ .relation = .used, .target = try self.resourceRef(.assertion, used, parent.revision) });
            },
            else => return error.InvalidResourceReference,
        }
        if (ref.kind == .assertion or ref.kind == .entity) {
            if (try self.read(GenerationProvenance, try self.generationKey(@tagName(ref.kind), ref.id))) |inputs| try self.appendProvenanceLinks(inputs, links);
        }
        if (ref.kind == .entity or ref.kind == .table or ref.kind == .configuration) {
            const prefix = try std.fmt.allocPrint(self.a, "audit-event/{s}/{s}/", .{ @tagName(ref.kind), try hash(self.a, ref.id) });
            const events = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, null, request.max_nodes);
            for (events.rows) |event| {
                const decision = try parse(prov.Resource, self.a, event.json);
                self.requireKnownAt(decision, null, request) catch |err| {
                    if (err == error.MemoryNotKnownAt) continue;
                    return err;
                };
                try self.explanation_decisions.put(self.a, try hash(self.a, decision), {});
                try links.append(self.a, .{ .relation = .generated_by, .target = decision });
            }
            if (events.next != null) self.explanation_truncated = true;
        }
        if (try self.read(prov.Resource, try self.auditIndex(ref.*))) |decision| if (ref.kind != .decision) {
            try self.explanation_decisions.put(self.a, try hash(self.a, decision), {});
            try links.append(self.a, .{ .relation = .generated_by, .target = decision });
        };
        return value;
    }
    fn encodeCursor(self: *Run, prefix: []const u8, position: ?[]const u8) !?[]const u8 {
        const value = position orelse return null;
        return try std.fmt.allocPrint(self.a, "{d}:{s}:{s}", .{ self.state.revision, try hash(self.a, .{ self.state.space, self.service.context.branch, self.service.context.principal, prefix }), value });
    }
    fn decodeCursor(self: *Run, prefix: []const u8, cursor_value: ?[]const u8) !?[]const u8 {
        const value = cursor_value orelse return null;
        var parts = std.mem.splitScalar(u8, value, ':');
        const revision = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidMemoryCursor, 10) catch return error.InvalidMemoryCursor;
        const scope = parts.next() orelse return error.InvalidMemoryCursor;
        const path = parts.next() orelse return error.InvalidMemoryCursor;
        if (parts.next() != null or !std.mem.startsWith(u8, path, prefix)) return error.InvalidMemoryCursor;
        if (revision != self.state.revision) return error.MemoryCursorStale;
        if (!eql(u8, scope, try hash(self.a, .{ self.state.space, self.service.context.branch, self.service.context.principal, prefix }))) return error.InvalidMemoryCursor;
        return path;
    }
    fn history(self: *Run, id: []const u8, request: anytype) ![]const u8 {
        const record = (try self.assertion(id)) orelse return error.MemoryNotFound;
        try self.authorize(record.owner, record.assertion.visibility);
        if (!try self.retained(record, 0)) return error.MemoryNotFound;
        if (request.limit == 0 or request.limit > t.max_items) return error.InvalidMemoryRequest;
        const prefix = try std.fmt.allocPrint(self.a, "history/{s}/", .{try hash(self.a, id)});
        const page = try self.recordPage(prefix, try self.decodeCursor(prefix, request.cursor), request.limit);
        var records: std.ArrayList(t.StoredAssertion) = .empty;
        for (page.rows) |row| {
            const value = (try self.read(t.StoredAssertion, row.key)) orelse continue;
            try self.authorize(value.owner, value.assertion.visibility);
            try records.append(self.a, try self.publicRecord(value));
        }
        return self.encode(.{ .items = records.items, .next_cursor = try self.encodeCursor(prefix, page.next), .complete = page.next == null, .revision = self.state.revision });
    }
    /// Snapshot membership is an ordered native index. Never discover branch
    /// records by scanning unrelated primary rows and filtering afterwards.
    fn recordPage(self: *Run, prefix: []const u8, after: ?[]const u8, limit: u32) !Page {
        if (self.lineage_head) |head| if (head.complete and !self.lineage_raw and (std.mem.startsWith(u8, prefix, "assertion/") or std.mem.startsWith(u8, prefix, "history/") or std.mem.startsWith(u8, prefix, "evidence/") or std.mem.startsWith(u8, prefix, "entity/"))) {
            const page = try self.persistentIndex().page(head.root, prefix, try std.mem.concat(self.a, u8, &.{ prefix, "~" }), after, @min(limit, 256));
            const paths = try self.a.alloc([]const u8, page.items.len);
            for (page.items, paths) |item, *path| path.* = item.value;
            try self.prefetch(paths);
            var rows: std.ArrayList(Row) = .empty;
            for (page.items) |item| {
                const json = if (self.change_positions.get(item.value)) |position| self.changes.items[position].json else if (self.backend_rows.getEntry(item.value)) |entry| entry.value_ptr.* else return error.MemoryCorruptState;
                if (json) |value| try rows.append(self.a, .{ .key = item.key, .json = value });
            }
            return .{ .rows = rows.items, .next = page.next };
        };
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, after, limit);
        for (page.rows) |row| try self.backend_rows.put(self.a, row.key, row.json);
        return page;
    }
    fn enumerate(self: *Run, request: t.Recall) ![]const u8 {
        self.historical_resource_reads = request.known_at != null;
        if (request.limit == 0 or request.limit > t.max_items or request.query.len != 0 or request.embedding != null) return error.InvalidMemoryRequest;
        var items: std.ArrayList(t.StoredAssertion) = .empty;
        var after = try self.decodeCursor("assertion/", request.cursor);
        var visited: usize = 0;
        var more = true;
        while (items.items.len < request.limit and visited < 4096) {
            try self.service.context.check();
            const page = try self.recordPage("assertion/", after, @intCast(@min(128, 4096 - visited)));
            more = page.next != null;
            for (page.rows, 0..) |row, index| {
                after = row.key;
                visited += 1;
                more = index + 1 < page.rows.len or page.next != null;
                var record = (try self.read(t.StoredAssertion, row.key)) orelse continue;
                if (request.known_at) |known| record = (try self.asOf(record.assertion.id, known)) orelse continue;
                self.authorize(record.owner, record.assertion.visibility) catch continue;
                if (!try self.evidenceVisibleTime(record, request.known_at, request.valid_at orelse if (!request.include_history) self.service.context.now_ms else null, 0)) continue;
                const claim = record.assertion;
                if (!matchesTime(request, record, self.service.context.now_ms)) continue;
                if (!request.include_provisional and claim.stability == .provisional) continue;
                const canonical_subject = (try self.canonical(claim.subject)) orelse continue;
                if (self.objectTyped(claim.predicate) and try self.canonical(claim.value) == null) continue;
                if (request.subject) |v| {
                    const wanted = (try self.canonical(v)) orelse continue;
                    if (!eql(u8, wanted, canonical_subject)) continue;
                }
                if (request.predicate) |v| if (!eql(u8, v, claim.predicate)) continue;
                if (request.scope) |v| if (!eql(u8, v, claim.scope)) continue;
                if (request.kind) |v| if (claim.kind != v) continue;
                if (request.environment) |v| if (claim.environment != null and !eql(u8, v, claim.environment.?)) continue;
                if (request.task) |v| if (claim.task != null and !eql(u8, v, claim.task.?)) continue;
                const projected = try self.publicRecord(record);
                if (!relationsMatch(request, projected.assertion)) continue;
                try items.append(self.a, projected);
                if (items.items.len == request.limit) break;
            }
            if (!more) break;
        }
        return self.encode(.{ .items = items.items, .next_cursor = if (more) try self.encodeCursor("assertion/", after) else null, .complete = !more, .revision = self.state.revision });
    }
    const RecallGraph = struct {
        const Node = struct { kind: enum { entity, assertion, episode }, id: []const u8, depth: u8, seed: f64 = 0, hit: ?t.RecallHit = null };
        a: A,
        options: t.GraphRecall,
        nodes: std.ArrayList(Node) = .empty,
        edges: std.ArrayList(graph_recall.Edge) = .empty,
        index: std.StringHashMapUnmanaged(usize) = .empty,
        edge_index: std.AutoHashMapUnmanaged(u64, void) = .empty,
        complete: bool = true,
        omitted: usize = 0,
        work: usize = 0,
        fn add(self: *RecallGraph, node: Node) !?usize {
            const identity = try std.fmt.allocPrint(self.a, "{s}/{s}", .{ @tagName(node.kind), node.id });
            if (self.index.get(identity)) |i| {
                self.nodes.items[i].seed = @max(self.nodes.items[i].seed, node.seed);
                return i;
            }
            if (self.nodes.items.len >= self.options.max_nodes) {
                self.complete = false;
                self.omitted += 1;
                return null;
            }
            const i = self.nodes.items.len;
            try self.nodes.append(self.a, node);
            try self.index.put(self.a, identity, i);
            return i;
        }
        fn connect(self: *RecallGraph, from: usize, to: usize, weight: f64) !void {
            if (from == to) return;
            const identity = (@as(u64, @intCast(@min(from, to))) << 32) | @as(u64, @intCast(@max(from, to)));
            if (self.edge_index.contains(identity)) return;
            if (self.edges.items.len >= self.options.max_edges) {
                self.complete = false;
                self.omitted += 1;
                return;
            }
            try self.edge_index.put(self.a, identity, {});
            try self.edges.append(self.a, .{ .from = from, .to = to, .weight = weight });
        }
    };
    fn graphEntity(self: *Run, graph: *RecallGraph, id: []const u8, depth: u8, seed: f64, known: ?i64) !?usize {
        const record = (try self.visibleCanonicalEntity(id, known)) orelse return null;
        return graph.add(.{ .kind = .entity, .id = record.entity.id, .depth = depth, .seed = seed });
    }
    fn graphRecall(self: *Run, request: t.Recall, vector: ?[]const f32, hits: *std.ArrayList(t.RecallHit)) !struct { status: t.GraphRecallStatus, entities: []const t.EntityView, episodes: []const t.Episode } {
        const options = request.graph.?;
        try graph_recall.validate(options);
        var graph: RecallGraph = .{ .a = self.a, .options = options };
        for (options.entity_seeds) |id| _ = try self.graphEntity(&graph, id, 0, 1, request.known_at);
        // Flat retrieval seeds are deterministic and bounded. Explicit seeds
        // bypass the global assertion scan and use indexed graph adjacency.
        if (options.entity_seeds.len == 0 and hits.items.len > 16) {
            graph.omitted += hits.items.len - 16;
            graph.complete = false;
        }
        if (options.entity_seeds.len == 0) for (hits.items[0..@min(hits.items.len, 16)]) |hit| {
            const node = (try graph.add(.{ .kind = .assertion, .id = hit.memory.assertion.id, .depth = 0, .seed = @max(0.001, hit.score), .hit = hit })) orelse break;
            _ = node;
        };
        var current: usize = 0;
        while (current < graph.nodes.items.len) : (current += 1) {
            try self.service.context.check();
            const node = graph.nodes.items[current];
            if (node.depth >= options.max_hops) continue;
            if (graph.work >= 4096) {
                graph.complete = false;
                break;
            }
            switch (node.kind) {
                .entity => {
                    const related = try self.relatedAssertions(node.id, @min(128, 4096 - graph.work));
                    graph.work += related.scanned;
                    graph.complete = graph.complete and related.complete;
                    for (related.items) |record| {
                        const hit = (try self.recallHit(record, request, vector, false)) orelse continue;
                        const target = (try graph.add(.{ .kind = .assertion, .id = hit.memory.assertion.id, .depth = node.depth + 1, .seed = if (request.query.len > 0) @max(0, hit.score) else 0, .hit = hit })) orelse continue;
                        const weight = if (record.assertion.stability == .provisional) options.provisional_weight else 1.0;
                        try graph.connect(current, target, weight);
                        // Typed entity edges are one conceptual hop, even
                        // though the assertion node retains their provenance.
                        if (self.objectTyped(record.assertion.predicate)) {
                            for ([_][]const u8{ record.assertion.subject, record.assertion.value }) |id| if (try self.graphEntity(&graph, id, node.depth + 1, 0, request.known_at)) |entity_node| {
                                try graph.connect(current, entity_node, weight * options.relation_weight);
                                try graph.connect(target, entity_node, weight);
                            };
                        }
                    }
                    if (options.include_episodes) for (try self.identityMembers(node.id)) |name| {
                        if (graph.work + 1 >= 4096) {
                            graph.complete = false;
                            break;
                        }
                        const prefix = try std.fmt.allocPrint(self.a, "dependency/mention-entity/{s}/", .{try hash(self.a, name)});
                        const page = try self.scanOverlay(prefix, null, @intCast(@min(64, 4095 - graph.work)));
                        graph.work += page.rows.len + 1;
                        graph.complete = graph.complete and page.next == null;
                        for (page.rows) |row| {
                            const binding = try parse(struct { child: []const u8, branch: []const u8 = "" }, self.a, row.json);
                            const mention_record = (try self.read(t.StoredMention, try key(self.a, "mention", binding.child))) orelse continue;
                            if (!try self.mentionVisible(mention_record, request.known_at)) continue;
                            if (options.include_episodes) for (mention_record.mention.evidence) |ref| {
                                const source = (try self.evidence(ref.id)) orelse continue;
                                if (source.evidence.thread_id) |thread| {
                                    if (source.evidence.ordinal == null) continue;
                                    if (try graph.add(.{ .kind = .episode, .id = thread, .depth = node.depth + 1 })) |episode_node| try graph.connect(current, episode_node, 1);
                                }
                            };
                        }
                    };
                },
                .assertion => {
                    const hit = node.hit.?;
                    const weight = if (hit.memory.assertion.stability == .provisional) options.provisional_weight else 1.0;
                    if (try self.graphEntity(&graph, hit.memory.assertion.subject, node.depth + 1, 0, request.known_at)) |target| try graph.connect(current, target, weight);
                    if (self.objectTyped(hit.memory.assertion.predicate)) if (try self.graphEntity(&graph, hit.memory.assertion.value, node.depth + 1, 0, request.known_at)) |target| try graph.connect(current, target, weight * options.relation_weight);
                    if (options.include_episodes) for (hit.citations) |ref| {
                        const source = (try self.evidence(ref.id)) orelse continue;
                        if (source.evidence.thread_id) |thread| {
                            if (source.evidence.ordinal == null) continue;
                            if (try graph.add(.{ .kind = .episode, .id = thread, .depth = node.depth + 1 })) |target| try graph.connect(current, target, weight);
                        }
                    };
                },
                .episode => {}, // Thread results do not fan out into unrelated turns.
            }
        }
        const seeds = try self.a.alloc(f64, graph.nodes.items.len);
        for (graph.nodes.items, seeds) |node, *seed| seed.* = node.seed;
        const scores = try graph_recall.rank(self.a, seeds, graph.edges.items, options);
        const order = try self.a.alloc(usize, scores.len);
        for (order, 0..) |*index, i| index.* = i;
        std.mem.sort(usize, order, scores, struct {
            fn less(ranks: []const f64, lhs: usize, rhs: usize) bool {
                return if (ranks[lhs] == ranks[rhs]) lhs < rhs else ranks[lhs] > ranks[rhs];
            }
        }.less);
        var entities: std.ArrayList(t.EntityView) = .empty;
        var episodes_out: std.ArrayList(t.Episode) = .empty;
        // In graph mode the ranked, authorized induced subgraph is the result
        // domain. Lexical matches seed it; they do not crowd out linked facts.
        hits.clearRetainingCapacity();
        for (order) |i| {
            if (scores[i] <= 0) {
                graph.omitted += 1;
                graph.complete = false;
                continue;
            }
            switch (graph.nodes.items[i].kind) {
                .assertion => {
                    var hit = graph.nodes.items[i].hit.?;
                    hit.score = scores[i];
                    try hits.append(self.a, hit);
                },
                .entity => if (entities.items.len < 8) {
                    const view = try self.entityViewFiltered(graph.nodes.items[i].id, .{ .limit = 4, .known_at = request.known_at, .valid_at = request.valid_at }, request);
                    graph.complete = graph.complete and view.complete;
                    try entities.append(self.a, view);
                } else {
                    graph.omitted += 1;
                    graph.complete = false;
                },
                .episode => if (episodes_out.items.len < 8) {
                    const item = self.episode(graph.nodes.items[i].id, .{ .limit = 4, .known_at = request.known_at }) catch |err| switch (err) {
                        error.MemoryNotFound => continue,
                        else => return err,
                    };
                    if (item.turns.len > 0) try episodes_out.append(self.a, item);
                    graph.complete = graph.complete and item.complete;
                } else {
                    graph.omitted += 1;
                    graph.complete = false;
                },
            }
        }
        return .{ .status = .{ .nodes = graph.nodes.items.len, .edges = graph.edges.items.len, .iterations = options.iterations, .complete = graph.complete, .omitted = graph.omitted }, .entities = entities.items, .episodes = episodes_out.items };
    }
    fn recallHit(self: *Run, original: t.StoredAssertion, request: t.Recall, vector: ?[]const f32, require_match: bool) !?t.RecallHit {
        var record = if (self.lineage_head != null and !self.lineage_raw) (try self.assertion(original.assertion.id)) orelse return null else original;
        self.authorize(record.owner, record.assertion.visibility) catch return null;
        if (request.known_at) |known| record = (try self.asOf(record.assertion.id, known)) orelse return null;
        const valid = request.valid_at orelse if (!request.include_history) self.service.context.now_ms else null;
        const claim = record.assertion;
        const historical = record.lifecycle == .superseded or record.lifecycle == .retracted or (claim.valid_to != null and claim.valid_to.? <= self.service.context.now_ms);
        if (!matchesTime(request, record, self.service.context.now_ms)) return null;
        if (!request.include_provisional and claim.stability == .provisional) return null;
        if (request.predicate) |value| if (!eql(u8, value, claim.predicate)) return null;
        if (request.scope) |value| if (!eql(u8, value, claim.scope)) return null;
        if (request.environment) |value| if (claim.environment != null and !eql(u8, value, claim.environment.?)) return null;
        if (request.task) |value| if (claim.task != null and !eql(u8, value, claim.task.?)) return null;
        if (request.kind) |value| if (value != claim.kind) return null;
        if (!try self.evidenceVisibleTime(record, request.known_at, valid, 0)) return null;
        const canonical_subject = (try self.canonical(claim.subject)) orelse return null;
        if (self.objectTyped(claim.predicate) and try self.canonical(claim.value) == null) return null;
        if (request.subject) |value| {
            const wanted = (try self.canonical(value)) orelse return null;
            if (!eql(u8, wanted, canonical_subject)) return null;
        }
        if (request.query.len > 0 and (self.lexical_query == null or !eql(u8, self.lexical_query.?.input, request.query))) {
            if (self.lexical_query) |*query| query.deinit();
            self.lexical_query = null;
            self.lexical_query = try lexical.PreparedQuery.init(self.lexical_allocator, request.query);
        }
        var score: f64 = if (request.query.len == 0) 1 else self.lexical_query.?.coverage(claim.statement);
        const compatible_embedding = if (vector != null) try self.vectorFor(self.state.config, claim) else null;
        if (vector) |query_vector| if (compatible_embedding) |embedding| {
            score += try cosine(query_vector, embedding);
        };
        if (require_match and request.query.len > 0 and score <= 0) return null;
        if (claim.stability == .provisional) score *= 0.5;
        const support = try self.visibleSupport(claim, request.known_at, valid);
        record.assertion.evidence = support.evidence;
        record.assertion.depends_on = support.depends_on;
        record.assertion.support_sets = &.{};
        record.assertion.embedding = null;
        record = try self.publicRecord(record);
        if (!relationsMatch(request, record.assertion)) return null;
        return .{ .memory = record, .score = score, .historical = historical, .citations = support.evidence };
    }
    fn recallWindow(self: *Run, request: t.Recall, vector: ?[]const f32) !recall_windows.Window {
        var text: std.ArrayList([]const u8) = .empty;
        var dense: std.ArrayList([]const u8) = .empty;
        if (self.service.context.admin) try self.requireAdmin();
        const scopes: []const []const u8 = if (self.service.context.admin) &.{"all"} else &.{ "team", try std.fmt.allocPrint(self.a, "private/{s}", .{self.service.context.principal}) };
        for (scopes) |scope| {
            if (try self.read(bool, try self.recallAudienceKey(self.state.config, scope)) == null) continue;
            try text.append(self.a, try self.recallIndexName(self.state.config, scope, false));
            if (vector != null) try dense.append(self.a, try self.recallIndexName(self.state.config, scope, true));
        }
        if (text.items.len == 0) return .{ .keys = &.{}, .vector = vector, .truncated = false };
        const page = self.service.backend.search_assertions.?(self.service.backend.ptr, self.a, .{ .text_indexes = text.items, .vector_indexes = dense.items, .query = request.query, .filters = .{ .predicate = request.predicate, .scope = request.scope, .kind = if (request.kind) |kind| @tagName(kind) else null, .environment = request.environment, .task = request.task }, .embedding = vector, .limit = recall_windows.max_candidates }) catch |err| switch (err) {
            error.IndexNotFound, error.IndexRebuilding => return error.MemoryCapabilityUnavailable,
            else => return err,
        };
        if (page.keys.len > recall_windows.max_candidates) return error.MemoryCorruptState;
        return .{ .keys = page.keys, .vector = vector, .truncated = page.more };
    }
    fn indexedRecallPage(self: *Run, keys: []const []const u8) ![]const t.StoredAssertion {
        if (keys.len > recall_windows.page_size) return error.MemoryCorruptState;
        try self.prefetch(keys);
        var projections: std.ArrayList(t.RecallProjection) = .empty;
        var paths: std.ArrayList([]const u8) = .empty;
        for (keys) |path| {
            if (!std.mem.startsWith(u8, path, "assertion-recall/")) return error.MemoryCorruptState;
            const projection = (try self.read(t.RecallProjection, path)) orelse continue;
            if (projection.generation != self.state.config.generation) continue;
            if (!eql(u8, projection.assertion_key, try key(self.a, "assertion", projection.assertion_id))) return error.MemoryCorruptState;
            try projections.append(self.a, projection);
            try paths.append(self.a, projection.assertion_key);
        }
        try self.prefetch(paths.items);
        var records: std.ArrayList(t.StoredAssertion) = .empty;
        var seen: ResourceSet = .empty;
        for (projections.items) |projection| {
            const record = (try self.assertion(projection.assertion_id)) orelse continue;
            if (seen.contains(record.assertion.id) or !eql(u8, record.owner, projection.owner) or record.assertion.visibility != projection.visibility or !eql(u8, projection.statement_hash, try hash(self.a, record.assertion.statement))) continue;
            try seen.put(self.a, record.assertion.id, {});
            try records.append(self.a, record);
        }
        return records.items;
    }
    fn recall(self: *Run, request: t.Recall) ![]const u8 {
        self.historical_resource_reads = request.known_at != null;
        if (request.limit == 0 or request.limit > t.max_items or request.token_budget == 0 or request.token_budget > self.state.config.max_recall_tokens) return error.InvalidMemoryRequest;
        const Hit = t.RecallHit;
        var hits: std.ArrayList(Hit) = .empty;
        var after: ?[]const u8 = null;
        var scanned: usize = 0;
        var vector = request.embedding;
        if (vector) |supplied| {
            if (!self.state.config.features.semantic_recall) return error.MemoryCapabilityUnavailable;
            const model = self.state.config.models.embedder orelse return error.MemoryModelRequired;
            if (request.embedding_model == null or !valueEqual(t.Model, request.embedding_model.?, model) or supplied.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
            for (supplied) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        }
        var more = false;
        if (request.graph) |options| try graph_recall.validate(options);
        const explicit_seeds = if (request.graph) |options| options.entity_seeds.len > 0 else false;
        if (explicit_seeds and request.cursor != null) return error.InvalidMemoryRequest;
        var scoped_request = request;
        scoped_request.cursor = null;
        const indexed_scope = try std.fmt.allocPrint(self.a, "recall/v2/{s}/", .{try hash(self.a, .{ scoped_request, self.state.table_id, self.state.database, self.state.namespace, self.state.space, self.state.created_at, self.service.context.branch, self.state.revision, self.state.config.generation, self.service.context.principal })});
        const legacy_cursor = if (request.cursor) |cursor| blk: {
            const colon = std.mem.lastIndexOfScalar(u8, cursor, ':') orelse return error.InvalidMemoryCursor;
            break :blk std.mem.startsWith(u8, cursor[colon + 1 ..], "assertion/");
        } else false;
        const index = if (self.service.backend.search_assertions != null) try self.read(t.RecallIndex, "assertion-recall-index") else null;
        const indexed = !explicit_seeds and !legacy_cursor and (request.query.len > 0 or vector != null) and index != null and index.?.version == t.recall_index_version and index.?.complete and index.?.generation == self.state.config.generation and (try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion-recall-work/", null, 1)).rows.len == 0;
        if (indexed) {
            const now = self.service.context.currentTime();
            var offset: usize = 0;
            var expected: ?[32]u8 = null;
            var expires = now +| recall_windows.ttl_ms;
            if (try self.decodeCursor(indexed_scope, request.cursor)) |position| {
                var fields = std.mem.splitScalar(u8, position[indexed_scope.len..], '/');
                const encoded = fields.next() orelse return error.InvalidMemoryCursor;
                if (encoded.len != 64) return error.InvalidMemoryCursor;
                var digest: [32]u8 = undefined;
                _ = std.fmt.hexToBytes(&digest, encoded) catch return error.InvalidMemoryCursor;
                expected = digest;
                offset = std.fmt.parseInt(usize, fields.next() orelse return error.InvalidMemoryCursor, 10) catch return error.InvalidMemoryCursor;
                expires = std.fmt.parseInt(i64, fields.next() orelse return error.InvalidMemoryCursor, 10) catch return error.InvalidMemoryCursor;
                if (fields.next() != null or offset == 0 or offset >= recall_windows.max_candidates or offset % recall_windows.page_size != 0) return error.InvalidMemoryCursor;
                if (expires <= now or expires > now +| recall_windows.ttl_ms) return error.MemoryCursorStale;
            }
            var cached: ?recall_windows.Window = if (expected) |digest| try recall_windows.get(self.a, digest, now) else null;
            if (cached == null) {
                if (self.state.config.features.semantic_recall and vector == null and request.query.len > 0) vector = try self.embed(self.state.config.models.embedder.?, request.query, .empty);
                cached = try self.recallWindow(request, vector);
                const digest = try recall_windows.fingerprint(self.a, indexed_scope, cached.?);
                if (expected) |prior| if (!eql(u8, &prior, &digest)) return error.MemoryCursorStale;
                expected = digest;
                try recall_windows.put(digest, now, cached.?);
            }
            const window = cached.?;
            vector = window.vector;
            if (offset > window.keys.len) return error.InvalidMemoryCursor;
            const end = @min(window.keys.len, offset + recall_windows.page_size);
            const records = try self.indexedRecallPage(window.keys[offset..end]);
            scanned = records.len;
            for (records) |record| if (try self.recallHit(record, request, vector, true)) |hit| try hits.append(self.a, hit);
            more = end < window.keys.len or window.truncated;
            after = if (end < window.keys.len) try std.fmt.allocPrint(self.a, "{s}{s}/{d}/{d}", .{ indexed_scope, std.fmt.bytesToHex(expected.?, .lower), end, expires }) else null;
        } else {
            if (self.state.config.features.semantic_recall and vector == null and request.query.len > 0) vector = try self.embed(self.state.config.models.embedder.?, request.query, .empty);
            if (!explicit_seeds) {
                after = try self.decodeCursor("assertion/", request.cursor);
                while (!explicit_seeds) {
                    try self.service.context.check();
                    const page = try self.recordPage("assertion/", after, 128);
                    for (page.rows) |row| {
                        scanned += 1;
                        const record = try parse(t.StoredAssertion, self.a, row.json);
                        if (try self.recallHit(record, request, vector, true)) |hit| try hits.append(self.a, hit);
                    }
                    after = page.next;
                    if (after == null) break;
                    if (scanned >= 4096) {
                        more = true;
                        break;
                    }
                }
            }
        }
        std.mem.sort(Hit, hits.items, {}, struct {
            fn less(_: void, left: Hit, right: Hit) bool {
                return if (left.score == right.score) std.mem.lessThan(u8, left.memory.assertion.id, right.memory.assertion.id) else left.score > right.score;
            }
        }.less);
        const graph_result = if (request.graph != null) try self.graphRecall(request, vector, &hits) else null;
        var selected: std.ArrayList(Hit) = .empty;
        var bytes: usize = 0;
        var omitted: usize = 0;
        for (hits.items) |hit| {
            var duplicate = false;
            if (!request.include_history and request.valid_at == null and request.known_at == null) for (selected.items) |present| {
                const left = hit.memory.assertion;
                const right = present.memory.assertion;
                if (try self.sameCanonicalGroup(left, right) and try self.sameCanonicalValue(left, right) and left.kind == right.kind and left.role == right.role and left.modality == right.modality and left.stability == right.stability and hit.memory.lifecycle == present.memory.lifecycle and (left.visibility == .team or eql(u8, hit.memory.owner, present.memory.owner))) {
                    duplicate = true;
                    break;
                }
            };
            if (duplicate) {
                omitted += 1;
                continue;
            }
            // UTF-8 bytes are a conservative token upper bound. Include the
            // complete rendered object, including citations and metadata.
            const size = (try self.encode(hit)).len;
            if (selected.items.len >= request.limit or bytes + size > request.token_budget) {
                omitted += 1;
                continue;
            }
            bytes += size;
            try selected.append(self.a, hit);
        }
        var result: t.RecallResult = .{ .items = selected.items, .next_cursor = try self.encodeCursor(if (indexed) indexed_scope else "assertion/", after), .complete = !more and omitted == 0, .truncated = more or omitted > 0, .scanned = scanned, .omitted = omitted, .token_upper_bound = 0, .revision = self.state.revision, .config_generation = self.state.config.generation, .retrieval = if (vector != null) .lexical_vector else .lexical };
        if (graph_result) |result_graph| {
            result.retrieval = .graph;
            result.graph = result_graph.status;
            result.entities = result_graph.entities;
            result.episodes = result_graph.episodes;
            result.complete = result.complete and result_graph.status.complete;
            result.truncated = result.truncated or !result_graph.status.complete;
        }
        while (true) {
            // Bound the complete rendered response, including framing,
            // readiness, citations and the bound's own decimal digits.
            const rendered = try self.encode(result);
            if (rendered.len == result.token_upper_bound) {
                if (rendered.len <= request.token_budget) return rendered;
                if (result.episodes.len > 0) {
                    result.episodes = result.episodes[0 .. result.episodes.len - 1];
                } else if (result.entities.len > 0) {
                    result.entities = result.entities[0 .. result.entities.len - 1];
                } else if (selected.items.len > 0) {
                    _ = selected.pop();
                } else return error.InvalidMemoryRequest;
                result.items = selected.items;
                result.omitted += 1;
                result.complete = false;
                result.truncated = true;
            }
            result.token_upper_bound = rendered.len;
        }
    }
    fn entityList(self: *Run, request: anytype) ![]const u8 {
        if (request.limit == 0 or request.limit > t.max_items) return error.InvalidMemoryRequest;
        const page = try self.recordPage("entity/", try self.decodeCursor("entity/", request.cursor), request.limit);
        var values: std.ArrayList(std.json.Value) = .empty;
        for (page.rows) |row| {
            const stored = (try self.read(t.EntityRecord, row.key)) orelse continue;
            self.authorize(stored.owner, stored.entity.visibility) catch continue;
            if (try self.read(bool, "forgotten-all") != null or !try self.entityVisible(stored)) continue;
            try values.append(self.a, try parse(std.json.Value, self.a, try self.encode(stored)));
        }
        return self.encode(.{ .items = values.items, .next_cursor = try self.encodeCursor("entity/", page.next), .complete = page.next == null });
    }
};

fn explicitDisposition(record: t.StoredAssertion) t.ExplicitDisposition {
    return .{ .lifecycle = record.lifecycle, .valid_to = record.assertion.valid_to, .superseded_by = record.superseded_by, .superseded_valid_at = record.superseded_valid_at, .recorded_to = record.recorded_to, .conflicts_with = record.conflicts_with, .reason = record.reason };
}

fn matchesTime(request: t.Recall, record: t.StoredAssertion, now: i64) bool {
    if (record.stale) return false;
    if (!request.include_history and request.valid_at == null and (record.lifecycle == .superseded or record.lifecycle == .retracted)) return false;
    if (request.valid_at orelse if (!request.include_history) now else null) |time| {
        if (record.lifecycle == .retracted) return false;
        if (record.assertion.valid_from) |start| if (start > time) return false;
        if (record.assertion.valid_to) |end| if (end <= time) return false;
    }
    return true;
}

fn relationValues(claim: t.Assertion, relation: t.Relationship) []const []const u8 {
    return switch (relation) {
        .depends_on => claim.depends_on,
        .supersedes => claim.supersedes,
        .motivates => claim.motivates,
        .rejected_in_favor_of => claim.rejected_in_favor_of,
    };
}
fn relationsMatch(request: t.Recall, claim: t.Assertion) bool {
    if (request.missing_relationship) |relation| if (relationValues(claim, relation).len != 0) return false;
    if (request.relationship) |relation| {
        const values = relationValues(claim, relation);
        if (request.target_id) |target| {
            for (values) |value| if (eql(u8, target, value)) return true;
            return false;
        }
        return values.len > 0;
    }
    return request.target_id == null;
}
pub fn actionGrant(action: Action) t.RequiredGrant {
    return switch (action) {
        .evaluation_results, .put_evaluation_dataset, .evaluation_dataset, .evaluation_cases, .start_evaluation, .create, .configure, .stage_embeddings => .admin,
        .evaluation_run, .evaluation_tasks, .evaluation_verdict, .propose_supersession, .supersession_proposals, .supersession_proposal, .review_supersession, .curate_entity, .propose_entity, .review_entity, .entity_proposals, .entity_proposal, .propose_relationship, .relationship_proposal, .review_relationship => .curator,
        else => .write,
    };
}
pub fn operationGrant(operation: t.Operation) t.RequiredGrant {
    return operation.required_grant orelse actionGrant(std.meta.stringToEnum(Action, operation.action) orelse return .admin);
}
fn publicOperation(input: t.Operation) t.OperationStatus {
    var result: t.OperationStatus = undefined;
    inline for (@typeInfo(t.OperationStatus).@"struct".field_names) |name| @field(result, name) = @field(input, name);
    return result;
}

fn parse(comptime T: type, a: A, bytes: []const u8) !T {
    return std.json.parseFromSliceLeaky(T, a, if (bytes.len == 0) "{}" else bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
}
fn textDigest(a: A, bytes: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
fn hash(a: A, value: anytype) ![]const u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
fn key(a: A, prefix: []const u8, id: []const u8) ![]const u8 {
    try t.validateId(id);
    return std.fmt.allocPrint(a, "{s}/{s}", .{ prefix, try hash(a, id) });
}
fn dependencyKey(a: A, kind: []const u8, parent: []const u8, child: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "dependency/{s}/{s}/{s}", .{ kind, try hash(a, parent), try hash(a, child) });
}
fn configKey(a: A, generation: u64) ![]const u8 {
    return std.fmt.allocPrint(a, "configuration/{d:0>20}", .{generation});
}
fn historyKey(a: A, id: []const u8, revision: u64) ![]const u8 {
    return std.fmt.allocPrint(a, "history/{s}/{d:0>20}", .{ try hash(a, id), revision });
}
fn groupPrefix(a: A, claim: t.Assertion, owner: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "group/{s}/", .{try hash(a, .{ claim.subject, claim.predicate, claim.scope, claim.environment, claim.task, claim.visibility, if (claim.visibility == .private) owner else "" })});
}
fn groupKey(a: A, claim: t.Assertion, owner: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}{s}", .{ try groupPrefix(a, claim, owner), try hash(a, claim.id) });
}
fn intervalsOverlap(left: t.Assertion, right: t.Assertion) bool {
    if (left.valid_to) |end| if (right.valid_from) |start| if (end <= start) return false;
    if (right.valid_to) |end| if (left.valid_from) |start| if (end <= start) return false;
    return true;
}
fn sameGroup(left: t.Assertion, right: t.Assertion) bool {
    return left.visibility == right.visibility and eql(u8, left.subject, right.subject) and eql(u8, left.predicate, right.predicate) and eql(u8, left.scope, right.scope) and optionalEqual(left.environment, right.environment) and optionalEqual(left.task, right.task);
}
fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    return if (left) |value| if (right) |other| eql(u8, value, other) else false else right == null;
}
fn cosine(left: []const f32, right: []const f32) !f64 {
    if (left.len == 0 or left.len != right.len) return error.MemoryEmbeddingMismatch;
    var dot: f64 = 0;
    var ln: f64 = 0;
    var rn: f64 = 0;
    for (left, right) |l, r| {
        if (!std.math.isFinite(l) or !std.math.isFinite(r)) return error.MemoryEmbeddingMismatch;
        dot += @as(f64, l) * r;
        ln += @as(f64, l) * l;
        rn += @as(f64, r) * r;
    }
    return if (ln == 0 or rn == 0) 0 else @max(0, dot / @sqrt(ln * rn));
}

fn encodedSize(value: anytype) !usize {
    var size: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(value, .{}, &size.writer);
    return @intCast(size.fullCount());
}

fn stringLess(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}
fn isConceptRelation(name: []const u8) bool {
    return eql(u8, name, "is_a") or eql(u8, name, "part_of") or eql(u8, name, "alternative_to");
}

fn compactName(a: A, value: []const u8) ![]const u8 {
    var result: std.ArrayList(u8) = .empty;
    for (value) |byte| if (std.ascii.isAlphanumeric(byte) or byte >= 128) {
        try result.append(a, std.ascii.toLower(byte));
    };
    return result.items;
}
fn sameNameNumbers(left: []const u8, right: []const u8) bool {
    var l: usize = 0;
    var r: usize = 0;
    while (true) {
        while (l < left.len and !std.ascii.isDigit(left[l])) : (l += 1) {}
        while (r < right.len and !std.ascii.isDigit(right[r])) : (r += 1) {}
        if (l == left.len or r == right.len) return l == left.len and r == right.len;
        const lb = l;
        const rb = r;
        while (l < left.len and std.ascii.isDigit(left[l])) : (l += 1) {}
        while (r < right.len and std.ascii.isDigit(right[r])) : (r += 1) {}
        if (!eql(u8, left[lb..l], right[rb..r])) return false;
    }
}
fn nameNumbers(a: A, value: []const u8) ![]const u8 {
    var result: std.ArrayList(u8) = .empty;
    var in_number = false;
    for (value) |byte| {
        if (std.ascii.isDigit(byte)) {
            try result.append(a, byte);
            in_number = true;
        } else if (in_number) {
            try result.append(a, '/');
            in_number = false;
        }
    }
    if (in_number) try result.append(a, '/');
    return result.items;
}

fn performanceTestRun(a: A) Run {
    const Empty = struct {
        fn get(_: *anyopaque, _: A, _: []const u8) !?Row {
            return null;
        }
        fn scan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: u32) !Page {
            return .{ .rows = &.{} };
        }
    };
    return .{ .a = a, .lexical_allocator = a, .service = .{ .backend = .{ .ptr = undefined, .get = Empty.get, .scan = Empty.scan, .commit = undefined }, .context = .{ .principal = "alice", .admin = true, .now_ms = 10 } }, .state = .{ .config = .{}, .owner = "alice", .created_at = 1 }, .explanation_snapshot_scratch = .init(a) };
}

test "memory performance decoded records allocate once and replay temporal dependency tracking" {
    const buffer = try std.testing.allocator.alloc(u8, 1024 * 1024);
    defer std.testing.allocator.free(buffer);
    var fixed = std.heap.FixedBufferAllocator.init(buffer);
    const a = fixed.allocator();
    var run = performanceTestRun(a);
    const claim: t.StoredAssertion = .{ .assertion = .{ .id = "a", .subject = "s", .predicate = "fact", .value = "v", .statement = "Large repeated payload", .evidence = &.{.{ .id = "e" }}, .valid_from = 50 }, .owner = "alice", .recorded_from = 1, .revision = 1, .config_generation = 1 };
    try run.put("assertion/a", claim);
    try run.put("evidence/e", t.StoredEvidence{ .evidence = .{ .id = "e", .source_id = "s", .source_version = "1", .text = "Source", .expires_at = 60 }, .owner = "alice", .recorded_at = 1, .revision = 1 });
    try run.put("entity/s", t.EntityRecord{ .entity = .{ .id = "s", .name = "Subject", .evidence = &.{.{ .id = "e" }} }, .owner = "alice", .revision = 1 });
    _ = try run.read(t.StoredAssertion, "assertion/a");
    _ = try run.read(t.StoredEvidence, "evidence/e");
    _ = try run.read(t.EntityRecord, "entity/s");
    const used = fixed.end_index;
    for (0..1024) |_| {
        _ = try run.read(t.StoredAssertion, "assertion/a");
        _ = try run.read(t.StoredEvidence, "evidence/e");
        _ = try run.read(t.EntityRecord, "entity/s");
    }
    try std.testing.expectEqual(used, fixed.end_index);
    run.temporal_tracking = true;
    _ = try run.read(t.StoredAssertion, "assertion/a");
    try std.testing.expectEqual(@as(usize, 1), run.semantic_assertions.count());
    try std.testing.expectEqual(@as(?i64, 50), run.temporal_boundary);
    run.semantic_assertions.clearRetainingCapacity();
    run.temporal_boundary = null;
    _ = try run.read(t.StoredEvidence, "evidence/e");
    try std.testing.expectEqual(@as(?i64, 60), run.temporal_boundary);
    _ = try run.read(t.StoredAssertion, "assertion/a");
    try std.testing.expectEqual(@as(usize, 1), run.semantic_assertions.count());
    try std.testing.expectEqual(@as(?i64, 50), run.temporal_boundary);
    const checkpoint = try a.dupe(Change, run.changes.items);
    var changed = claim;
    changed.assertion.statement = "Replacement";
    try run.put("assertion/a", changed);
    try std.testing.expectEqualStrings("Replacement", (try run.read(t.StoredAssertion, "assertion/a")).?.assertion.statement);
    try run.remove("evidence/e");
    try std.testing.expect(try run.read(t.StoredEvidence, "evidence/e") == null);
    try run.resetChanges(checkpoint);
    try std.testing.expectEqualStrings(claim.assertion.statement, (try run.read(t.StoredAssertion, "assertion/a")).?.assertion.statement);
    try std.testing.expect(try run.read(t.StoredEvidence, "evidence/e") != null);
}

test "memory performance ten thousand overlay writes preserve order replacements tombstones and rollback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = performanceTestRun(a);
    for (0..10000) |i| try run.put(try std.fmt.allocPrint(a, "row/{d}", .{i}), @as(u64, @intCast(i)));
    const checkpoint = try a.dupe(Change, run.changes.items);
    for (0..10000) |i| {
        const path = try std.fmt.allocPrint(a, "row/{d}", .{i});
        try std.testing.expectEqual(@as(?u64, @intCast(i)), try run.read(u64, path));
        if (i % 2 == 0) try run.remove(path) else try run.put(path, @as(u64, 42));
    }
    try std.testing.expectEqual(@as(usize, 10000), run.changes.items.len);
    for (run.changes.items, 0..) |change, i| {
        try std.testing.expectEqualStrings(checkpoint[i].key, change.key);
        if (i % 2 == 0) try std.testing.expect(change.json == null) else try std.testing.expectEqualStrings("42", change.json.?);
    }
    try run.resetChanges(checkpoint);
    for (0..10000) |i| try std.testing.expectEqual(@as(?u64, @intCast(i)), try run.read(u64, try std.fmt.allocPrint(a, "row/{d}", .{i})));
}

test "memory performance identity components rebuild on policy replacement removal and rollback" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = performanceTestRun(a);
    const redirects = try a.alloc(curation.Redirect, 1000);
    for (redirects, 0..) |*redirect, i| redirect.* = .{ .key = try std.fmt.allocPrint(a, "alias-{d}", .{i}), .target = if (i < 500) "first" else "second" };
    try run.put("entity-policy", EntityPolicy{ .redirects = redirects });
    const members = try run.identityMembers("alias-123");
    try std.testing.expectEqual(@as(usize, 501), members.len);
    const used = arena.queryCapacity();
    for (0..1000) |_| try std.testing.expect((try run.identityMembers("first")).ptr == members.ptr);
    try std.testing.expectEqual(used, arena.queryCapacity());
    const checkpoint = try a.dupe(Change, run.changes.items);
    try run.put("entity-policy", EntityPolicy{ .redirects = &.{.{ .key = "first", .target = "second" }} });
    try std.testing.expectEqual(@as(usize, 2), (try run.identityMembers("first")).len);
    try run.remove("entity-policy");
    try std.testing.expectEqual(@as(usize, 1), (try run.identityMembers("first")).len);
    try run.resetChanges(checkpoint);
    try std.testing.expectEqual(@as(usize, 501), (try run.identityMembers("alias-123")).len);
}

test "memory performance overlay range pages merge replacements and tombstones among ten thousand unrelated writes" {
    const BackendScan = struct {
        fn scan(_: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !Page {
            const source = [_]Row{ .{ .key = "tree/0050", .json = "0" }, .{ .key = "tree/0060", .json = "0" }, .{ .key = "tree/0099", .json = "0" }, .{ .key = "tree/0200", .json = "200" } };
            var begin: usize = 0;
            while (begin < source.len and cursor != null and !std.mem.lessThan(u8, cursor.?, source[begin].key)) : (begin += 1) {}
            const end = @min(source.len, begin + limit);
            try std.testing.expectEqualStrings("tree/", prefix);
            return .{ .rows = try a.dupe(Row, source[begin..end]), .next = if (end < source.len) source[end - 1].key else null };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = performanceTestRun(a);
    run.service.backend = .{ .ptr = &run, .get = undefined, .scan = BackendScan.scan, .commit = undefined };
    for (0..10000) |i| try run.put(try std.fmt.allocPrint(a, "unrelated/{d}", .{i}), @as(u64, @intCast(i)));
    for (0..100) |i| try run.put(try std.fmt.allocPrint(a, "tree/{d:0>4}", .{i}), @as(u64, @intCast(i)));
    try run.put("tree/0050", @as(u64, 999));
    try run.remove("tree/0060");
    const checkpoint = try a.dupe(Change, run.changes.items);
    try run.remove("tree/0051");
    try run.resetChanges(checkpoint);
    var cursor: ?[]const u8 = null;
    var count: usize = 0;
    while (true) {
        const page = try run.scanOverlay("tree/", cursor, 7);
        try std.testing.expect(page.rows.len <= 7);
        for (page.rows) |row| {
            if (cursor) |after| try std.testing.expect(std.mem.lessThan(u8, after, row.key));
            try std.testing.expect(!eql(u8, row.key, "tree/0060"));
            if (eql(u8, row.key, "tree/0050")) try std.testing.expectEqualStrings("999", row.json);
            count += 1;
        }
        cursor = page.next;
        if (cursor == null) break;
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "memory projection provenance equality validates all values without allocating" {
    var storage: [1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&storage);
    var run = performanceTestRun(fixed.allocator());
    const model: t.Model = .{ .provider = "test", .model = "dense", .revision = "1", .dimensions = 2 };
    run.state.config.models.embedder = model;
    const record: t.EntityRecord = .{ .entity = .{ .id = "e", .name = "Name", .aliases = &.{"Alias"} }, .owner = "alice", .revision = 2 };
    const dependencies: []const t.DerivedDependency = &.{.{ .resource = .{ .table = "memory", .table_id = 1, .kind = .entity, .id = "e", .revision = 2 }, .evidence = &.{.{ .id = "source" }} }};
    var view: t.EntityView = undefined;
    view.fingerprint = "fingerprint";
    view.dependencies = dependencies;
    view.description = "Description";
    var projection: t.EntityProjection = .{ .entity_id = "e", .entity_revision = 2, .model = model, .generation = run.state.config.embedding_generation, .config_generation = run.state.config.generation, .owner = "alice", .visibility = record.entity.visibility, .kind = .names, .fingerprint = view.fingerprint, .dependencies = dependencies, .text = "Name\nAlias", .embedding = &.{ 1, 0 } };
    for (0..1024) |_| try std.testing.expect(try run.semanticCurrent(projection, record, view));
    try std.testing.expectEqual(@as(usize, 0), fixed.end_index);
    projection.model.revision = "2";
    try std.testing.expect(!try run.semanticCurrent(projection, record, view));
    projection.model = model;
    projection.dependencies = &.{.{ .resource = .{ .table = "memory", .table_id = 1, .kind = .entity, .id = "e", .revision = 3 }, .evidence = &.{.{ .id = "source" }} }};
    try std.testing.expect(!try run.semanticCurrent(projection, record, view));
    projection.dependencies = dependencies;
    projection.text = "Name\nAlias\n";
    try std.testing.expect(!try run.semanticCurrent(projection, record, view));
    projection.text = "Name\nAlias";
    projection.embedding = &.{1};
    try std.testing.expect(!try run.semanticCurrent(projection, record, view));
}

test "memory index retirement retains an embedding generation still reserved by semantic recall" {
    var config: t.Config = .{ .embedding_generation = 7, .features = .{ .semantic_recall = true } };
    try std.testing.expect(Run.indexLeaseLive(config, .{ .kind = .semantic, .generation = 7 }));
    try std.testing.expect(!Run.indexLeaseLive(config, .{ .kind = .semantic, .generation = 6 }));
    config.features.semantic_recall = false;
    try std.testing.expect(!Run.indexLeaseLive(config, .{ .kind = .semantic, .generation = 7 }));
    // Re-enabling either semantic feature after both were disabled allocates a
    // new embedding generation; a retired name cannot become current again.
    config.embedding_generation = 8;
    config.semantic_discovery.enabled = true;
    try std.testing.expect(!Run.indexLeaseLive(config, .{ .kind = .semantic, .generation = 7 }));
}

fn inputFingerprint(a: A, value: anytype, secret: []const u8, version: u8) ![]const u8 {
    if (version == 0) return hash(a, value);
    if (version != 1 or secret.len != 64) return error.MemoryCorruptState;
    var key_bytes: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&key_bytes, secret) catch return error.MemoryCorruptState;
    const raw = try std.json.Stringify.valueAlloc(a, value, .{});
    var digest: [32]u8 = undefined;
    std.crypto.auth.hmac.sha2.HmacSha256.create(&digest, raw, &key_bytes);
    return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}
fn screenedAction(action: Action) bool {
    return switch (action) {
        .recall, .start_evaluation, .fork_lineage, .checkpoint_lineage, .revert_lineage, .put_evaluation_dataset, .evaluation_verdict, .propose_supersession, .review_supersession, .observe, .assertions, .correct, .outcome, .curate_entity, .propose_entity, .review_entity, .propose_relationship, .review_relationship => true,
        else => false,
    };
}

fn screenBody(a: A, action: Action, body: []const u8, config: screening.Config) !screening.Result {
    inline for (@typeInfo(Action).@"enum".field_names) |name| {
        const candidate = @field(Action, name);
        if (comptime screenedAction(candidate)) {
            if (action == candidate) {
                const T = operations.Body(candidate);
                const value = try parse(T, a, body);
                const result = try screening.typed(T, a, value, config);
                if (comptime candidate == .assertions or candidate == .observe) {
                    try checkScreenedEmbeddings(value.assertions, result.value.assertions);
                }
                return .{ .value = if (result.report.redactions == 0) body else try std.json.Stringify.valueAlloc(a, result.value, .{}), .report = result.report };
            }
        }
    }
    return .{ .value = body };
}

fn checkScreenedEmbeddings(original: []const t.Assertion, admitted: []const t.Assertion) !void {
    for (original, admitted) |before, after| if (before.embedding != null and !eql(u8, before.statement, after.statement)) return error.MemoryEmbeddingMismatch;
}

test "memory performance indexed identity scope excludes ten thousand unrelated redirects" {
    const Empty = struct {
        var scans: usize = 0;
        fn scan(_: *anyopaque, _: A, prefix: []const u8, _: ?[]const u8, _: u32) !Page {
            try std.testing.expect(prefix.len != 0);
            scans += 1;
            return .{ .rows = &.{}, .next = null };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = performanceTestRun(a);
    run.service.backend.scan = Empty.scan;
    const redirects = try a.alloc(curation.Redirect, 10000);
    for (redirects, 0..) |*redirect, i| redirect.* = .{ .key = try std.fmt.allocPrint(a, "alias-{d}", .{i}), .target = try std.fmt.allocPrint(a, "canonical-{d}", .{i}) };
    try run.identityStore().adoptNamespace(.{ .key_template = Run.identity_template, .revision = 1, .merges = redirects });
    Empty.scans = 0;
    try run.prepareIdentityIndex(&.{"alias-123"}, false);
    try std.testing.expectEqual(@as(usize, 0), Empty.scans);
    try std.testing.expectEqual(@as(usize, 1), run.identity_policy.?.redirects.len);
    try std.testing.expectEqualStrings("canonical-123", run.identity_index.?.canonical("alias-123"));
    const first_scope = run.identity_index.?.source.ptr;
    for (0..512) |i| {
        try run.prepareIdentityIndex(&.{try std.fmt.allocPrint(a, "alias-{d}", .{i})}, false);
        try std.testing.expectEqual(@as(usize, 1), run.identity_policy.?.redirects.len);
    }
    try run.prepareIdentityIndex(&.{"alias-123"}, false);
    try std.testing.expect(first_scope == run.identity_index.?.source.ptr);
    try std.testing.expectEqual(@as(usize, 512), run.identity_scopes.count());
    const members = try run.identityMembers("alias-123");
    try std.testing.expectEqual(@as(usize, 2), members.len);
    try std.testing.expect(Empty.scans <= 6);
    try std.testing.expectEqual(@as(usize, 1), run.identity_policy.?.redirects.len);
}

test "memory performance forward identity shares long chains and converging paths" {
    const Empty = struct {
        fn scan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: u32) !Page {
            return .{ .rows = &.{}, .next = null };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var run = performanceTestRun(a);
    run.service.backend.scan = Empty.scan;
    const redirects = try a.alloc(curation.Redirect, 10512);
    for (redirects[0..10000], 0..) |*redirect, i| redirect.* = .{ .key = try std.fmt.allocPrint(a, "chain-{d}", .{i}), .target = try std.fmt.allocPrint(a, "chain-{d}", .{i + 1}) };
    for (redirects[10000..], 0..) |*redirect, i| redirect.* = .{ .key = try std.fmt.allocPrint(a, "branch-{d}", .{i}), .target = "chain-5000" };
    try run.identityStore().adoptNamespace(.{ .key_template = Run.identity_template, .revision = 1, .merges = redirects });
    for (redirects) |redirect| try std.testing.expectEqualStrings("chain-10000", (try run.forwardIdentity(redirect.key)).canonical);
    try std.testing.expectEqual(@as(usize, 10513), run.test_identity_forward_steps);
    try std.testing.expectEqual(@as(usize, 10513), run.identity_forward.count());
    const before = (try run.identityStore().component(Run.identity_template, &.{"chain-0"})).?;
    var next = before.namespace;
    next.revision += 1;
    next.state.suppress.keys = &.{"chain-10000"};
    try run.identityStore().apply(try identity_records.difference(a, before.namespace, next));
    try std.testing.expect((try run.forwardIdentity("chain-9999")).target_suppressed);
}

test "memory admitted verdict snapshot excludes cached overlay redirects" {
    const Base = struct {
        rows: std.StringHashMapUnmanaged([]const u8) = .empty,
        fn get(ptr: *anyopaque, _: A, path: []const u8) !?Row {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return if (self.rows.get(path)) |raw| .{ .key = path, .json = raw } else null;
        }
        fn scan(ptr: *anyopaque, a: A, prefix: []const u8, after: ?[]const u8, limit: u32) !Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var rows: std.ArrayList(Row) = .empty;
            var it = self.rows.iterator();
            while (it.next()) |entry| if (std.mem.startsWith(u8, entry.key_ptr.*, prefix) and (after == null or std.mem.lessThan(u8, after.?, entry.key_ptr.*))) try rows.append(a, .{ .key = entry.key_ptr.*, .json = entry.value_ptr.* });
            std.mem.sort(Row, rows.items, {}, struct {
                fn less(_: void, lhs: Row, rhs: Row) bool {
                    return std.mem.lessThan(u8, lhs.key, rhs.key);
                }
            }.less);
            const count = @min(limit, rows.items.len);
            return .{ .rows = rows.items[0..count], .next = if (count < rows.items.len) rows.items[count - 1].key else null };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var base: Base = .{};
    var run = performanceTestRun(a);
    run.service.backend.ptr = &base;
    run.service.backend.get = Base.get;
    run.service.backend.scan = Base.scan;
    const original: @import("../storage/db/resolver_namespace.zig").Namespace = .{ .key_template = Run.identity_template, .revision = 1 };
    try run.identityStore().adoptNamespace(original);
    for (run.changes.items) |change| try base.rows.put(a, change.key, change.json.?);
    // Start a new operation after committing the fixture's admitted base.
    run = performanceTestRun(a);
    run.service.backend.ptr = &base;
    run.service.backend.get = Base.get;
    run.service.backend.scan = Base.scan;
    var replacement = original;
    replacement.revision = 2;
    replacement.merges = &.{.{ .key = "a", .target = "b" }};
    try run.identityStore().apply(try identity_records.difference(a, original, replacement));
    try std.testing.expectEqualStrings("b", (try run.forwardIdentity("a")).canonical);
    var before = run.admittedViewRun();
    try std.testing.expectEqualStrings("a", (try before.forwardIdentity("a")).canonical);
    try std.testing.expectEqualStrings("b", (try run.forwardIdentity("a")).canonical);
}

test "memory performance supersession byte caps allocate one bounded page and preserve the maximal ranked prefix" {
    var inputs = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer inputs.deinit();
    const a = inputs.allocator();
    const statement = try a.alloc(u8, t.max_text_bytes);
    @memset(statement, 'x');
    // Exercise JSON escaping in exact sizing as well as large plain text.
    @memcpy(statement[0..4], "\"\\\n\t");
    const seed: t.ClaimView = .{ .memory = .{ .assertion = .{ .id = "seed", .subject = "entity/subject", .predicate = "property", .value = "value", .statement = statement, .evidence = &.{.{ .id = "source" }} }, .owner = "alice", .recorded_from = 1, .revision = 1, .config_generation = 1 }, .fingerprint = "fingerprint", .config_generation = 1 };
    const items = try a.alloc(t.SupersessionCandidate, 64);
    for (items, 0..) |*item, i| {
        var view = seed;
        view.memory.assertion.id = try std.fmt.allocPrint(a, "candidate-{d}", .{i});
        item.* = .{ .view = view, .lexical_score = 1, .semantic_similarity = null };
    }
    const page: t.SupersessionCandidatePage = .{ .seed = seed, .items = items, .retrieval = .indexed, .semantic_discovery = .disabled, .truncated = false, .revision = 1 };
    const storage = try a.alloc(u8, 256 * 1024);
    var fixed = std.heap.FixedBufferAllocator.init(storage);
    var run = performanceTestRun(fixed.allocator());
    const output = try run.renderSupersessionCandidates(page, storage.len);
    const parsed = try parse(t.SupersessionCandidatePage, a, output);
    try std.testing.expect(parsed.truncated);
    try std.testing.expectEqual(@as(usize, 2), parsed.items.len);
    try std.testing.expectEqualStrings("candidate-0", parsed.items[0].view.memory.assertion.id);
    try std.testing.expectEqualStrings("candidate-1", parsed.items[1].view.memory.assertion.id);
    try std.testing.expectEqualStrings(statement, parsed.seed.memory.assertion.statement);
    try std.testing.expectEqual(output.len, fixed.end_index);
    const bytes = output.len;
    // Exact boundary, including the shorter `true` JSON boolean.
    fixed.reset();
    try std.testing.expectEqual(bytes, (try run.renderSupersessionCandidates(page, bytes)).len);
    fixed.reset();
    const smaller = try parse(t.SupersessionCandidatePage, a, try run.renderSupersessionCandidates(page, bytes - 1));
    try std.testing.expectEqual(@as(usize, 1), smaller.items.len);
    var one = page;
    one.items = items[0..1];
    fixed.reset();
    const full = try parse(t.SupersessionCandidatePage, a, try run.renderSupersessionCandidates(one, storage.len));
    try std.testing.expect(!full.truncated);
    fixed.reset();
    try std.testing.expectError(error.MemoryInputTooLarge, run.renderSupersessionCandidates(page, 4096));
    try std.testing.expectEqual(@as(usize, 0), fixed.end_index);
    const Cancel = struct {
        fn check(_: *anyopaque) !void {
            return error.Canceled;
        }
    };
    var unused: u8 = 0;
    run.service.context.check_ptr = &unused;
    run.service.context.check_fn = Cancel.check;
    try std.testing.expectError(error.Canceled, run.renderSupersessionCandidates(page, storage.len));
    try std.testing.expectEqual(@as(usize, 0), fixed.end_index);
    std.debug.print("SUPERSESSION BYTE CAP candidates=64 max_bytes=262144 kept=2 output_bytes={d} allocated_bytes={d} discarded_page_buffers=0\n", .{ bytes, bytes });
}

test "memory performance identity rebuild discovers each canonical group once across dependency pages" {
    const Counts = struct {
        groups: usize = 0,
        dependencies: usize = 0,
        fn scan(ptr: *anyopaque, _: A, prefix: []const u8, _: ?[]const u8, _: u32) !Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.startsWith(u8, prefix, "group/")) self.groups += 1;
            if (std.mem.startsWith(u8, prefix, "dependency/identity/")) self.dependencies += 1;
            return .{ .rows = &.{} };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var counts: Counts = .{};
    var run = performanceTestRun(a);
    run.service.backend.ptr = &counts;
    run.service.backend.scan = Counts.scan;
    try run.put("entity-policy", EntityPolicy{ .redirects = &.{.{ .key = "alias", .target = "subject" }}, .state = .{ .suppress = .{ .keys = &.{"subject"} } } });
    var names: std.StringHashMapUnmanaged(void) = .empty;
    try names.put(a, "alias", {});
    try names.put(a, "subject", {});
    for (0..206) |i| {
        const id = try std.fmt.allocPrint(a, "claim-{d}", .{i});
        var record: t.StoredAssertion = .{ .assertion = .{ .id = id, .subject = if (i % 2 == 0) "alias" else "subject", .predicate = "fact", .value = "value", .statement = "Claim", .evidence = &.{}, .visibility = .team }, .owner = if (i % 2 == 0) "alice" else "bob", .recorded_from = 1, .revision = 1, .config_generation = 1 };
        switch (i) {
            200 => record.assertion.scope = "other-scope",
            201 => record.assertion.environment = "other-environment",
            202 => record.assertion.task = "other-task",
            203, 204 => record.assertion.visibility = .private,
            205 => record.assertion.predicate = "other-predicate",
            else => {},
        }
        try run.put(try key(a, "assertion", id), record);
        try run.put(try groupKey(a, record.assertion, record.owner), .{ .id = id });
        // Both dependency lists mention every claim, as happens when an
        // identity is used as both a subject and an object during curation.
        for ([_][]const u8{ "alias", "subject" }) |name| try run.put(try dependencyKey(a, "identity", name, id), .{ .child = id });
    }
    const affected = try run.collectIdentityGroups(&names);
    try std.testing.expectEqual(@as(usize, 206), affected.count());
    try std.testing.expectEqual(@as(usize, 4), counts.dependencies);
    try std.testing.expectEqual(@as(usize, 14), counts.groups); // Seven groups × two raw identities.
    std.debug.print("IDENTITY GROUP DISCOVERY dependencies=412 groups=7 raw_group_scans={d} retained_claims={d}\n", .{ counts.groups, affected.count() });
}
