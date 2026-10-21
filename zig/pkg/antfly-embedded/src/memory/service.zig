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
pub const types = @import("types.zig");
pub const generation_policy = @import("generation.zig");
pub const operations = @import("operations.zig");
const t = types;
const prov = t.provenance;
const A = std.mem.Allocator;
const eql = std.mem.eql;
const curation = @import("antfly_resolver").curation;
const max_dependency_depth: u8 = 32;
const max_support_work = 65536;
const resource_check_interval_ms: i64 = 30000;
const ReasonBindings = struct { assertion: ?[]const u8 = null, disposition: ?[]const u8 = null };
const DecisionInput = struct { id: []const u8, revision: u64, reasons: ReasonBindings, correction_evidence: []const t.EvidenceRef = &.{} };
const DecisionProvenance = struct { principal: []const u8, recorded_at: i64, config_generation: u64, assertion_id: []const u8, evidence_ids: []const []const u8, inputs: []const DecisionInput, incoming_reason: ?[]const u8 = null };
// Causal inference inputs are conjunctive retention/authorization dependencies,
// independent of the alternative factual support selected by a producer.
const GenerationProvenance = struct { operation_id: []const u8, config_generation: u64, evidence_ids: []const []const u8 };
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
pub const Backend = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, []const u8) anyerror!?Row,
    scan: *const fn (*anyopaque, A, []const u8, ?[]const u8, u32) anyerror!Page,
    commit: *const fn (*anyopaque, A, ?Row, []const Change) anyerror!void,
    // Model integration must use Antfly's admitted provider/runtime. No model
    // client or credentials are owned by this policy module.
    resource: ?*const fn (*anyopaque, A, prov.Resource) anyerror!prov.Snapshot = null,
    generate: ?*const fn (*anyopaque, A, t.Config, []const t.StoredEvidence, InferenceBoundary) anyerror!t.Generated = null,
    decide: ?*const fn (*anyopaque, A, t.Config, t.Assertion, []const t.StoredAssertion) anyerror!t.ReconciliationDecision = null,
    embed: ?*const fn (*anyopaque, A, t.Model, []const u8) anyerror![]const f32 = null,
    validate_models: ?*const fn (*anyopaque, t.Config) anyerror!void = null,
};
pub const Context = struct {
    table_id: u64 = 0,
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    space: []const u8 = "",
    execution_principal: []const u8 = "",
    execution_signature: []const u8 = "",
    durable_execution_required: bool = false,
    principal: []const u8,
    admin: bool = false,
    now_ms: i64,
    grant_ptr: ?*anyopaque = null,
    require_grant_fn: ?*const fn (*anyopaque, t.RequiredGrant) anyerror!void = null,
    final_check_ptr: ?*anyopaque = null,
    final_check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    check_ptr: ?*anyopaque = null,
    check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    pub fn requireGrant(self: Context, grant: t.RequiredGrant) !void {
        if (grant == .admin and !self.admin) return error.Forbidden;
        if (self.require_grant_fn) |call| try call(self.grant_ptr.?, grant);
    }
    pub fn finish(self: Context) !void {
        try self.check();
        if (self.final_check_fn) |call| try call(self.final_check_ptr.?);
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
        var run: Run = .{ .service = self, .a = a, .processing_failure = execution.processing_failure, .defer_timeouts = execution.defer_timeouts };
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
            if (run.state.table_id != 0 and self.context.table_id != 0 and run.state.table_id != self.context.table_id) return error.MemorySpaceBindingChanged;
            if (run.state.space.len > 0 and self.context.space.len > 0 and (!eql(u8, run.state.space, self.context.space) or !eql(u8, run.state.database, self.context.database) or !eql(u8, run.state.namespace, self.context.namespace))) return error.MemorySpaceBindingChanged;
        }
        const fingerprint = try hash(a, .{ .action = action, .id = id, .body = body, .principal = self.context.principal });
        const receipt_key = if (idempotency_key.len > 0) try key(a, "operation", try hash(a, .{ self.context.principal, idempotency_key })) else "";
        const required_grant = if (mutates(action)) try run.requiredGrant(action, id, body) else null;
        if (required_grant) |grant| try self.context.requireGrant(grant);
        if (mutates(action)) {
            if (idempotency_key.len == 0 or idempotency_key.len > 256) return error.MemoryIdempotencyKeyRequired;
            if (try run.read(t.Operation, receipt_key)) |receipt| {
                if (!eql(u8, receipt.request_hash, fingerprint)) return error.MemoryIdempotencyConflict;
                try self.context.requireGrant(operationGrant(receipt));
                try self.context.finish();
                return try std.json.Stringify.valueAlloc(alloc, publicOperation(receipt), .{});
            }
            if (execution.expected_revision) |revision| if (run.state.revision != revision) return error.MemoryRevisionConflict;
            if (run.state.pending_config != null and action != .advance and action != .forget and action != .stage_embeddings) return error.MemoryConfigurationTransition;
            if (action == .create) {
                if (state_row != null) return error.MemorySpaceExists;
                try self.validate(run.state.config);
            }
            if (run.state.revision == std.math.maxInt(u64)) return error.MemoryCorruptState;
            run.state.revision += 1;
        }
        // A current ingestion budget cannot invalidate a committed receipt or
        // prevent the control operations needed to recover/configure a space.
        if (operations.usesInputBudget(action) and body.len > run.state.config.max_input_bytes) return error.MemoryInputTooLarge;
        var op: t.Operation = .{
            .required_grant = required_grant,
            .execution_principal = self.context.execution_principal,
            .execution_signature = self.context.execution_signature,
            .id = if (receipt_key.len > 0) try hash(a, .{ self.context.principal, idempotency_key }) else "",
            .action = @tagName(action),
            .request_hash = fingerprint,
            .owner = self.context.principal,
            .revision = run.state.revision,
            .config_generation = run.state.config.generation,
            .created_at = self.context.now_ms,
        };
        var output: ?[]const u8 = null;
        switch (action) {
            .create => try run.put(try configKey(a, run.state.config.generation), run.state.config),
            .explain => output = try run.explain(id, try parse(prov.ExplainRequest, a, body)),
            .configuration => {
                const generation = std.fmt.parseInt(u64, id, 10) catch return error.InvalidMemoryRequest;
                const config = (try run.read(t.Config, try configKey(a, generation))) orelse return error.MemoryNotFound;
                output = try run.encode(config);
            },
            .inspect => output = try run.encode(.{ .state = .{ .table_id = run.state.table_id, .database = run.state.database, .namespace = run.state.namespace, .space = run.state.space, .config = run.state.config, .pending_config = run.state.pending_config, .revision = run.state.revision, .owner = run.state.owner, .created_at = run.state.created_at }, .capabilities = .{ .native = true, .model_free = true, .generator_available = self.backend.generate != null, .embedder_available = self.backend.embed != null } }),
            .configure => {
                try run.requireAdmin();
                const request = try parse(operations.Configure, a, body);
                if (request.expected_revision != run.state.revision - 1) return error.MemoryRevisionConflict;
                var config = request.config;
                // Admission revisions are durable and never reused, including failed
                // builds. Serving generations are not allocation counters.
                config.generation = run.state.revision;
                try self.validate(config);
                if (self.context.execution_signature.len > 0) {
                    run.state.execution_principal = self.context.execution_principal;
                    run.state.execution_signature = self.context.execution_signature;
                    run.state.execution_owner = self.context.principal;
                }
                if (config.retention_ms != run.state.config.retention_ms) run.state.next_expiry_at = self.context.now_ms;
                const previous_model = try run.encode(run.state.config.models.embedder);
                const replacement_model = try run.encode(config.models.embedder);
                const rebuild = config.features.semantic_recall and (!run.state.config.features.semantic_recall or !eql(u8, previous_model, replacement_model));
                config.embedding_generation = if (rebuild) config.generation else run.state.config.embedding_generation;
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
                const request = try parse(t.ObservationRequest, a, body);
                try run.observe(request, &op);
            },
            .assertions => {
                const request = try parse(t.AssertionRequest, a, body);
                try run.assertions(request.assertions, false);
            },
            .correct => try run.correct(try parse(t.Correction, a, body)),
            .forget => try run.forget(try parse(t.Forget, a, body)),
            .outcome => try run.outcome(try parse(t.Outcome, a, body)),
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
                if (!eql(u8, receipt.owner, self.context.principal) and !self.context.admin) return error.Forbidden;
                output = try run.encode(publicOperation(receipt));
            },
            .history => output = try run.history(id, try parse(operations.Page, a, body)),
            .recall => output = try run.recall(try parse(t.Recall, a, body)),
            .list => output = try run.enumerate(try parse(t.Recall, a, body)),
            .stage_embeddings => try run.stageEmbeddings(try parse(t.EmbeddingBatch, a, body)),
            .curate_entity => try run.curateEntity(try parse(t.EntityCorrection, a, body)),
            .resolve_entity => output = try run.resolveEntity(id),
            .entities => output = try run.entityList(try parse(operations.Page, a, body)),
        }
        if (mutates(action)) {
            try run.recordDecision(op);
            try run.put(receipt_key, op);
            try run.recordOperationVersions(op);
            if (op.phase == .pending and (action == .observe or action == .configure)) try run.put(try key(a, "pending", op.id), .{ .id = op.id });
            try run.put("control", run.state);
            try self.context.finish();
            if (run.inferred) try run.validateResources();
            try self.backend.commit(self.backend.ptr, a, state_row, run.changes.items);
            output = try run.encode(publicOperation(op));
        } else {
            // Reads spanning pages must not report a mixture of revisions.
            const latest = (try self.backend.get(self.backend.ptr, a, "control")) orelse return error.MemorySpaceNotFound;
            const state = try parse(t.State, a, latest.json);
            if (state.revision != run.state.revision) return error.MemoryRevisionConflict;
            try self.context.finish();
        }
        return try alloc.dupe(u8, output.?);
    }

    fn validate(self: Service, config: t.Config) !void {
        try config.validate();
        if (self.context.durable_execution_required and self.context.execution_signature.len == 0 and (config.features.assertion_distillation or config.features.entity_extraction or config.features.summaries or (config.features.semantic_recall and !eql(u8, config.models.embedder.?.provider, "external")) or config.retention_ms != null)) return error.MemoryExecutionIdentityRequired;
        if (config.features.assertion_distillation and self.backend.generate == null) return error.MemoryCapabilityUnavailable;
        if (config.features.reconciliation == .model and self.backend.decide == null) return error.MemoryCapabilityUnavailable;
        if (config.features.semantic_recall and self.backend.embed == null) return error.MemoryCapabilityUnavailable;
        if (self.backend.validate_models) |validate_models| try validate_models(self.backend.ptr, config) else {
            if (config.features.entity_extraction or config.features.summaries or config.features.reconciliation == .model) return error.MemoryCapabilityUnavailable;
        }
    }
};

const Run = struct {
    const ResourceRead = struct { reference: prov.Resource, snapshot: prov.Snapshot, epoch: u64 };
    const ResourceSet = std.StringHashMapUnmanaged(void);
    service: Service,
    a: A,
    state: t.State = undefined,
    state_row: ?Row = null,
    changes: std.ArrayList(Change) = .empty,
    identity_policy: ?EntityPolicy = null,
    identity_index: ?curation.RedirectIndex = null,
    suppression_index: ?curation.Index = null,
    dependency_depths: std.StringHashMapUnmanaged(u8) = .empty,
    dependency_visits: usize = 0,
    support_work: usize = 0,
    historical_resource_reads: bool = false,
    retention_maintenance: bool = false,
    resource_states: std.StringHashMapUnmanaged(ResourceRead) = .empty,
    inferred: bool = false,
    inference_epoch: u64 = 0,
    capturing_resources: ?*ResourceSet = null,
    processing_failure: ?anyerror = null,
    defer_timeouts: bool = false,
    explanation_decisions: std.StringHashMapUnmanaged(void) = .empty,
    explanation_truncated: bool = false,
    explanation_incomplete_history: bool = false,
    explanation_records: std.StringHashMapUnmanaged(ExplainedRecord) = .empty,
    decision_targets: std.ArrayList(prov.Resource) = .empty,
    views: std.StringHashMapUnmanaged(bool) = .empty,
    backend_rows: std.StringHashMapUnmanaged(?[]const u8) = .empty,
    historical: std.StringHashMapUnmanaged(?t.StoredAssertion) = .empty,

    fn encode(self: *Run, value: anytype) ![]const u8 {
        return std.json.Stringify.valueAlloc(self.a, value, .{});
    }
    fn put(self: *Run, path: []const u8, value: anytype) !void {
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
        const json = try self.encode(value);
        for (self.changes.items) |*change| if (eql(u8, change.key, path)) {
            change.json = json;
            return;
        };
        try self.changes.append(self.a, .{ .key = path, .json = json });
    }
    fn remove(self: *Run, path: []const u8) !void {
        self.views.clearRetainingCapacity();
        self.historical.clearRetainingCapacity();
        for (self.changes.items) |*change| if (eql(u8, change.key, path)) {
            change.json = null;
            return;
        };
        try self.changes.append(self.a, .{ .key = path, .json = null });
    }
    fn read(self: *Run, comptime T: type, path: []const u8) !?T {
        for (self.changes.items) |change| if (eql(u8, change.key, path)) return if (change.json) |json| try parse(T, self.a, json) else null;
        if (self.backend_rows.getEntry(path)) |entry| return if (entry.value_ptr.*) |json| try parse(T, self.a, json) else null;
        const row = try self.service.backend.get(self.service.backend.ptr, self.a, path);
        const json = if (row) |value| value.json else null;
        try self.backend_rows.put(self.a, try self.a.dupe(u8, path), json);
        return if (json) |value| try parse(T, self.a, value) else null;
    }
    fn requireAdmin(self: *Run) !void {
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
        if (try self.read(bool, try key(self.a, "forgotten-assertion", decision.assertion_id)) != null) return false;
        if (view == .visible) {
            if (!try self.generationVisible("assertion", decision.assertion_id)) return false;
        } else if (!try self.generationRetained("assertion", decision.assertion_id)) return false;
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
        var evidence_ids: std.StringHashMapUnmanaged(void) = .empty;
        for (claim.evidence) |ref| try evidence_ids.put(self.a, ref.id, {});
        for (claim.support_sets) |support| for (support.evidence) |ref| {
            try self.supportStep();
            try evidence_ids.put(self.a, ref.id, {});
        };
        const ids = try self.a.alloc([]const u8, evidence_ids.count());
        var evidence_iterator = evidence_ids.keyIterator();
        var count: usize = 0;
        while (evidence_iterator.next()) |id| : (count += 1) ids[count] = id.*;
        var inputs: std.ArrayList(DecisionInput) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (prior) |record| {
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
        const path = try std.fmt.allocPrint(self.a, "decision/{s}/{d:0>20}", .{ try hash(self.a, claim.id), self.state.revision });
        try self.put(path, DecisionProvenance{ .principal = self.service.context.principal, .recorded_at = self.service.context.now_ms, .config_generation = self.state.config.generation, .assertion_id = claim.id, .evidence_ids = ids, .inputs = inputs.items, .incoming_reason = incoming_reason });
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
        return record;
    }
    fn authorize(self: *Run, owner: []const u8, visibility: t.Visibility) !void {
        if (visibility == .private and !eql(u8, owner, self.service.context.principal) and !self.service.context.admin) return error.Forbidden;
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
        if (self.capturing_resources) |inputs| try inputs.put(self.a, cache_key, {});
        if (self.resource_states.getPtr(cache_key)) |value| {
            // Another invocation may have released the lease since this input
            // was checked. Validate before exposing it to a later invocation.
            if (value.epoch != self.inference_epoch) {
                try self.validateResource(value.*);
                value.epoch = self.inference_epoch;
            }
            return value.snapshot;
        }
        const resolve = self.service.backend.resource orelse return error.MemoryCapabilityUnavailable;
        const value = try resolve(self.service.backend.ptr, self.a, reference);
        try self.cacheResource(reference, value);
        return value;
    }
    fn cacheResource(self: *Run, reference: prov.Resource, snapshot: prov.Snapshot) !void {
        try self.resource_states.put(self.a, try hash(self.a, .{ reference, self.service.context.principal }), .{ .reference = reference, .snapshot = snapshot, .epoch = self.inference_epoch });
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
        std.debug.assert(self.capturing_resources == null);
        self.capturing_resources = &inputs;
        defer self.capturing_resources = null;
        self.views.clearRetainingCapacity();
        const result = try @call(.auto, handler, .{self} ++ args);
        return .{ .resources = inputs, .result = result };
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
        for (inputs.evidence_ids) |input| {
            try self.supportStep();
            const source = (try self.evidence(input)) orelse return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
            if (!try self.evidenceRetained(source)) return false;
        }
        return true;
    }
    fn entityRetained(self: *Run, record: t.EntityRecord) !bool {
        return try self.retainedSupport(record.entity.evidence, &.{}, 0) and try self.generationRetained("entity", record.entity.id);
    }
    fn generationVisible(self: *Run, kind: []const u8, id: []const u8) !bool {
        const provenance = (try self.read(GenerationProvenance, try self.generationKey(kind, id))) orelse return true;
        for (provenance.evidence_ids) |input| {
            if (!try self.supportVisible(&.{.{ .id = input }}, &.{}, 0)) return false;
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
        return hash(self.a, .{ kind, id, revision, known, valid, depth, self.state.config.generation, self.service.context.principal, self.service.context.admin, self.retention_maintenance });
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
        const cache_key = try hash(self.a, .{ id, known });
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
                const version = try parse(t.StoredAssertion, self.a, row.json);
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
        for (request.evidence) |original| {
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
                source.text = snapshot.json;
                if (source.source_id.len == 0) source.source_id = try hash(self.a, .{ reference.database, reference.namespace, reference.table_id, reference.kind, reference.id });
                if (source.source_version.len == 0) source.source_version = try std.fmt.allocPrint(self.a, "{d}", .{reference.revision});
                try self.put(try std.fmt.allocPrint(self.a, "resource-evidence/{s}/{s}", .{ try hash(self.a, reference.identity()), try hash(self.a, source.id) }), .{ .evidence_id = source.id });
                try self.put(try key(self.a, "resource-watch", source.id), .{ .evidence_id = source.id, .last_state = snapshot.state });
                self.state.next_resource_check_at = self.service.context.now_ms +| resource_check_interval_ms;
            }
            try t.validateId(source.id);
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
            } else try self.put(path, t.StoredEvidence{ .evidence = source, .owner = self.service.context.principal, .recorded_at = self.service.context.now_ms, .revision = self.state.revision });
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
            if (claim.embedding_model == null or !eql(u8, try self.encode(claim.embedding_model.?), try self.encode(model))) return error.MemoryEmbeddingMismatch;
            for (vector) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        }
    }
    fn assertions(self: *Run, claims: []const t.Assertion, generated: bool) !void {
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
            if (self.state.config.features.semantic_recall and claim.embedding == null) {
                claim.embedding = try self.embed(self.state.config.models.embedder.?, claim.statement, embedding_inputs);
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
        const decision = try decide(self.service.backend.ptr, self.a, self.state.config, claim.*, prior);
        try self.inferenceCompleted(inputs.resources);
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
        return try self.storeDecision(claim.*, prior, incoming_reason);
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
        record.assertion = visible.assertion;
        // Apply only the new verdict; retain already accepted relationship links.
        try self.applySupersessions(record.assertion, record.owner, decision);
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
    fn objectTyped(self: *Run, predicate: []const u8) bool {
        for (self.state.config.predicates) |definition| if (eql(u8, definition.name, predicate)) return definition.object_type != null;
        return false;
    }
    fn prepareIdentityIndex(self: *Run) !void {
        if (self.identity_index != null) return;
        const policy = (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
        if (policy.redirects.len > 4096) return error.MemoryWorkLimit;
        const redirects = try curation.RedirectIndex.init(self.a, policy.redirects);
        var suppression = try curation.Index.init(self.a, "", try self.encode(policy.state));
        try suppression.bindRedirects(self.a, redirects);
        self.identity_policy = policy;
        self.identity_index = redirects;
        self.suppression_index = suppression;
    }
    fn identityMembers(self: *Run, id: []const u8) ![]const []const u8 {
        try self.prepareIdentityIndex();
        const policy = self.identity_policy.?;
        const index = self.identity_index.?;
        const target = index.canonical(id);
        var names: std.StringHashMapUnmanaged(void) = .empty;
        try names.put(self.a, id, {});
        try names.put(self.a, target, {});
        for (policy.redirects) |redirect| if (eql(u8, index.canonical(redirect.key), target)) {
            try names.put(self.a, redirect.key, {});
        };
        const result = try self.a.alloc([]const u8, names.count());
        var iterator = names.keyIterator();
        var count: usize = 0;
        while (iterator.next()) |name| : (count += 1) result[count] = name.*;
        return result;
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
            var cursor: ?[]const u8 = null;
            while (true) {
                try self.service.context.check();
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
    fn rebuildIdentityGroups(self: *Run, names: *std.StringHashMapUnmanaged(void)) !void {
        var affected: std.StringHashMapUnmanaged(t.StoredAssertion) = .empty;
        var iterator = names.keyIterator();
        while (iterator.next()) |name| {
            const prefix = try std.fmt.allocPrint(self.a, "dependency/identity/{s}/", .{try hash(self.a, name.*)});
            var cursor: ?[]const u8 = null;
            while (true) {
                const page = try self.scanOverlay(prefix, cursor, 128);
                for (page.rows) |row| {
                    const entry = try parse(struct { child: []const u8 }, self.a, row.json);
                    const seed = (try self.assertion(entry.child)) orelse continue;
                    if (!names.contains(seed.assertion.subject) and !(self.objectTyped(seed.assertion.predicate) and names.contains(seed.assertion.value))) continue;
                    for (try self.canonicalGroup(seed.assertion, seed.owner)) |record| {
                        try affected.put(self.a, record.assertion.id, record);
                        if (affected.count() > t.max_items) return error.MemoryWorkLimit;
                    }
                }
                cursor = page.next;
                if (cursor == null) break;
            }
        }
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
        if (eql(u8, try self.encode(.{ previous.predicates, previous.source_rules, previous.features.reconciliation, previous.ontology_version, previous.policy_version }), try self.encode(.{ replacement.predicates, replacement.source_rules, replacement.features.reconciliation, replacement.ontology_version, replacement.policy_version }))) return;
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
    }
    fn canonical(self: *Run, id: []const u8) !?[]const u8 {
        if (try self.read(bool, try key(self.a, "forgotten-entity", id)) != null) return null;
        const input = try self.read(t.EntityRecord, try key(self.a, "entity", id));
        if (input) |record| {
            self.authorize(record.owner, record.entity.visibility) catch return null;
            if (!try self.entityVisible(record)) return null;
        }
        try self.prepareIdentityIndex();
        const redirects = self.identity_index.?;
        const index = self.suppression_index.?;
        if (index.keySuppressed(id)) return null;
        const canonical_id = redirects.canonical(id);
        if (index.keySuppressed(canonical_id)) return null;
        if (!eql(u8, id, canonical_id)) {
            if (input == null) return null;
            const target = (try self.read(t.EntityRecord, try key(self.a, "entity", canonical_id))) orelse return null;
            self.authorize(target.owner, target.entity.visibility) catch return null;
            if (!try self.entityVisible(target)) return null;
        }
        return canonical_id;
    }
    fn requireDistinctIdentities(self: *Run, names: *std.StringHashMapUnmanaged(void), redirects: curation.RedirectIndex) !void {
        var members = names.keyIterator();
        while (members.next()) |id| {
            try self.service.context.check();
            const record = (try self.read(t.EntityRecord, try key(self.a, "entity", id.*))) orelse continue;
            for (record.entity.distinct_from) |distinct| if (eql(u8, redirects.canonical(id.*), redirects.canonical(distinct))) return error.MemoryEntityConflict;
        }
    }
    fn curateEntity(self: *Run, request: t.EntityCorrection) !void {
        try self.requireAdmin();
        if (try self.read(bool, "forgotten-all") != null) return error.MemoryForgotten;
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        const stored = (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
        const from = (try self.read(t.EntityRecord, try key(self.a, "entity", request.from))) orelse return error.MemoryNotFound;
        try self.authorize(from.owner, .private);
        if (!try self.entityVisible(from)) return error.MemoryForgotten;
        if (request.to) |target| {
            const to = (try self.read(t.EntityRecord, try key(self.a, "entity", target))) orelse return error.MemoryNotFound;
            try self.authorize(to.owner, .private);
            if (!try self.entityVisible(to)) return error.MemoryForgotten;
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
                .suppress => .suppress,
                .unsuppress => .unsuppress,
            },
            .from = if (request.action == .merge or request.action == .unmerge) request.from else null,
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
        try self.put("entity-policy", EntityPolicy{ .state = plan.state, .redirects = plan.merges });
        self.identity_policy = null;
        self.identity_index = null;
        self.suppression_index = null;
        // Reconcile affected belief groups in the redirect transaction.
        // A work-limit rejection publishes neither identity nor partial facts.
        try self.rebuildIdentityGroups(&affected_names);
        try self.invalidate("entity", request.from);
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
            if (ref.span) |span| if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
        }
        if (value.aliases.len > t.max_items or value.distinct_from.len > t.max_items) return error.MemoryInputTooLarge;
        for (value.aliases) |alias| try t.validateId(alias);
        for (value.distinct_from) |other| {
            try t.validateId(other);
            if (eql(u8, other, value.id)) return error.MemoryEntityConflict;
        }
        const path = try key(self.a, "entity", value.id);
        if (try self.read(t.EntityRecord, path)) |old| {
            try self.authorize(old.owner, .private);
            if (!eql(u8, try self.encode(old.entity), try self.encode(value))) return error.MemoryEntityConflict;
            return;
        }
        // Identity is explicit. Names/aliases are discoverable candidates and
        // never constitute authority to merge two canonical IDs.
        try self.put(path, t.EntityRecord{ .entity = value, .owner = self.service.context.principal, .revision = self.state.revision, .recorded_at = self.service.context.now_ms });
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
        for (page.rows) |row| try rows.append(self.a, row);
        for (self.changes.items) |change| {
            if (!std.mem.startsWith(u8, change.key, prefix)) continue;
            if (cursor) |after| if (!std.mem.lessThan(u8, after, change.key)) continue;
            if (page.next) |end| if (std.mem.lessThan(u8, end, change.key)) continue;
            var exists = false;
            for (rows.items) |row| if (eql(u8, row.key, change.key)) {
                exists = true;
                break;
            };
            if (!exists) if (change.json) |json| try rows.append(self.a, .{ .key = change.key, .json = json });
        }
        return .{ .rows = rows.items, .next = page.next };
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
            self.identity_policy = null;
            self.identity_index = null;
            self.suppression_index = null;
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
            try self.remove(try key(self.a, "evidence", id));
            try self.put(try key(self.a, "forgotten-evidence", id), true);
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
        if (!eql(u8, pending.owner, self.service.context.principal) and !self.service.context.admin) return error.Forbidden;
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
                try invocation.run.service.context.finish();
                // Forgetting, configuration and other memory mutations may
                // commit while an embedded lease is released. Stop stale work
                // before its next provider call, rather than waiting for CAS.
                const expected = invocation.run.state_row orelse return error.MemorySpaceNotFound;
                const latest = (try invocation.run.service.backend.get(invocation.run.service.backend.ptr, invocation.run.a, "control")) orelse return error.MemorySpaceNotFound;
                if (latest.version != expected.version or !eql(u8, latest.json, expected.json)) return error.MemoryRevisionConflict;
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
        self.decision_targets.clearRetainingCapacity();
        // A rejected model output publishes no partial assertions or entities.
        self.changes.clearRetainingCapacity();
        pending.phase = if (err == error.MemoryForgotten or err == error.MemoryResourceChanged) .cancelled else if (pending.attempts >= self.state.config.max_attempts) .failed else .pending;
        pending.error_code = @errorName(err);
        pending.assertions_ready = false;
        pending.next_retry_at = if (pending.phase == .pending) self.service.context.now_ms +| (@as(i64, 1000) * (@as(i64, 1) << @intCast(@min(pending.attempts, 10)))) else null;
        try self.put(path, pending.*);
        if (pending.phase == .failed or pending.phase == .cancelled) try self.remove(try key(self.a, "pending", pending.id));
    }
    fn publishGenerated(self: *Run, pending: t.Operation, generated: t.Generated, sources: []const t.StoredEvidence) !void {
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
        for (generated.assertions) |claim| if (try self.assertion(claim.id) == null) {
            try self.put(try self.generationKey("assertion", claim.id), provenance);
            for (inputs) |input| try self.put(try dependencyKey(self.a, "evidence", input, claim.id), .{ .child = claim.id });
        };
        try self.assertions(generated.assertions, true);
    }
    const VectorProjection = struct { assertion_id: []const u8, generation: u64, model: t.Model, statement_hash: []const u8, embedding: []const f32 };
    fn vectorKey(self: *Run, generation: u64, id: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.a, "vector/{d:0>20}/{s}", .{ generation, try hash(self.a, id) });
    }
    fn putVector(self: *Run, config: t.Config, claim: t.Assertion, embedding: []const f32) !void {
        const model = config.models.embedder orelse return error.MemoryModelRequired;
        if (embedding.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
        for (embedding) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        try self.put(try self.vectorKey(config.embedding_generation, claim.id), VectorProjection{ .assertion_id = claim.id, .generation = config.embedding_generation, .model = model, .statement_hash = try hash(self.a, claim.statement), .embedding = embedding });
    }
    fn vectorFor(self: *Run, config: t.Config, claim: t.Assertion) !?[]const f32 {
        const projection = (try self.read(VectorProjection, try self.vectorKey(config.embedding_generation, claim.id))) orelse return null;
        if (projection.generation != config.embedding_generation or !eql(u8, projection.assertion_id, claim.id) or !eql(u8, projection.statement_hash, try hash(self.a, claim.statement)) or !eql(u8, try self.encode(projection.model), try self.encode(config.models.embedder))) return error.MemoryEmbeddingMismatch;
        return projection.embedding;
    }
    fn stageEmbeddings(self: *Run, request: t.EmbeddingBatch) !void {
        try self.requireAdmin();
        const config = self.state.pending_config orelse return error.MemoryConfigurationChanged;
        if (request.generation != config.embedding_generation or config.models.embedder == null or !eql(u8, try self.encode(request.model), try self.encode(config.models.embedder.?))) return error.MemoryEmbeddingMismatch;
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
        for (page.rows) |row| {
            const record = try parse(t.StoredAssertion, self.a, row.json);
            const inputs = try self.captureResources(publicationCandidate, .{record});
            if (!inputs.result) continue;
            if (eql(u8, config.models.embedder.?.provider, "external")) {
                if (try self.vectorFor(config, record.assertion) == null) return error.MemoryExternalVectorRequired;
            } else {
                const embedding = try self.embed(config.models.embedder.?, record.assertion.statement, inputs.resources);
                try self.putVector(config, record.assertion, embedding);
            }
        }
        // Do not mutate the pending receipt or serving state before all
        // projections for this page, including final belief cutover, succeed.
        if (page.next == null) {
            try self.applyConfiguration(config);
            self.state.pending_config = null;
            try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
            pending.phase = .succeeded;
            pending.assertions_ready = true;
            try self.remove(try key(self.a, "pending", pending.id));
        }
        pending.cursor = page.next;
        pending.attempts = 0;
        pending.error_code = null;
        pending.next_retry_at = null;
        try self.put(path, pending.*);
    }
    fn retireAssertion(self: *Run, id: []const u8) !void {
        // Retirement is irreversible physical cleanup, not a belief revision.
        // Fence identity reuse before dropping payload and generation metadata.
        try self.put(try key(self.a, "forgotten-assertion", id), true);
        try self.remove(try self.generationKey("assertion", id));
        try self.remove(try key(self.a, "assertion", id));
        try self.invalidate("assertion", id);
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
        try self.pollResources();
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
            self.changes.clearRetainingCapacity();
            try self.changes.appendSlice(self.a, checkpoint);
            self.state = state_before;
            self.views.clearRetainingCapacity();
            self.historical.clearRetainingCapacity();
            return false;
        };
        return true;
    }
    fn maintainChild(self: *Run, row: Row, item: DependencyWork) anyerror!void {
        const child = try parse(struct { child: []const u8 }, self.a, row.json);
        if (try self.assertion(child.child)) |value| {
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
        if (eql(u8, row.key, "entity-policy")) {
            // Also scrub policy rows left by earlier implementations
            // or a restored pre-cleanup snapshot of a forgotten space.
            if (try self.read(bool, "forgotten-all") != null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "evidence/")) {
            const source = try parse(t.StoredEvidence, self.a, row.json);
            const expires = self.evidenceExpiry(source);
            if (expires) |time| if (time > self.service.context.now_ms and (self.state.next_expiry_at == null or time < self.state.next_expiry_at.?)) {
                self.state.next_expiry_at = time;
            };
            if (!try self.evidenceRetained(source)) {
                try self.remove(row.key);
                try self.put(try key(self.a, "forgotten-evidence", source.evidence.id), true);
            }
        } else if (std.mem.startsWith(u8, row.key, "entity-curation/")) {
            const provenance = try parse(EntityCurationProvenance, self.a, row.json);
            if (!try self.curationProvenanceRetained(provenance)) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "entity/")) {
            const stored = try parse(t.EntityRecord, self.a, row.json);
            if (try self.read(bool, "forgotten-all") != null or !try self.entityRetained(stored)) {
                try self.put(try key(self.a, "forgotten-entity", stored.entity.id), true);
                try self.remove(try self.generationKey("entity", stored.entity.id));
                try self.remove(row.key);
            }
        } else if (std.mem.startsWith(u8, row.key, "decision/")) {
            const decision = try parse(DecisionProvenance, self.a, row.json);
            if (try self.assertion(decision.assertion_id) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "reason-binding/")) {
            var parts = std.mem.splitScalar(u8, row.key, '/');
            _ = parts.next();
            const id_hash = parts.next() orelse return error.MemoryCorruptState;
            if (try self.read(std.json.Value, try std.fmt.allocPrint(self.a, "assertion/{s}", .{id_hash})) == null) try self.remove(row.key);
        } else if (std.mem.startsWith(u8, row.key, "generation-input/")) {
            const owner_key = row.key["generation-input/".len..];
            if (try self.read(std.json.Value, owner_key) == null) try self.remove(row.key);
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
        const decision: prov.Decision = .{
            .resource = ref,
            .operation = try self.resourceRef(.operation, operation.id, operation.revision),
            .action = operation.action,
            .status = switch (operation.phase) {
                .pending, .running => .proposed,
                .failed => .rejected,
                .cancelled => .cancelled,
                else => .applied,
            },
            .initiator = .{ .id = self.service.context.principal, .assurance = .authenticated },
            .executor = .{ .id = if (self.service.context.execution_principal.len > 0) self.service.context.execution_principal else self.service.context.principal, .kind = .service, .assurance = .authenticated },
            .origin = operation.origin,
            .decided_at = self.service.context.now_ms,
            .applied_at = if (operation.phase == .succeeded) self.service.context.now_ms else null,
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
            const payload_truncated = (try self.encode(record)).len > request.max_bytes / 2;
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
        while (true) {
            const rendered = try self.encode(prov.Explanation{ .nodes = nodes.items, .complete = !truncated and !missing and !self.explanation_incomplete_history, .truncated = truncated, .missing_evidence = missing, .incomplete_history = self.explanation_incomplete_history });
            if (rendered.len <= request.max_bytes) return rendered;
            truncated = true;
            if (nodes.items.len > 1) {
                _ = nodes.pop();
                continue;
            }
            if (nodes.items.len == 1) {
                if (nodes.items[0].record != .null) {
                    nodes.items[0].record = .null;
                    nodes.items[0].payload_truncated = true;
                    continue;
                }
                if (nodes.items[0].links.len > 0) {
                    nodes.items[0].links = &.{};
                    continue;
                }
                _ = nodes.pop();
            }
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
    fn capturedSource(self: *Run, ref: prov.Resource, request: prov.ExplainRequest) !?t.StoredEvidence {
        const prefix = try std.fmt.allocPrint(self.a, "resource-evidence/{s}/", .{try hash(self.a, ref.identity())});
        const snapshots = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, null, request.max_nodes);
        for (snapshots.rows) |row| {
            const binding = try parse(struct { evidence_id: []const u8 }, self.a, row.json);
            const source = (try self.evidence(binding.evidence_id)) orelse continue;
            self.authorize(source.owner, source.evidence.visibility) catch continue;
            const actual = source.evidence.resource orelse return error.MemoryCorruptState;
            if (!ref.sameVersion(actual)) continue;
            if (ref.digest) |expected| {
                if (actual.digest == null or !std.ascii.eqlIgnoreCase(expected, actual.digest.?)) continue;
            }
            if (request.known_at) |known| if (source.recorded_at > known) continue;
            if (!try self.evidenceRetained(source)) continue;
            return source;
        }
        if (snapshots.next != null) self.explanation_truncated = true;
        return null;
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
            if (snapshot.state == .changed) {
                // Look up immutable identity, not mutable routing/display hints.
                const source = (try self.capturedSource(ref.*, request)) orelse {
                    if (request.known_at != null) {
                        self.explanation_incomplete_history = true;
                        return error.MemoryNotKnownAt;
                    }
                    return error.MemoryResourceChanged;
                };
                json = source.evidence.text;
            } else if (request.known_at != null) {
                // Ordinary-row versions do not attest when Antfly memory learned
                // about them. An admitted snapshot supplies that knowledge time.
                const source = (try self.capturedSource(ref.*, request)) orelse {
                    self.explanation_incomplete_history = true;
                    return error.MemoryNotKnownAt;
                };
                json = source.evidence.text;
            }
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
            return parse(std.json.Value, self.a, try self.encode(.{ .snapshot_json = json, .revision = ref.revision, .digest = ref.digest, .source_state = snapshot.state, .metadata_assurance = "source_declared" }));
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
        return try std.fmt.allocPrint(self.a, "{d}:{s}:{s}", .{ self.state.revision, try hash(self.a, .{ self.state.space, self.service.context.principal, prefix }), value });
    }
    fn decodeCursor(self: *Run, prefix: []const u8, cursor_value: ?[]const u8) !?[]const u8 {
        const value = cursor_value orelse return null;
        var parts = std.mem.splitScalar(u8, value, ':');
        const revision = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidMemoryCursor, 10) catch return error.InvalidMemoryCursor;
        const scope = parts.next() orelse return error.InvalidMemoryCursor;
        const path = parts.next() orelse return error.InvalidMemoryCursor;
        if (parts.next() != null or !std.mem.startsWith(u8, path, prefix)) return error.InvalidMemoryCursor;
        if (revision != self.state.revision) return error.MemoryCursorStale;
        if (!eql(u8, scope, try hash(self.a, .{ self.state.space, self.service.context.principal, prefix }))) return error.InvalidMemoryCursor;
        return path;
    }
    fn history(self: *Run, id: []const u8, request: anytype) ![]const u8 {
        const record = (try self.assertion(id)) orelse return error.MemoryNotFound;
        try self.authorize(record.owner, record.assertion.visibility);
        if (!try self.retained(record, 0)) return error.MemoryNotFound;
        if (request.limit == 0 or request.limit > t.max_items) return error.InvalidMemoryRequest;
        const prefix = try std.fmt.allocPrint(self.a, "history/{s}/", .{try hash(self.a, id)});
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, try self.decodeCursor(prefix, request.cursor), request.limit);
        var records: std.ArrayList(t.StoredAssertion) = .empty;
        for (page.rows) |row| {
            const value = try parse(t.StoredAssertion, self.a, row.json);
            try self.authorize(value.owner, value.assertion.visibility);
            try records.append(self.a, try self.publicRecord(value));
        }
        return self.encode(.{ .items = records.items, .next_cursor = try self.encodeCursor(prefix, page.next), .complete = page.next == null, .revision = self.state.revision });
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
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion/", after, @intCast(@min(128, 4096 - visited)));
            more = page.next != null;
            for (page.rows, 0..) |row, index| {
                after = row.key;
                visited += 1;
                more = index + 1 < page.rows.len or page.next != null;
                var record = try parse(t.StoredAssertion, self.a, row.json);
                if (request.known_at) |known| record = (try self.asOf(record.assertion.id, known)) orelse continue;
                self.authorize(record.owner, record.assertion.visibility) catch continue;
                if (!try self.evidenceVisibleTime(record, request.known_at, request.valid_at orelse if (!request.include_history) self.service.context.now_ms else null, 0)) continue;
                const claim = record.assertion;
                if (!relationsMatch(request, claim)) continue;
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
                try items.append(self.a, try self.publicRecord(record));
                if (items.items.len == request.limit) break;
            }
            if (!more) break;
        }
        return self.encode(.{ .items = items.items, .next_cursor = if (more) try self.encodeCursor("assertion/", after) else null, .complete = !more, .revision = self.state.revision });
    }
    fn recall(self: *Run, request: t.Recall) ![]const u8 {
        self.historical_resource_reads = request.known_at != null;
        if (request.limit == 0 or request.limit > t.max_items or request.token_budget == 0 or request.token_budget > self.state.config.max_recall_tokens) return error.InvalidMemoryRequest;
        const Hit = t.RecallHit;
        var hits: std.ArrayList(Hit) = .empty;
        var after = try self.decodeCursor("assertion/", request.cursor);
        var scanned: usize = 0;
        var vector = request.embedding;
        if (vector) |supplied| {
            if (!self.state.config.features.semantic_recall) return error.MemoryCapabilityUnavailable;
            const model = self.state.config.models.embedder orelse return error.MemoryModelRequired;
            if (request.embedding_model == null or !eql(u8, try self.encode(request.embedding_model.?), try self.encode(model)) or supplied.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
            for (supplied) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        }
        if (self.state.config.features.semantic_recall and vector == null and request.query.len > 0) vector = try self.embed(self.state.config.models.embedder.?, request.query, .empty);
        var more = false;
        while (true) {
            try self.service.context.check();
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion/", after, 128);
            for (page.rows) |row| {
                scanned += 1;
                var record = try parse(t.StoredAssertion, self.a, row.json);
                self.authorize(record.owner, record.assertion.visibility) catch continue;
                if (request.known_at) |known| record = (try self.asOf(record.assertion.id, known)) orelse continue;
                const valid = request.valid_at orelse if (!request.include_history) self.service.context.now_ms else null;
                if (!try self.evidenceVisibleTime(record, request.known_at, valid, 0)) continue;
                const claim = record.assertion;
                if (!relationsMatch(request, claim)) continue;
                const historical = record.lifecycle == .superseded or record.lifecycle == .retracted or (claim.valid_to != null and claim.valid_to.? <= self.service.context.now_ms);
                if (!matchesTime(request, record, self.service.context.now_ms)) continue;
                if (!request.include_provisional and claim.stability == .provisional) continue;
                const canonical_subject = (try self.canonical(claim.subject)) orelse continue;
                if (self.objectTyped(claim.predicate) and try self.canonical(claim.value) == null) continue;
                if (request.subject) |value| {
                    const wanted = (try self.canonical(value)) orelse continue;
                    if (!eql(u8, wanted, canonical_subject)) continue;
                }
                if (request.predicate) |value| if (!eql(u8, value, claim.predicate)) continue;
                if (request.scope) |value| if (!eql(u8, value, claim.scope)) continue;
                if (request.environment) |value| if (claim.environment != null and !eql(u8, value, claim.environment.?)) continue;
                if (request.task) |value| if (claim.task != null and !eql(u8, value, claim.task.?)) continue;
                if (request.kind) |value| if (value != claim.kind) continue;
                var score = lexical(request.query, claim.statement);
                const compatible_embedding = if (vector != null) try self.vectorFor(self.state.config, claim) else null;
                if (vector) |query_vector| if (compatible_embedding) |embedding| {
                    score += try cosine(query_vector, embedding);
                };
                if (request.query.len > 0 and score <= 0) continue;
                if (claim.stability == .provisional) score *= 0.5;
                const support = try self.visibleSupport(claim, request.known_at, valid);
                record.assertion.evidence = support.evidence;
                record.assertion.depends_on = support.depends_on;
                record.assertion.support_sets = &.{};
                record.assertion.embedding = null;
                record = try self.publicRecord(record);
                try hits.append(self.a, .{ .memory = record, .score = score, .historical = historical, .citations = support.evidence });
            }
            after = page.next;
            if (after == null) break;
            if (scanned >= 4096) {
                more = true;
                break;
            }
        }
        std.mem.sort(Hit, hits.items, {}, struct {
            fn less(_: void, left: Hit, right: Hit) bool {
                return if (left.score == right.score) std.mem.lessThan(u8, left.memory.assertion.id, right.memory.assertion.id) else left.score > right.score;
            }
        }.less);
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
        var result: t.RecallResult = .{ .items = selected.items, .next_cursor = try self.encodeCursor("assertion/", after), .complete = !more and omitted == 0, .truncated = more or omitted > 0, .scanned = scanned, .omitted = omitted, .token_upper_bound = 0, .revision = self.state.revision, .config_generation = self.state.config.generation, .retrieval = if (vector != null) .lexical_vector else .lexical };
        while (true) {
            // Bound the complete rendered response, including framing,
            // readiness, citations and the bound's own decimal digits.
            const rendered = try self.encode(result);
            if (rendered.len == result.token_upper_bound) {
                if (rendered.len <= request.token_budget) return rendered;
                if (selected.items.len == 0) return error.InvalidMemoryRequest;
                _ = selected.pop();
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
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "entity/", try self.decodeCursor("entity/", request.cursor), request.limit);
        var values: std.ArrayList(std.json.Value) = .empty;
        for (page.rows) |row| {
            const stored = try parse(t.EntityRecord, self.a, row.json);
            self.authorize(stored.owner, stored.entity.visibility) catch continue;
            if (try self.read(bool, "forgotten-all") != null or !try self.entityVisible(stored)) continue;
            try values.append(self.a, try parse(std.json.Value, self.a, row.json));
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
        .create, .configure, .curate_entity, .stage_embeddings => .admin,
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
fn lexical(query: []const u8, text: []const u8) f64 {
    if (query.len == 0) return 1;
    var tokens = std.mem.tokenizeAny(u8, query, " \t\r\n,.;:?!");
    var score: f64 = 0;
    var count: f64 = 0;
    while (tokens.next()) |token| {
        count += 1;
        if (token.len <= text.len) {
            for (0..text.len - token.len + 1) |i| {
                if (std.ascii.eqlIgnoreCase(text[i..][0..token.len], token)) {
                    score += 1;
                    break;
                }
            }
        }
    }
    return if (count > 0) score / count else 0;
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
