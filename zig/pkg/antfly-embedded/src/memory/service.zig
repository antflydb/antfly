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
const t = types;
const A = std.mem.Allocator;
const eql = std.mem.eql;
const curation = @import("antfly_resolver").curation;
const EntityPolicy = struct { state: curation.State = .{}, redirects: []const curation.Redirect = &.{} };

pub const Row = struct { key: []const u8, json: []const u8, version: u64 = 0, digest: ?[32]u8 = null };
pub const Change = struct { key: []const u8, json: ?[]const u8 };
pub const Page = struct { rows: []const Row, next: ?[]const u8 = null };
pub const Backend = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, []const u8) anyerror!?Row,
    scan: *const fn (*anyopaque, A, []const u8, ?[]const u8, u32) anyerror!Page,
    commit: *const fn (*anyopaque, A, ?Row, []const Change) anyerror!void,
    // Model integration must use Antfly's admitted provider/runtime. No model
    // client or credentials are owned by this policy module.
    generate: ?*const fn (*anyopaque, A, t.Config, []const t.StoredEvidence) anyerror!t.Generated = null,
    decide: ?*const fn (*anyopaque, A, t.Config, t.Assertion, []const t.StoredAssertion) anyerror!t.ReconciliationDecision = null,
    embed: ?*const fn (*anyopaque, A, t.Model, []const u8) anyerror![]const f32 = null,
    validate_models: ?*const fn (*anyopaque, t.Config) anyerror!void = null,
};
pub const Context = struct {
    database: []const u8 = "default",
    namespace: []const u8 = "public",
    space: []const u8 = "",
    execution_principal: []const u8 = "",
    execution_signature: []const u8 = "",
    durable_execution_required: bool = false,
    principal: []const u8,
    admin: bool = false,
    now_ms: i64,
    final_check_ptr: ?*anyopaque = null,
    final_check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    check_ptr: ?*anyopaque = null,
    check_fn: ?*const fn (*anyopaque) anyerror!void = null,
    pub fn finish(self: Context) !void {
        try self.check();
        if (self.final_check_fn) |call| try call(self.final_check_ptr.?);
    }
    pub fn check(self: Context) !void {
        if (self.principal.len == 0) return error.Unauthorized;
        if (self.check_fn) |call| try call(self.check_ptr.?);
    }
};
pub const Action = enum { create, inspect, configure, observe, assertions, recall, get, history, correct, forget, outcome, operation, advance, list, entities, curate_entity, resolve_entity, evidence, stage_embeddings, configuration };
pub fn mutates(action: Action) bool {
    return switch (action) {
        .inspect, .recall, .get, .history, .operation, .list, .entities, .resolve_entity, .evidence, .configuration => false,
        else => true,
    };
}
pub const Service = struct {
    backend: Backend,
    context: Context,

    pub fn execute(self: Service, alloc: A, action: Action, id: []const u8, body: []const u8, idempotency_key: []const u8) ![]u8 {
        try self.context.check();
        if (body.len > 4 * 1024 * 1024) return error.MemoryInputTooLarge;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var run: Run = .{ .service = self, .a = a };
        const state_row = try self.backend.get(self.backend.ptr, a, "control");
        run.state_row = state_row;
        if (action == .create and state_row == null) {
            if (!self.context.admin) return error.Forbidden;
            var config = try parse(t.Config, a, body);
            config.generation = 1;
            config.embedding_generation = 1;
            run.state = .{ .database = self.context.database, .namespace = self.context.namespace, .space = self.context.space, .execution_principal = self.context.execution_principal, .execution_signature = self.context.execution_signature, .execution_owner = self.context.principal, .config = config, .owner = self.context.principal, .created_at = self.context.now_ms };
        } else {
            const row = state_row orelse return error.MemorySpaceNotFound;
            run.state = try parse(t.State, a, row.json);
            if (run.state.space.len > 0 and self.context.space.len > 0 and (!eql(u8, run.state.space, self.context.space) or !eql(u8, run.state.database, self.context.database) or !eql(u8, run.state.namespace, self.context.namespace))) return error.MemorySpaceBindingChanged;
            if (body.len > run.state.config.max_input_bytes) return error.MemoryInputTooLarge;
        }
        const fingerprint = try hash(a, .{ .action = action, .id = id, .body = body, .principal = self.context.principal });
        const receipt_key = if (idempotency_key.len > 0) try key(a, "operation", try hash(a, .{ self.context.principal, idempotency_key })) else "";
        if (mutates(action)) {
            if (idempotency_key.len == 0 or idempotency_key.len > 256) return error.MemoryIdempotencyKeyRequired;
            if (try run.read(t.Operation, receipt_key)) |receipt| {
                if (!eql(u8, receipt.request_hash, fingerprint)) return error.MemoryIdempotencyConflict;
                try self.context.finish();
                return try std.json.Stringify.valueAlloc(alloc, publicOperation(receipt), .{});
            }
            if (run.state.pending_config != null and action != .advance and action != .forget and action != .stage_embeddings) return error.MemoryConfigurationTransition;
            if (action == .create) {
                if (state_row != null) return error.MemorySpaceExists;
                try self.validate(run.state.config);
            }
            if (run.state.revision == std.math.maxInt(u64)) return error.MemoryCorruptState;
            run.state.revision += 1;
        }
        var op: t.Operation = .{
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
            .configuration => {
                const generation = std.fmt.parseInt(u64, id, 10) catch return error.InvalidMemoryRequest;
                const config = (try run.read(t.Config, try configKey(a, generation))) orelse return error.MemoryNotFound;
                output = try run.encode(config);
            },
            .inspect => output = try run.encode(.{ .state = .{ .database = run.state.database, .namespace = run.state.namespace, .space = run.state.space, .config = run.state.config, .pending_config = run.state.pending_config, .revision = run.state.revision, .owner = run.state.owner, .created_at = run.state.created_at }, .capabilities = .{ .native = true, .model_free = true, .generator_available = self.backend.generate != null, .embedder_available = self.backend.embed != null } }),
            .configure => {
                try run.requireAdmin();
                const request = try parse(struct { expected_revision: u64, config: t.Config }, a, body);
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
                if (rebuild) {
                    run.state.pending_config = config;
                    op.phase = .pending;
                    op.assertions_ready = false;
                } else run.state.config = config;
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
                if (source.forgotten or run.isExpired(source) or try run.read(bool, "forgotten-all") != null or try run.read(bool, try key(a, "forgotten-source", source.evidence.source_id)) != null) return error.MemoryNotFound;
                output = try run.encode(source);
            },
            .get => {
                const record = (try run.assertion(id)) orelse return error.MemoryNotFound;
                try run.authorize(record.owner, record.assertion.visibility);
                if (!try run.evidenceVisible(record)) return error.MemoryNotFound;
                output = try run.encode(record);
            },
            .operation => {
                const receipt = (try run.read(t.Operation, try key(a, "operation", id))) orelse return error.MemoryNotFound;
                if (!eql(u8, receipt.owner, self.context.principal) and !self.context.admin) return error.Forbidden;
                output = try run.encode(publicOperation(receipt));
            },
            .history => output = try run.history(id, try parse(struct { cursor: ?[]const u8 = null, limit: u16 = 64 }, a, body)),
            .recall => output = try run.recall(try parse(t.Recall, a, body)),
            .list => output = try run.enumerate(try parse(t.Recall, a, body)),
            .stage_embeddings => try run.stageEmbeddings(try parse(t.EmbeddingBatch, a, body)),
            .curate_entity => try run.curateEntity(try parse(t.EntityCorrection, a, body)),
            .resolve_entity => output = try run.resolveEntity(id),
            .entities => output = try run.entityList(try parse(struct { cursor: ?[]const u8 = null, limit: u16 = 64 }, a, body)),
        }
        if (mutates(action)) {
            try run.put(receipt_key, op);
            if (op.phase == .pending and (action == .observe or action == .configure)) try run.put(try key(a, "pending", op.id), .{ .id = op.id });
            try run.put("control", run.state);
            try self.context.finish();
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
    service: Service,
    a: A,
    state: t.State = undefined,
    state_row: ?Row = null,
    changes: std.ArrayList(Change) = .empty,
    identity_policy: ?EntityPolicy = null,
    identity_index: ?curation.RedirectIndex = null,
    suppression_index: ?curation.Index = null,

    fn encode(self: *Run, value: anytype) ![]const u8 {
        return std.json.Stringify.valueAlloc(self.a, value, .{});
    }
    fn put(self: *Run, path: []const u8, value: anytype) !void {
        const json = try self.encode(value);
        for (self.changes.items) |*change| if (eql(u8, change.key, path)) {
            change.json = json;
            return;
        };
        try self.changes.append(self.a, .{ .key = path, .json = json });
    }
    fn remove(self: *Run, path: []const u8) !void {
        for (self.changes.items) |*change| if (eql(u8, change.key, path)) {
            change.json = null;
            return;
        };
        try self.changes.append(self.a, .{ .key = path, .json = null });
    }
    fn read(self: *Run, comptime T: type, path: []const u8) !?T {
        for (self.changes.items) |change| if (eql(u8, change.key, path)) return if (change.json) |json| try parse(T, self.a, json) else null;
        const row = (try self.service.backend.get(self.service.backend.ptr, self.a, path)) orelse return null;
        return try parse(T, self.a, row.json);
    }
    fn requireAdmin(self: *Run) !void {
        if (!self.service.context.admin and !eql(u8, self.state.owner, self.service.context.principal)) return error.Forbidden;
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
    // Physical retention is independent of current belief/lifecycle. A
    // superseded premise can still support an auditable historical revision.
    fn retained(self: *Run, record: t.StoredAssertion, depth: u8) anyerror!bool {
        if (depth >= 32) return error.MemoryWorkLimit;
        if (try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-assertion", record.assertion.id)) != null) return false;
        if (try self.retainedSupport(record.assertion.evidence, record.assertion.depends_on, depth)) return true;
        for (record.assertion.support_sets) |support| if (try self.retainedSupport(support.evidence, support.depends_on, depth)) return true;
        return false;
    }
    fn retainedSupport(self: *Run, refs: []const t.EvidenceRef, parents: []const []const u8, depth: u8) anyerror!bool {
        if (refs.len == 0) return false;
        for (refs) |ref| {
            const source = (try self.evidence(ref.id)) orelse return false;
            if (source.forgotten or self.isExpired(source) or try self.read(bool, try key(self.a, "forgotten-source", source.evidence.source_id)) != null) return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
        }
        for (parents) |id| {
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
        const prefix = try std.fmt.allocPrint(self.a, "history/{s}/", .{try hash(self.a, id)});
        var after: ?[]const u8 = null;
        var selected: ?t.StoredAssertion = null;
        var count: usize = 0;
        while (true) {
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, after, 128);
            for (page.rows) |row| {
                count += 1;
                if (count > 4096) return error.MemoryWorkLimit;
                const version = try parse(t.StoredAssertion, self.a, row.json);
                if (version.recorded_at <= known and (selected == null or version.revision > selected.?.revision)) selected = version;
            }
            after = page.next;
            if (after == null) return selected;
        }
    }
    fn evidenceVisibleDepth(self: *Run, record: t.StoredAssertion, depth: u8) anyerror!bool {
        return self.evidenceVisibleTime(record, null, null, depth);
    }
    fn evidenceVisibleTime(self: *Run, record: t.StoredAssertion, known: ?i64, valid: ?i64, depth: u8) anyerror!bool {
        if (depth >= 32) return error.MemoryWorkLimit;
        if (try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-assertion", record.assertion.id)) != null) return false;
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
            const source = (try self.evidence(ref.id)) orelse return false;
            if (source.forgotten or self.isExpired(source)) return false;
            if (try self.read(bool, try key(self.a, "forgotten-source", source.evidence.source_id)) != null) return false;
            self.authorize(source.owner, source.evidence.visibility) catch return false;
        }
        for (dependencies) |dependency| {
            const parent = (if (known) |time| try self.asOf(dependency, time) else try self.assertion(dependency)) orelse return false;
            self.authorize(parent.owner, parent.assertion.visibility) catch return false;
            if (parent.stale or parent.lifecycle == .retracted or parent.lifecycle == .disputed or parent.conflicts_with.len > 0) return false;
            if (parent.lifecycle == .superseded and valid == null) return false;
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
        if (request.evidence.len == 0 or request.evidence.len > t.max_items or request.context_evidence_ids.len > t.max_items or request.entities.len > self.state.config.max_output_items) return error.InvalidMemoryRequest;
        var inputs: std.ArrayList([]const u8) = .empty;
        for (request.evidence) |original| {
            var source = original;
            if (self.state.config.processing == .managed) source.managed = true;
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
                // A source is either client-produced or managed in this
                // request; never infer a second authoritative claim stream.
                for (request.assertions) |claim| for (claim.evidence) |ref| if (eql(u8, ref.id, source.id)) return error.MemoryProducerConflict;
                try inputs.append(self.a, source.id);
            } else if (self.state.config.features.entity_extraction or self.state.config.features.summaries) try inputs.append(self.a, source.id);
        }
        for (request.context_evidence_ids) |id| {
            const source = (try self.evidence(id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (source.forgotten or self.isExpired(source)) return error.MemoryForgotten;
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
    fn validateAssertion(self: *Run, claim: t.Assertion) anyerror!void {
        try t.validateId(claim.id);
        try t.validateId(claim.subject);
        try t.validateId(claim.predicate);
        if (try self.read(t.EntityRecord, try key(self.a, "entity", claim.subject))) |subject| {
            try self.authorize(subject.owner, subject.entity.visibility);
            if (claim.visibility == .team and subject.entity.visibility == .private) return error.MemoryVisibilityConflict;
        }
        for (self.state.config.predicates) |predicate| if (eql(u8, predicate.name, claim.predicate)) {
            if (predicate.subject_type) |expected| {
                const subject = (try self.read(t.EntityRecord, try key(self.a, "entity", claim.subject))) orelse return error.MemoryEntityRequired;
                try self.authorize(subject.owner, subject.entity.visibility);
                if (!eql(u8, expected, subject.entity.type)) return error.MemoryOntologyMismatch;
                if (claim.visibility == .team and subject.entity.visibility == .private) return error.MemoryVisibilityConflict;
            }
            if (predicate.object_type) |expected| {
                const object = (try self.read(t.EntityRecord, try key(self.a, "entity", claim.value))) orelse return error.MemoryEntityRequired;
                try self.authorize(object.owner, object.entity.visibility);
                if (!eql(u8, expected, object.entity.type)) return error.MemoryOntologyMismatch;
                if (claim.visibility == .team and object.entity.visibility == .private) return error.MemoryVisibilityConflict;
            }
        };
        if (claim.value.len == 0 or claim.statement.len == 0 or claim.statement.len > t.max_text_bytes or !std.unicode.utf8ValidateSlice(claim.statement)) return error.InvalidMemoryAssertion;
        if (claim.evidence.len == 0 or claim.evidence.len > t.max_items or claim.depends_on.len > t.max_items) return error.InvalidMemoryAssertion;
        if (claim.valid_from != null and claim.valid_to != null and claim.valid_from.? >= claim.valid_to.?) return error.InvalidMemoryAssertion;
        if (try self.read(bool, try key(self.a, "forgotten-assertion", claim.id)) != null) return error.MemoryForgotten;
        for (claim.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (source.forgotten or self.isExpired(source)) return error.MemoryForgotten;
            if (claim.visibility == .team and source.evidence.visibility == .private) return error.MemoryVisibilityConflict;
            if (ref.span) |span| {
                if (span.start >= span.end or span.end > source.evidence.text.len or !std.unicode.utf8ValidateSlice(source.evidence.text[span.start..span.end])) return error.InvalidMemorySpan;
            }
        }
        for (claim.depends_on) |dependency| {
            if (eql(u8, dependency, claim.id)) return error.MemoryDependencyCycle;
            const parent = (try self.assertion(dependency)) orelse return error.MemoryDependencyMissing;
            try self.authorize(parent.owner, parent.assertion.visibility);
            if (parent.stale or parent.lifecycle != .active or !try self.evidenceVisible(parent)) return error.MemoryDependencyStale;
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
        if (claim.support_sets.len > t.max_items) return error.MemoryInputTooLarge;
        for (claim.support_sets) |support| {
            var branch = claim;
            branch.evidence = support.evidence;
            branch.depends_on = support.depends_on;
            branch.support_sets = &.{};
            try self.validateAssertion(branch);
        }
        if (claim.embedding) |vector| {
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
            try self.validateAssertion(claim);
            if (!generated) for (claim.evidence) |ref| {
                const source = (try self.evidence(ref.id)).?;
                if (source.evidence.managed) return error.MemoryProducerConflict;
            };
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
                claim.embedding = try self.service.backend.embed.?(self.service.backend.ptr, self.a, self.state.config.models.embedder.?, claim.statement);
                claim.embedding_model = self.state.config.models.embedder;
            }
            try self.validateAssertion(claim);
            if (self.state.config.features.reconciliation == .model and self.singlePredicate(claim.predicate) and claim.modality == .actual and claim.role != .hypothesized and claim.stability == .settled) {
                const decide = self.service.backend.decide orelse return error.MemoryCapabilityUnavailable;
                const prior = try self.candidates(claim);
                if (prior.len > 0) {
                    const decision = try decide(self.service.backend.ptr, self.a, self.state.config, claim, prior);
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
                }
            }
            const record: t.StoredAssertion = .{ .assertion = claim, .input_hash = input_hash, .asserted_valid_to = claim.valid_to, .owner = self.service.context.principal, .recorded_from = self.service.context.now_ms, .revision = self.state.revision, .config_generation = self.state.config.generation, .embedding_generation = self.state.config.embedding_generation };
            try self.putAssertion(record);
            if (claim.kind == .summary or claim.kind == .procedural) try self.put(try dependencyKey(self.a, "entity", claim.subject, claim.id), .{ .child = claim.id });
            for (claim.evidence) |ref| try self.put(try dependencyKey(self.a, "evidence", ref.id, claim.id), .{ .child = claim.id });
            for (claim.depends_on) |parent| try self.put(try dependencyKey(self.a, "assertion", parent, claim.id), .{ .child = claim.id });
            for (claim.support_sets) |support| {
                for (support.evidence) |ref| try self.put(try dependencyKey(self.a, "evidence", ref.id, claim.id), .{ .child = claim.id });
                for (support.depends_on) |parent| try self.put(try dependencyKey(self.a, "assertion", parent, claim.id), .{ .child = claim.id });
            }
            for (claim.supersedes) |previous| {
                if (eql(u8, previous, claim.id)) return error.MemoryInvalidSupersession;
                var old = (try self.assertion(previous)) orelse return error.MemoryNotFound;
                try self.authorize(old.owner, old.assertion.visibility);
                if (claim.visibility == .private and !eql(u8, old.owner, self.service.context.principal)) return error.MemoryVisibilityConflict;
                if (!try self.sameCanonicalGroup(claim, old.assertion) or claim.modality != .actual or claim.role == .hypothesized or claim.stability != .settled) return error.MemoryInvalidSupersession;
                old.lifecycle = .superseded;
                old.superseded_by = claim.id;
                if (claim.valid_from) |start| {
                    if (old.assertion.valid_from) |previous_start| if (start < previous_start) return error.MemoryInvalidSupersession;
                    if (old.assertion.valid_to == null or old.assertion.valid_to.? > start) old.assertion.valid_to = start;
                }
                old.recorded_to = self.service.context.now_ms;
                old.explicit_disposition = explicitDisposition(old);
                old.revision = self.state.revision;
                try self.putAssertion(old);
                try self.invalidate("assertion", previous);
            }
            if (self.state.config.features.reconciliation == .deterministic) try self.reconcile(record);
        }
    }
    fn candidates(self: *Run, claim: t.Assertion) ![]const t.StoredAssertion {
        var result: std.ArrayList(t.StoredAssertion) = .empty;
        for (try self.canonicalGroup(claim, self.service.context.principal)) |old| {
            if (eql(u8, old.assertion.id, claim.id) or old.lifecycle != .active or old.stale or old.assertion.modality != .actual or old.assertion.role == .hypothesized or old.assertion.stability != .settled) continue;
            self.authorize(old.owner, old.assertion.visibility) catch continue;
            if (!try self.evidenceVisible(old)) continue;
            if (result.items.len >= t.max_items) return error.MemoryWorkLimit;
            try result.append(self.a, old);
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
                    for (try self.canonicalGroup(seed.assertion, seed.owner)) |record| {
                        try affected.put(self.a, record.assertion.id, record);
                        if (affected.count() > t.max_items) return error.MemoryWorkLimit;
                    }
                }
                cursor = page.next;
                if (cursor == null) break;
            }
        }
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
                record.recorded_to = baseline.recorded_to;
                record.conflicts_with = baseline.conflicts_with;
                record.reason = baseline.reason;
            }
            if (record.assertion.kind == .summary or record.assertion.kind == .procedural) record.stale = true;
            record.revision = self.state.revision;
            try self.putAssertion(record);
        }
        values = affected.valueIterator();
        while (values.next()) |value| if (try self.assertion(value.assertion.id)) |record| try self.reconcile(record);
    }
    fn canonical(self: *Run, id: []const u8) !?[]const u8 {
        const input = try self.read(t.EntityRecord, try key(self.a, "entity", id));
        if (input) |record| {
            self.authorize(record.owner, record.entity.visibility) catch return null;
            if (!try self.supportVisible(record.entity.evidence, &.{}, 0)) return null;
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
            if (!try self.supportVisible(target.entity.evidence, &.{}, 0)) return null;
        }
        return canonical_id;
    }
    fn curateEntity(self: *Run, request: t.EntityCorrection) !void {
        try self.requireAdmin();
        if (request.expected_revision != self.state.revision - 1) return error.MemoryRevisionConflict;
        const stored = (try self.read(EntityPolicy, "entity-policy")) orelse EntityPolicy{};
        const from = (try self.read(struct { entity: t.Entity, owner: []const u8 }, try key(self.a, "entity", request.from))) orelse return error.MemoryNotFound;
        try self.authorize(from.owner, .private);
        if (request.to) |target| {
            const to = (try self.read(struct { entity: t.Entity, owner: []const u8 }, try key(self.a, "entity", target))) orelse return error.MemoryNotFound;
            try self.authorize(to.owner, .private);
            if (request.action == .merge) {
                if (from.entity.visibility != to.entity.visibility or (from.entity.visibility == .private and !eql(u8, from.owner, to.owner))) return error.MemoryVisibilityConflict;
                for (from.entity.distinct_from) |id| if (eql(u8, id, target)) return error.MemoryEntityConflict;
                for (to.entity.distinct_from) |id| if (eql(u8, id, request.from)) return error.MemoryEntityConflict;
            }
        }
        var affected_names: std.StringHashMapUnmanaged(void) = .empty;
        for (try self.identityMembers(request.from)) |name| try affected_names.put(self.a, name, {});
        if (request.to) |target| for (try self.identityMembers(target)) |name| {
            try affected_names.put(self.a, name, {});
        };
        const plan = try curation.plan(self.a, try self.encode(stored.state), stored.redirects, stored.redirects, .{
            .action = switch (request.action) {
                .merge => .merge,
                .unmerge => .unmerge,
                .suppress => .suppress,
                .unsuppress => .unsuppress,
            },
            .from = if (request.action == .merge or request.action == .unmerge) request.from else null,
            .to = request.to,
            .keys = if (request.action == .suppress or request.action == .unsuppress) &.{request.from} else &.{},
            .replace = request.replace,
            .pin_distinct = request.pin_distinct,
        });
        const redirects = try curation.RedirectIndex.init(self.a, plan.merges);
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
        if (!try self.supportVisible(requested.entity.evidence, &.{}, 0)) return error.MemoryNotFound;
        const selected = (try self.canonical(id)) orelse return error.MemoryEntitySuppressed;
        const record = (try self.read(struct { entity: t.Entity, owner: []const u8 }, try key(self.a, "entity", selected))) orelse return error.MemoryNotFound;
        try self.authorize(record.owner, record.entity.visibility);
        if (!try self.supportVisible(record.entity.evidence, &.{}, 0)) return error.MemoryNotFound;
        return self.encode(.{ .entity = record.entity, .canonical_id = selected, .requested_id = id, .revision = self.state.revision, .identity = "explicit", .uncertainty = "Names and aliases alone do not prove entity identity" });
    }
    fn entity(self: *Run, value: t.Entity) !void {
        try t.validateId(value.id);
        try t.validateId(value.name);
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
        if (try self.read(struct { entity: t.Entity, owner: []const u8 }, path)) |old| {
            try self.authorize(old.owner, .private);
            if (!eql(u8, try self.encode(old.entity), try self.encode(value))) return error.MemoryEntityConflict;
            return;
        }
        // Identity is explicit. Names/aliases are discoverable candidates and
        // never constitute authority to merge two canonical IDs.
        try self.put(path, .{ .entity = value, .owner = self.service.context.principal });
    }
    fn putAssertion(self: *Run, value: t.StoredAssertion) !void {
        var record = value;
        record.recorded_at = self.service.context.now_ms;
        try self.put(try key(self.a, "assertion", record.assertion.id), record);
        try self.put(try historyKey(self.a, record.assertion.id, self.state.revision), record);
        try self.put(try groupKey(self.a, record.assertion, record.owner), .{ .id = record.assertion.id });
        try self.put(try dependencyKey(self.a, "identity", record.assertion.subject, record.assertion.id), .{ .child = record.assertion.id });
        if (self.objectTyped(record.assertion.predicate)) try self.put(try dependencyKey(self.a, "identity", record.assertion.value, record.assertion.id), .{ .child = record.assertion.id });
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
    fn authority(self: *Run, claim: t.Assertion) !u16 {
        var rank: u16 = 0;
        for (claim.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse continue;
            for (self.state.config.source_rules) |rule| if (eql(u8, rule.source_id, source.evidence.source_id)) {
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
        if (replace) record.superseded_by = next.assertion.id;
        if (record.lifecycle != .disputed) record.lifecycle = .superseded;
        if (record.assertion.valid_to == null or record.assertion.valid_to.? > next.assertion.valid_from.?) record.assertion.valid_to = next.assertion.valid_from;
    }
    fn reconcile(self: *Run, incoming: t.StoredAssertion) !void {
        const claim = incoming.assertion;
        if (try self.canonical(claim.subject) == null) return;
        if (self.objectTyped(claim.predicate) and try self.canonical(claim.value) == null) return;
        if (incoming.lifecycle != .active or incoming.stale or claim.modality != .actual or claim.role == .hypothesized or claim.stability != .settled or !self.singlePredicate(claim.predicate)) return;
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
            // Unknown business validity is never inferred from ingestion
            // order. Competing unknown/simultaneous assertions are disputed.
            var current = (try self.assertion(claim.id)).?;
            if (self.state.config.features.reconciliation == .deterministic and claim.valid_from != null and old.assertion.valid_from != null and claim.valid_from.? != old.assertion.valid_from.?) {
                const newer = claim.valid_from.? > old.assertion.valid_from.?;
                const incoming_authority = try self.authority(claim);
                const previous_authority = try self.authority(old.assertion);
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
        if (record.revision != request.expected_revision) return error.MemoryRevisionConflict;
        if (request.reason.len == 0 or request.reason.len > t.max_text_bytes) return error.InvalidMemoryRequest;
        for (request.evidence) |ref| {
            const source = (try self.evidence(ref.id)) orelse return error.MemoryEvidenceRequired;
            try self.authorize(source.owner, source.evidence.visibility);
            if (source.forgotten) return error.MemoryForgotten;
        }
        record.lifecycle = request.lifecycle;
        if (request.lifecycle == .active) {
            record.recorded_to = null;
            record.superseded_by = null;
            record.conflicts_with = &.{};
            record.assertion.valid_to = record.asserted_valid_to;
        }
        if (request.stability) |stability| record.assertion.stability = stability;
        record.reason = request.reason;
        record.revision = self.state.revision;
        if (request.lifecycle == .retracted) record.recorded_to = self.service.context.now_ms;
        record.explicit_disposition = explicitDisposition(record);
        try self.putAssertion(record);
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
        if (request.all) try self.put("forgotten-all", true);
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
            try self.remove(try key(self.a, "assertion", id));
            try self.put(try key(self.a, "forgotten-assertion", id), true);
            try self.invalidate("assertion", id);
        }
        // Cleanup is bounded and resumable. Recall checks the durable fences
        // immediately, before asynchronous physical cleanup has completed.
        try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
    }
    fn outcome(self: *Run, request: t.Outcome) !void {
        try t.validateId(request.id);
        try t.validateId(request.task_id);
        if (request.used_memory_ids.len > t.max_items) return error.MemoryInputTooLarge;
        for (request.used_memory_ids) |id| {
            const record = (try self.assertion(id)) orelse return error.MemoryNotFound;
            try self.authorize(record.owner, record.assertion.visibility);
            if (!try self.evidenceVisible(record)) return error.MemoryNotFound;
        }
        try self.put(try key(self.a, "outcome", request.id), .{ .outcome = request, .owner = self.service.context.principal, .recorded_at = self.service.context.now_ms });
    }
    fn advance(self: *Run, id: []const u8, receipt: *t.Operation) !void {
        if (id.len == 0) return self.maintenance();
        const path = try key(self.a, "operation", id);
        var pending = (try self.read(t.Operation, path)) orelse return error.MemoryNotFound;
        if (!eql(u8, pending.owner, self.service.context.principal) and !self.service.context.admin) return error.Forbidden;
        if (pending.phase == .succeeded or pending.phase == .cancelled) return;
        if (pending.phase == .failed) return error.MemoryRetryExhausted;
        if (pending.next_retry_at) |time| if (time > self.service.context.now_ms) return error.MemoryRetryNotReady;
        const caller = self.service.context.principal;
        self.service.context.principal = pending.owner;
        defer self.service.context.principal = caller;
        if (eql(u8, pending.action, "configure")) {
            self.reindex(path, &pending, receipt) catch |err| {
                if (err != error.MemoryExternalVectorRequired) pending.attempts += 1;
                try self.processingFailure(path, &pending, receipt, err);
                if (pending.phase == .failed) self.state.pending_config = null;
            };
            return;
        }
        if (pending.config_generation != self.state.config.generation) {
            pending.phase = .cancelled;
            pending.error_code = "MemoryConfigurationChanged";
            try self.put(path, pending);
            try self.remove(try key(self.a, "pending", pending.id));
            return;
        }
        if (pending.attempts >= self.state.config.max_attempts) return error.MemoryRetryExhausted;
        if (pending.next_retry_at) |time| if (time > self.service.context.now_ms) return error.MemoryRetryNotReady;
        var sources: std.ArrayList(t.StoredEvidence) = .empty;
        for (pending.inputs) |input| {
            const source = (try self.evidence(input)) orelse {
                pending.phase = .cancelled;
                pending.error_code = "MemoryForgotten";
                try self.put(path, pending);
                try self.remove(try key(self.a, "pending", pending.id));
                return;
            };
            try self.authorize(source.owner, source.evidence.visibility);
            if (source.forgotten or self.isExpired(source) or try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-source", source.evidence.source_id)) != null) {
                pending.phase = .cancelled;
                pending.error_code = "MemoryForgotten";
                try self.put(path, pending);
                try self.remove(try key(self.a, "pending", pending.id));
                return;
            }
            try sources.append(self.a, source);
        }
        for (pending.context_inputs) |input| {
            var source = (try self.evidence(input)) orelse return self.processingFailure(path, &pending, receipt, error.MemoryForgotten);
            try self.authorize(source.owner, source.evidence.visibility);
            if (source.forgotten or self.isExpired(source) or try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-source", source.evidence.source_id)) != null) return self.processingFailure(path, &pending, receipt, error.MemoryForgotten);
            source.context_only = true;
            try sources.append(self.a, source);
        }
        pending.attempts += 1;
        const generate = self.service.backend.generate orelse return error.MemoryCapabilityUnavailable;
        const generated = generate(self.service.backend.ptr, self.a, self.state.config, sources.items) catch |err| return self.processingFailure(path, &pending, receipt, err);
        self.publishGenerated(pending, generated) catch |err| return self.processingFailure(path, &pending, receipt, err);
        pending.phase = .succeeded;
        pending.assertions_ready = true;
        pending.error_code = null;
        pending.next_retry_at = null;
        try self.put(path, pending);
        try self.remove(try key(self.a, "pending", pending.id));
    }
    fn processingFailure(self: *Run, path: []const u8, pending: *t.Operation, receipt: *t.Operation, err: anyerror) !void {
        // A rejected model output publishes no partial assertions or entities.
        self.changes.clearRetainingCapacity();
        pending.phase = if (err == error.MemoryForgotten) .cancelled else if (pending.attempts >= self.state.config.max_attempts) .failed else .pending;
        pending.error_code = @errorName(err);
        pending.next_retry_at = self.service.context.now_ms +| (@as(i64, 1000) * (@as(i64, 1) << @intCast(@min(pending.attempts, 10))));
        try self.put(path, pending.*);
        if (pending.phase == .failed or pending.phase == .cancelled) try self.remove(try key(self.a, "pending", pending.id));
        receipt.phase = pending.phase;
        receipt.assertions_ready = false;
        receipt.error_code = pending.error_code;
    }
    fn checkGeneratedEvidence(self: *Run, pending: t.Operation, refs: []const t.EvidenceRef) !void {
        _ = self;
        for (refs) |ref| {
            var found = false;
            for (pending.inputs) |input| if (eql(u8, input, ref.id)) {
                found = true;
                break;
            };
            if (!found) for (pending.context_inputs) |input| if (eql(u8, input, ref.id)) {
                found = true;
                break;
            };
            if (!found) return error.MemoryUngroundedOutput;
        }
    }
    fn publishGenerated(self: *Run, pending: t.Operation, generated: t.Generated) !void {
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
        // Every support branch and entity must be grounded in admitted inputs.
        for (generated.assertions) |claim| {
            var current_support = false;
            for (claim.evidence) |ref| for (pending.inputs) |input| if (eql(u8, ref.id, input)) {
                current_support = true;
            };
            if (!current_support) return error.MemoryUngroundedOutput;
            try self.checkGeneratedEvidence(pending, claim.evidence);
            for (claim.support_sets) |support| try self.checkGeneratedEvidence(pending, support.evidence);
        }
        for (generated.entities) |value| try self.checkGeneratedEvidence(pending, value.evidence);
        for (generated.entities) |value| try self.entity(value);
        try self.assertions(generated.assertions, true);
    }
    fn stageEmbeddings(self: *Run, request: t.EmbeddingBatch) !void {
        try self.requireAdmin();
        const config = self.state.pending_config orelse return error.MemoryConfigurationChanged;
        if (request.generation != config.embedding_generation or config.models.embedder == null or !eql(u8, try self.encode(request.model), try self.encode(config.models.embedder.?))) return error.MemoryEmbeddingMismatch;
        if (request.items.len == 0 or request.items.len > 64) return error.InvalidMemoryRequest;
        for (request.items) |item| {
            var record = (try self.assertion(item.id)) orelse return error.MemoryNotFound;
            if (item.embedding.len != request.model.dimensions.?) return error.MemoryEmbeddingMismatch;
            for (item.embedding) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
            if (record.next_embedding_generation == self.state.config.embedding_generation) {
                record.assertion.embedding = record.next_embedding;
                record.embedding_generation = record.next_embedding_generation.?;
            }
            record.next_embedding = item.embedding;
            record.next_embedding_generation = request.generation;
            try self.put(try key(self.a, "assertion", item.id), record);
        }
    }
    fn reindex(self: *Run, path: []const u8, pending: *t.Operation, receipt: *t.Operation) !void {
        try self.requireAdmin();
        const config = self.state.pending_config orelse return error.MemoryConfigurationChanged;
        if (config.generation != pending.config_generation) return error.MemoryConfigurationChanged;
        const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion/", pending.cursor, 8);
        for (page.rows) |row| {
            var record = try parse(t.StoredAssertion, self.a, row.json);
            if (!try self.evidenceVisible(record)) continue;
            const external = eql(u8, config.models.embedder.?.provider, "external");
            if (external and record.next_embedding_generation != config.embedding_generation) return error.MemoryExternalVectorRequired;
            const embedding = if (external) record.next_embedding.? else try self.service.backend.embed.?(self.service.backend.ptr, self.a, config.models.embedder.?, record.assertion.statement);
            if (record.next_embedding_generation == self.state.config.embedding_generation) {
                record.assertion.embedding = record.next_embedding;
                record.embedding_generation = record.next_embedding_generation.?;
            }
            record.next_embedding = embedding;
            record.next_embedding_generation = config.embedding_generation;
            try self.put(row.key, record);
        }
        pending.cursor = page.next;
        pending.attempts = 0;
        pending.error_code = null;
        pending.next_retry_at = null;
        if (page.next == null) {
            if (config.retention_ms != self.state.config.retention_ms) self.state.next_expiry_at = self.service.context.now_ms;
            self.state.config = config;
            self.state.pending_config = null;
            pending.phase = .succeeded;
            pending.assertions_ready = true;
            try self.remove(try key(self.a, "pending", pending.id));
        }
        receipt.phase = pending.phase;
        receipt.assertions_ready = pending.assertions_ready;
        try self.put(path, pending.*);
    }
    fn maintenance(self: *Run) !void {
        if (!self.service.context.admin) return error.Forbidden;
        if (self.state.next_expiry_at) |time| if (time <= self.service.context.now_ms) {
            try self.put("cleanup", .{ .cursor = @as(?[]const u8, null) });
            self.state.next_expiry_at = null;
        };
        // One dependency page per transaction. Newly enqueued children form
        // a durable frontier; restart never loses invalidation debt.
        const work = try self.service.backend.scan(self.service.backend.ptr, self.a, "dependency/work/", null, 1);
        if (work.rows.len > 0) {
            const item = try parse(struct { kind: []const u8, id: []const u8, revision: u64, cursor: ?[]const u8 = null }, self.a, work.rows[0].json);
            const prefix = try std.fmt.allocPrint(self.a, "dependency/{s}/{s}/", .{ item.kind, try hash(self.a, item.id) });
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, prefix, item.cursor, 64);
            for (page.rows) |row| {
                const child = try parse(struct { child: []const u8 }, self.a, row.json);
                if (try self.assertion(child.child)) |value| {
                    var record = value;
                    if (!record.stale and (eql(u8, item.kind, "entity") or !try self.evidenceVisible(record))) {
                        record.stale = true;
                        record.revision = self.state.revision;
                        try self.putAssertion(record);
                        try self.invalidate("assertion", child.child);
                    }
                }
            }
            if (page.next) |cursor| try self.put(work.rows[0].key, .{ .kind = item.kind, .id = item.id, .revision = item.revision, .cursor = cursor }) else try self.remove(work.rows[0].key);
        }
        if (try self.read(struct { cursor: ?[]const u8 = null }, "cleanup")) |cleanup| {
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "", cleanup.cursor, 64);
            for (page.rows) |row| {
                if (std.mem.startsWith(u8, row.key, "evidence/")) {
                    const source = try parse(t.StoredEvidence, self.a, row.json);
                    const expires = self.evidenceExpiry(source);
                    if (expires) |time| if (time > self.service.context.now_ms and (self.state.next_expiry_at == null or time < self.state.next_expiry_at.?)) {
                        self.state.next_expiry_at = time;
                    };
                    if (try self.read(bool, "forgotten-all") != null or try self.read(bool, try key(self.a, "forgotten-source", source.evidence.source_id)) != null or self.isExpired(source)) {
                        try self.remove(row.key);
                        try self.put(try key(self.a, "forgotten-evidence", source.evidence.id), true);
                    }
                } else if (std.mem.startsWith(u8, row.key, "entity/")) {
                    const stored = try parse(struct { entity: t.Entity, owner: []const u8 }, self.a, row.json);
                    if (try self.read(bool, "forgotten-all") != null or !try self.supportVisible(stored.entity.evidence, &.{}, 0)) try self.remove(row.key);
                } else if (std.mem.startsWith(u8, row.key, "outcome/")) {
                    const stored = try parse(struct { outcome: t.Outcome, owner: []const u8, recorded_at: i64 }, self.a, row.json);
                    var remove_outcome = try self.read(bool, "forgotten-all") != null;
                    for (stored.outcome.used_memory_ids) |used| {
                        const parent = try self.assertion(used);
                        if (parent == null or !try self.evidenceVisible(parent.?)) remove_outcome = true;
                    }
                    if (remove_outcome) try self.remove(row.key);
                } else if (std.mem.startsWith(u8, row.key, "assertion/") or std.mem.startsWith(u8, row.key, "history/")) {
                    const record = try parse(t.StoredAssertion, self.a, row.json);
                    if (!try self.retained(record, 0)) try self.remove(row.key);
                }
            }
            if (page.next) |cursor| try self.put("cleanup", .{ .cursor = cursor }) else try self.remove("cleanup");
        }
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
            try records.append(self.a, value);
        }
        return self.encode(.{ .items = records.items, .next_cursor = try self.encodeCursor(prefix, page.next), .complete = page.next == null, .revision = self.state.revision });
    }
    fn enumerate(self: *Run, request: t.Recall) ![]const u8 {
        if (request.limit == 0 or request.limit > t.max_items or request.query.len != 0 or request.embedding != null) return error.InvalidMemoryRequest;
        var items: std.ArrayList(t.StoredAssertion) = .empty;
        var after = try self.decodeCursor("assertion/", request.cursor);
        var visited: usize = 0;
        var more = true;
        while (items.items.len < request.limit and visited < 4096) {
            const page = try self.service.backend.scan(self.service.backend.ptr, self.a, "assertion/", after, 1);
            if (page.rows.len == 0) {
                more = false;
                break;
            }
            const row = page.rows[0];
            after = row.key;
            visited += 1;
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
            try items.append(self.a, record);
            if (page.next == null) {
                more = false;
                break;
            }
        }
        return self.encode(.{ .items = items.items, .next_cursor = if (more) try self.encodeCursor("assertion/", after) else null, .complete = !more, .revision = self.state.revision });
    }
    fn recall(self: *Run, request: t.Recall) ![]const u8 {
        if (request.limit == 0 or request.limit > t.max_items or request.token_budget == 0 or request.token_budget > self.state.config.max_recall_tokens) return error.InvalidMemoryRequest;
        const Hit = t.RecallHit;
        var hits: std.ArrayList(Hit) = .empty;
        var after = try self.decodeCursor("assertion/", request.cursor);
        var scanned: usize = 0;
        var vector = request.embedding;
        if (vector) |supplied| {
            const model = self.state.config.models.embedder orelse return error.MemoryModelRequired;
            if (request.embedding_model == null or !eql(u8, try self.encode(request.embedding_model.?), try self.encode(model)) or supplied.len != model.dimensions.?) return error.MemoryEmbeddingMismatch;
            for (supplied) |value| if (!std.math.isFinite(value)) return error.MemoryEmbeddingMismatch;
        }
        if (self.state.config.features.semantic_recall and vector == null and request.query.len > 0) vector = try self.service.backend.embed.?(self.service.backend.ptr, self.a, self.state.config.models.embedder.?, request.query);
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
                const compatible_embedding = if (record.next_embedding_generation == self.state.config.embedding_generation) record.next_embedding else if (record.embedding_generation == self.state.config.embedding_generation) claim.embedding else null;
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
                record.next_embedding = null;
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
            const stored = try parse(struct { entity: t.Entity, owner: []const u8 }, self.a, row.json);
            self.authorize(stored.owner, stored.entity.visibility) catch continue;
            if (try self.read(bool, "forgotten-all") != null or !try self.supportVisible(stored.entity.evidence, &.{}, 0)) continue;
            try values.append(self.a, try parse(std.json.Value, self.a, row.json));
        }
        return self.encode(.{ .items = values.items, .next_cursor = try self.encodeCursor("entity/", page.next), .complete = page.next == null });
    }
};

fn explicitDisposition(record: t.StoredAssertion) t.ExplicitDisposition {
    return .{ .lifecycle = record.lifecycle, .valid_to = record.assertion.valid_to, .superseded_by = record.superseded_by, .recorded_to = record.recorded_to, .conflicts_with = record.conflicts_with, .reason = record.reason };
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
