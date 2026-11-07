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

//! Metadata owns durable SQL population and empty-generation jobs. Population
//! keeps its one producer evaluation; TRUNCATE has none. Every control binds
//! the exact source kind, admitted job and immutable staging plan.
const std = @import("std");
const jobs = @import("../api/restore_jobs.zig");
const stages = @import("restore_staging.zig");
const operation = @import("antfly_local_sources").api_operation;
const A = std.mem.Allocator;
pub const max_command_bytes = 2 * stages.max_encoded_bytes + 64 * 1024;
pub const max_response_bytes = 512 * 1024;

pub const Reference = struct {
    job_id: u64,
    namespace: []const u8,
    key: []const u8,
    fingerprint: []const u8,
    plan_digest: [32]u8,

    pub fn validate(self: Reference, a: A) !void {
        if (self.namespace.len == 0 or self.namespace.len > 256 or self.key.len == 0 or self.key.len > 256 or
            self.fingerprint.len != 64 or std.mem.allEqual(u8, &self.plan_digest, 0)) return error.InvalidGenerationPublication;
        if (self.job_id != try jobs.jobIdForIdempotency(a, self.namespace, self.key)) return error.InvalidGenerationPublication;
    }
    pub fn requireState(self: Reference, a: A, state: jobs.JobState) !void {
        try self.validate(a);
        const expected_kind: jobs.SourceKind = if (std.mem.startsWith(u8, self.namespace, "sql-truncate:")) .empty_generation else .query_population;
        if (expected_kind == .empty_generation) {
            const id = try stages.idForAttempt(self.job_id, 1);
            const location = try std.fmt.allocPrint(a, "metadata://empty-generation/{s}", .{std.fmt.bytesToHex(id, .lower)});
            defer a.free(location);
            if (!std.mem.eql(u8, location, state.location)) return error.IdempotencyConflict;
        } else if (!std.mem.eql(u8, state.location, "metadata://query-population")) return error.IdempotencyConflict;
        if (state.source_kind != expected_kind or state.scope != .cluster or state.job_id != self.job_id or
            !std.mem.eql(u8, self.namespace, state.idempotency_namespace) or !std.mem.eql(u8, self.key, state.idempotency_key) or
            !std.mem.eql(u8, self.fingerprint, state.request_fingerprint)) return error.IdempotencyConflict;
    }
};
pub const LostNative = struct {
    scope: @import("sql_relation_attestation.zig").CreationScope,
    table_id: u64,
    expected_boot: @import("sql_relation_boots.zig").Record,

    pub fn requireObserved(self: LostNative, target: stages.Target, observed: @import("sql_relation_boots.zig").Observation) !void {
        const native = target.table.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        try native.validate();
        if (target.table.table_id != self.table_id or target.ranges.len != 1 or target.native_reset != null or target.rewrite != null or target.schema_only_snapshot != null or target.source_artifacts.len != 0) return error.RestoreStagingScopeChanged;
        const boot = observed.record orelse return error.SqlRelationOwnerChanged;
        try boot.validate();
        if (!observed.registration_current or !std.meta.eql(boot, self.expected_boot) or boot.node_id != native.owner_node_id or boot.identity.store_id != native.owner_store_id) return error.SqlRelationOwnerChanged;
        const lost_image = switch (native.kind) {
            .permanent => false,
            .temporary => !std.mem.eql(u8, &native.generation_instance, &boot.instance),
            .unlogged => if (observed.initialized) |initialized| boot.unclean_restart and !std.mem.eql(u8, &initialized.binding.native_instance, &boot.instance) else false,
        };
        if (!lost_image) return error.SqlRelationOwnerChanged;
        if (observed.initialized) |initialized| {
            try initialized.verify();
            if (!initialized.binding.descriptor.eql(native) or initialized.binding.table_id != self.table_id or !std.meta.eql(initialized.creation_scope, @as(?@import("sql_relation_attestation.zig").CreationScope, self.scope))) return error.SqlRelationOwnerChanged;
        }
    }
};
pub const NativeRecovery = struct {
    entry: @import("sql_relation_lifetimes.zig").Entry,
    boot: @import("sql_relation_boots.zig").Record,
    pub fn validate(self: NativeRecovery) !void {
        try self.entry.validate();
        try self.boot.validate();
        if (self.boot.phase != .ready and self.boot.phase != .recovering) return error.SqlRelationOwnerChanged;
        if (self.entry.descriptor.owner_node_id != self.boot.node_id or self.entry.descriptor.owner_store_id != self.boot.identity.store_id) return error.SqlRelationOwnerChanged;
        switch (self.entry.descriptor.kind) {
            .permanent => return error.InvalidSqlRelationPersistence,
            .temporary => if (std.mem.eql(u8, &self.entry.descriptor.generation_instance, &self.boot.instance)) return error.SqlRelationOwnerChanged,
            .unlogged => {
                const before = self.entry.initialized orelse return error.SqlRelationRecoveryIncomplete;
                if (self.boot.phase != .recovering or !self.boot.unclean_restart or self.boot.generation <= before.boot_generation or std.mem.eql(u8, &before.binding.native_instance, &self.boot.instance)) return error.SqlRelationOwnerChanged;
            },
        }
    }
};
pub const NativeRecoveryResult = struct { phase: enum { pending, retired } };

pub const Request = union(enum) {
    admit: jobs.StartRequest,
    observe: Reference,
    cancel: Reference,
    cancel_lost_native: LostNative,
    recover_native_entry: NativeRecovery,
    pub fn isMutation(self: Request) bool {
        return self != .observe;
    }
    pub fn validate(self: Request, a: A) !void {
        switch (self) {
            .admit => |req| {
                if (req.scope != .cluster or req.table_name != null or req.rewrite_plan_json != null or
                    req.generation_plan_json == null or req.idempotency_key == null or req.table_names == null or
                    req.generation_plan_json.?.len > stages.max_encoded_bytes or !std.mem.eql(u8, req.connection, "internal")) return error.InvalidGenerationPublication;
                switch (req.source_kind) {
                    .query_population => if (req.table_names.?.len != 1 or !std.mem.eql(u8, req.location, "metadata://query-population") or std.mem.startsWith(u8, req.idempotency_namespace, "sql-truncate:")) return error.InvalidGenerationPublication,
                    .empty_generation => try validateTruncateAdmission(a, req),
                    else => return error.InvalidGenerationPublication,
                }
            },
            .observe, .cancel => |ref| try ref.validate(a),
            .recover_native_entry => |recovery| try recovery.validate(),
            .cancel_lost_native => |lost| {
                try lost.scope.validate();
                try lost.expected_boot.validate();
                if (lost.table_id == 0 or (lost.expected_boot.phase != .ready and lost.expected_boot.phase != .recovering)) return error.InvalidGenerationPublication;
            },
        }
    }
};

fn validateTruncateAdmission(backing: A, req: jobs.StartRequest) !void {
    if (req.table_names.?.len == 0 or req.table_names.?.len > 128 or req.idempotency_namespace.len > 256 or req.idempotency_key.?.len == 0 or req.idempotency_key.?.len > 256 or
        req.destination_authorization_principal.len == 0 or !std.mem.eql(u8, req.restore_mode, "overwrite") or
        !std.mem.startsWith(u8, req.idempotency_namespace, "sql-truncate:") or req.routine_mappings != null) return error.InvalidGenerationPublication;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const expected_namespace = try std.fmt.allocPrint(a, "sql-truncate:{s}", .{req.destination_authorization_principal});
    if (!std.mem.eql(u8, expected_namespace, req.idempotency_namespace)) return error.InvalidGenerationPublication;
    const plan = std.json.parseFromSliceLeaky(stages.Plan, a, req.generation_plan_json.?, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidGenerationPublication;
    plan.validate(a) catch |err| return if (err == error.OutOfMemory) err else error.InvalidGenerationPublication;
    const id = try stages.idForAttempt(try jobs.jobIdForIdempotency(a, req.idempotency_namespace, req.idempotency_key.?), 1);
    const expected_location = try std.fmt.allocPrint(a, "metadata://empty-generation/{s}", .{std.fmt.bytesToHex(id, .lower)});
    if (!std.mem.eql(u8, &plan.id, &id) or !std.mem.eql(u8, expected_location, req.location) or
        !std.mem.eql(u8, &std.fmt.bytesToHex(plan.cohort_digest, .lower), req.backup_id) or plan.preparing_sources or
        plan.type_retirement != null or plan.constraint_name_guard != null or plan.targets.len != req.table_names.?.len) return error.InvalidGenerationPublication;
    for (plan.targets, req.table_names.?) |target, name| {
        if (!target.empty_generation or target.replace == null or target.source_table_id != target.replace.?.table.table_id or
            target.native_reset != null or target.schema_only_snapshot != null or target.population != null or target.rewrite != null or
            target.materialized_view != null or target.source_artifacts.len != 0 or !std.mem.eql(u8, target.table.name, name)) return error.InvalidGenerationPublication;
    }
}

/// Shared durable dispatch: standalone/metadata callers use the existing Store;
/// data/API callers route the same typed StartRequest to the metadata owner.
/// Parsing or delivery failure after proposal remains an unknown admission.
pub fn admit(server: anytype, a: A, context: operation.RequestContext, req: jobs.StartRequest) !Admission {
    if (!context.setting_admin) return error.Forbidden;
    try (Request{ .admit = req }).validate(a);
    const raw = (if (server.cfg.deployment_mode == .standalone or server.restore_job_store.replicated != null)
        server.executeQueryPopulationJob(a, context, .{ .admit = req })
    else
        server.source.systemCatalog(a, context, .{ .query_population_job = .{ .admit = req } })) catch |err| {
        if (err == error.MetadataMutationOutcomeUnknown) context.markSQLSubmission();
        return err;
    };
    // Even malformed accepted bytes cannot be classified as unsubmitted.
    context.markSQLSubmission();
    defer a.free(raw);
    var parsed = std.json.parseFromSlice(Admission, a, raw, .{}) catch return error.MetadataMutationOutcomeUnknown;
    defer parsed.deinit();
    return .{ .result = switch (parsed.value.result) {
        .accepted => |bytes| .{ .accepted = a.dupe(u8, bytes) catch return error.MetadataMutationOutcomeUnknown },
        .unknown => |bytes| .{ .unknown = a.dupe(u8, bytes) catch return error.MetadataMutationOutcomeUnknown },
    }, .plan_digest = parsed.value.plan_digest };
}

/// Only a receiver's explicit not-proposed witness may expose these errors.
/// A dropped response or a post-proposal error remains an unknown admission.
pub fn definiteHttpFailure(status: u16, body: []const u8) ?anyerror {
    const domain = @import("antfly_local_sources").system_catalog_domain;
    inline for (.{ error.IdempotencyConflict, error.InvalidGenerationPublication, error.Forbidden, error.NotFound, error.AsyncRestoreUnavailable, error.NotLeader, error.TableTopologyProtocolUpgradeRequired, error.RelationalTopologyProtocolUpgradeRequired }) |err| {
        if (status == domain.httpStatus(err) and std.mem.eql(u8, body, @errorName(err))) return err;
    }
    return null;
}
/// The producer is not necessarily a replica of the target. Admission returns
/// the persisted plan commitment through administrative job authority; it must
/// not borrow a target-owner placement grant just to prove its own evaluation.
pub const Admission = struct {
    result: jobs.Store.Admission,
    plan_digest: ?stages.Digest = null,
};

/// Takes ownership of result; failures after durable admission remain unknown.
pub fn bindAdmission(a: A, source: anytype, context: operation.RequestContext, result: jobs.Store.Admission) !Admission {
    errdefer switch (result) {
        inline else => |bytes| a.free(bytes),
    };
    if (result == .unknown) return .{ .result = result };
    var state = std.json.parseFromSlice(jobs.JobState, a, result.accepted, .{ .ignore_unknown_fields = true }) catch return error.MetadataMutationOutcomeUnknown;
    defer state.deinit();
    if (state.value.phase == .failed) return .{ .result = result };
    const id = stages.idForAttempt(state.value.job_id, state.value.staging_attempt_id) catch return error.MetadataMutationOutcomeUnknown;
    const bytes = (source.getRestoreStaging(a, id, context) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    defer a.free(bytes);
    if (@import("schema_reader_wire.zig").requiresTruncatePreparation(bytes)) {
        var retained = requireRetainedTruncateJob(a, bytes, id) catch return error.MetadataMutationOutcomeUnknown;
        defer retained.deinit();
        const projected = jobs.projectTruncatePreparationResultAlloc(a, state.value.result_json, retained.value.plan.truncate_preparation.?, retained.value.plan_digest, false, false) catch return error.MetadataMutationOutcomeUnknown;
        defer a.free(projected);
        state.value.result_json = projected;
        const accepted = std.json.Stringify.valueAlloc(a, state.value, .{ .emit_null_optional_fields = false }) catch return error.MetadataMutationOutcomeUnknown;
        a.free(result.accepted);
        return .{ .result = .{ .accepted = accepted }, .plan_digest = retained.value.plan_digest };
    }
    var header = std.json.parseFromSlice(struct { plan_digest: stages.Digest, plan: struct { id: stages.Id } }, a, bytes, .{ .ignore_unknown_fields = true }) catch return error.MetadataMutationOutcomeUnknown;
    defer header.deinit();
    if (!std.mem.eql(u8, &header.value.plan.id, &id) or std.mem.allEqual(u8, &header.value.plan_digest, 0)) return error.MetadataMutationOutcomeUnknown;
    return .{ .result = result, .plan_digest = header.value.plan_digest };
}

/// Join only the immutable SQL TRUNCATE job, never execute a trigger on a
/// worker or status reader. Ordinary jobs require no staging-plan lookup.
pub fn projectRetainedTruncateResult(a: A, source: anytype, context: operation.RequestContext, state: jobs.JobState, base_result: ?[]const u8, include_notices: bool) !?[]u8 {
    if (state.source_kind != .empty_generation or !std.mem.startsWith(u8, state.idempotency_namespace, "sql-truncate:")) return null;
    const id = try stages.idForAttempt(state.job_id, state.staging_attempt_id);
    const bytes = (try source.getRestoreStaging(a, id, context)) orelse {
        // A rejected/canceled job may precede staging reservation. Preserve its
        // authored failure without granting notice or publication authority.
        const has_reference = if (state.result_json) |result| @import("schema_reader_wire.zig").requiresTruncatePreparation(result) else false;
        if (!has_reference and (state.phase == .failed or state.phase == .cancelled)) return null;
        return error.InvalidGenerationPublication;
    };
    defer a.free(bytes);
    if (!@import("schema_reader_wire.zig").requiresTruncatePreparation(bytes)) return null;
    var retained = try requireRetainedTruncateJob(a, bytes, id);
    defer retained.deinit();
    const reference: Reference = .{ .job_id = state.job_id, .namespace = state.idempotency_namespace, .key = state.idempotency_key, .fingerprint = state.request_fingerprint, .plan_digest = retained.value.plan_digest };
    try reference.requireState(a, state);
    var release_after = false;
    if (include_notices and retained.value.state == .published) {
        const progress = (try source.getRestoreStagingProgress(a, id, context)) orelse return error.InvalidGenerationPublication;
        if (progress.state != .published or progress.revision != retained.value.revision or progress.completed_owners != retained.value.completed_owners) return error.InvalidGenerationPublication;
        release_after = true;
    }
    return try jobs.projectTruncatePreparationResultAlloc(a, base_result, retained.value.plan.truncate_preparation.?, retained.value.plan_digest, include_notices, release_after);
}

fn requireRetainedTruncateJob(a: A, bytes: []const u8, id: stages.Id) !std.json.Parsed(stages.Job) {
    if (bytes.len > stages.max_encoded_bytes) return error.InvalidGenerationPublication;
    var retained = try std.json.parseFromSlice(stages.Job, a, bytes, .{});
    errdefer retained.deinit();
    if (!std.mem.eql(u8, &retained.value.plan.id, &id) or retained.value.plan.truncate_preparation == null) return error.InvalidGenerationPublication;
    var validation: std.heap.ArenaAllocator = .init(a);
    defer validation.deinit();
    try retained.value.plan.validate(validation.allocator());
    const digest = try retained.value.plan.digest(validation.allocator());
    if (!std.mem.eql(u8, &digest, &retained.value.plan_digest)) return error.InvalidGenerationPublication;
    return retained;
}

pub const Port = struct {
    ptr: *anyopaque,
    execute: *const fn (*anyopaque, A, operation.RequestContext, Request) anyerror![]u8,
};
pub const Input = struct { alloc: A, context: operation.RequestContext, request: Request };

test "SQL scope retirement population controls bind exact job custody and keep observations read only" {
    const a = std.testing.allocator;
    const Call = @import("../system_catalog/server_call.zig").Call;
    const ref: Reference = .{ .job_id = try jobs.jobIdForIdempotency(a, "population-test", "capture-1"), .namespace = "population-test", .key = "capture-1", .fingerprint = &@as([64]u8, @splat('a')), .plan_digest = @splat(2) };
    try ref.validate(a);
    for ([_]Request{ .{ .observe = ref }, .{ .cancel = ref } }) |request| {
        const call: Call = .{ .query_population_job = request };
        try std.testing.expect(call.requiresAdministrativeGrant());
        try std.testing.expect(call.requiresSettingAuthorityReadGrant());
        try std.testing.expectEqual(request != .observe, call.isMutation());
    }
    var changed = ref;
    changed.key = "capture-2";
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    changed = ref;
    changed.plan_digest = @splat(0);
    try std.testing.expectError(error.InvalidGenerationPublication, changed.validate(a));
    const state: jobs.JobState = .{
        .format_version = 1,
        .job_id = ref.job_id,
        .enqueue_sequence = 1,
        .dispatch_sequence = 1,
        .scope = .cluster,
        .source_kind = .query_population,
        .backup_id = "definition",
        .location = "metadata://query-population",
        .connection = "internal",
        .idempotency_namespace = ref.namespace,
        .idempotency_key = ref.key,
        .request_fingerprint = ref.fingerprint,
        .created_at_ms = 1,
        .updated_at_ms = 1,
        .expires_at_ms = 100,
    };
    try ref.requireState(a, state);
    var other = state;
    other.source_kind = .table_snapshot;
    try std.testing.expectError(error.IdempotencyConflict, ref.requireState(a, other));
    other = state;
    other.request_fingerprint = &@as([64]u8, @splat('b'));
    try std.testing.expectError(error.IdempotencyConflict, ref.requireState(a, other));
}

test "SQL native population cancellation rejoins exact lost image and refuses healthy or superseded owners" {
    const local = @import("antfly_local_sources");
    const persistence = local.common_topology_records.RelationPersistence;
    const attest = @import("sql_relation_attestation.zig");
    const boots = @import("sql_relation_boots.zig");
    const seed: [32]u8 = @splat(7);
    const keys = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .root_incarnation = 11, .seed = seed, .public_key = keys.public_key.toBytes() };
    const descriptor: persistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 1 };
    const scope: attest.CreationScope = .{ .plan_id = @splat(1), .plan_digest = @splat(2) };
    const identity: @import("store_root_enrollment.zig").Identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = 11, .public_key = root.public_key };
    const signed = try attest.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .boot_generation = 1, .identity = identity, .reporter_incarnation = 1, .creation_scope = scope, .binding = .{ .descriptor = descriptor, .native_instance = @splat(1), .table_id = 201, .shard_id = 202, .range_id = 202, .physical_root_incarnation = 303, .schema_version = 1, .schema_digest = @splat(4), .catalog_generation = 1, .catalog_fingerprint = @splat(5) } }, root);
    const boot: boots.Record = .{ .node_id = 9, .identity = identity, .reporter_incarnation = 2, .generation = 2, .instance = @splat(2), .claim_digest = @splat(6), .unclean_restart = true, .phase = .recovering };
    const target: stages.Target = .{ .source_table_id = 0, .table = .{ .table_id = 201, .name = "population", .sql_persistence = descriptor }, .ranges = &.{.{ .table_id = 201, .group_id = 202, .range_id = 202, .doc_identity_shard_id = 202, .doc_identity_range_id = 202, .start_key = "" }} };
    const lost: LostNative = .{ .scope = scope, .table_id = 201, .expected_boot = boot };
    var observed: boots.Observation = .{ .record = boot, .registration_current = true, .initialized = signed };
    try lost.requireObserved(target, observed);
    var recovery: NativeRecovery = .{ .entry = .{ .table_id = 201, .descriptor = descriptor, .initialized = signed }, .boot = boot };
    try recovery.validate();
    const request: Request = .{ .recover_native_entry = recovery };
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, request, .{});
    defer std.testing.allocator.free(encoded);
    var roundtrip = try std.json.parseFromSlice(Request, std.testing.allocator, encoded, .{});
    defer roundtrip.deinit();
    try roundtrip.value.validate(std.testing.allocator);
    try std.testing.expectEqualDeep(request, roundtrip.value);
    const recovery_call: @import("../system_catalog/server_call.zig").Call = .{ .query_population_job = .{ .recover_native_entry = recovery } };
    try std.testing.expect(recovery_call.requiresAdministrativeGrant());
    recovery.boot.unclean_restart = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, recovery.validate());
    recovery.boot = boot;
    recovery.boot.instance = signed.binding.native_instance;
    try std.testing.expectError(error.SqlRelationOwnerChanged, recovery.validate());
    observed.registration_current = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, lost.requireObserved(target, observed));
    observed.registration_current = true;
    observed.record.?.instance = @splat(3);
    try std.testing.expectError(error.SqlRelationOwnerChanged, lost.requireObserved(target, observed));
    observed.record = boot;
    observed.initialized = null;
    try std.testing.expectError(error.SqlRelationOwnerChanged, lost.requireObserved(target, observed));
    observed.initialized = signed;
    var clean = lost;
    clean.expected_boot.unclean_restart = false;
    observed.record = clean.expected_boot;
    try std.testing.expectError(error.SqlRelationOwnerChanged, clean.requireObserved(target, observed));
    observed.record = boot;
    var wrong_scope = lost;
    wrong_scope.scope.plan_digest[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, wrong_scope.requireObserved(target, observed));
    var another = target;
    another.table.table_id += 1;
    try std.testing.expectError(error.RestoreStagingScopeChanged, lost.requireObserved(another, observed));
    const call: @import("../system_catalog/server_call.zig").Call = .{ .query_population_job = .{ .cancel_lost_native = lost } };
    try std.testing.expect(call.requiresAdministrativeGrant());
}

pub fn checkTruncateTransport(alloc: A) !void {
    var region = std.heap.ArenaAllocator.init(alloc);
    defer region.deinit();
    const a = region.allocator();
    const namespace = "sql-truncate:basic:owner";
    const key = "truncate-cohort-1";
    const job_id = try jobs.jobIdForIdempotency(a, namespace, key);
    const id = try stages.idForAttempt(job_id, 1);
    const records = @import("antfly_local_sources").common_topology_records;
    const topology = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
    var targets: [2]stages.Target = undefined;
    for (&targets, 0..) |*target, i| {
        const offset: u64 = @intCast(i);
        const name = if (i == 0) "first" else "second";
        const before: records.TableRecord = .{ .table_id = 9 + offset, .name = name, .schema_json = "{}" };
        const old_range: records.RangeRecord = .{ .table_id = before.table_id, .group_id = 301 + offset, .range_id = 301 + offset, .start_key = "" };
        const fence: topology.Fence = .{ .role = .rewrite_source, .transition_id = job_id, .attempt = 1, .owner_group_id = old_range.group_id, .peer_group_id = 401 + offset, .namespace = .{ .table_id = before.table_id, .shard_id = old_range.group_id, .range_id = old_range.group_id }, .catalog_digest = @splat(4) };
        const handoff: stages.GenerationHandoffRange = .{ .source_group_id = old_range.group_id, .target_group_id = 401 + offset, .source_namespace = fence.namespace, .admissions = &.{}, .admissions_digest = try @import("antfly_local_sources").storage_portable_backup.sourceGenerationAdmissionSummaryDigest(fence.namespace, &.{}), .retired_digest = @splat(8), .retired_count = 0 };
        const old_ranges = try a.dupe(records.RangeRecord, &.{old_range});
        const fences = try a.dupe(topology.Fence, &.{fence});
        const handoffs = try a.dupe(stages.GenerationHandoffRange, &.{handoff});
        const ranges = try a.dupe(records.RangeRecord, &.{.{ .table_id = 19 + offset, .group_id = 401 + offset, .range_id = 401 + offset, .start_key = "", .doc_identity_shard_id = 401 + offset, .doc_identity_range_id = 401 + offset }});
        target.* = .{ .source_table_id = before.table_id, .empty_generation = true, .table = .{ .table_id = 19 + offset, .name = name, .schema_json = "{}" }, .ranges = ranges, .generation_handoffs = handoffs, .replace = .{ .table = before, .ranges = old_ranges, .fences = fences } };
    }
    const plan: stages.Plan = .{ .id = id, .cohort_digest = @splat(3), .targets = &targets };
    const location = try std.fmt.allocPrint(a, "metadata://empty-generation/{s}", .{std.fmt.bytesToHex(id, .lower)});
    const start: jobs.StartRequest = .{ .scope = .cluster, .source_kind = .empty_generation, .backup_id = &std.fmt.bytesToHex(plan.cohort_digest, .lower), .location = location, .connection = "internal", .restore_mode = "overwrite", .table_names = &.{ "first", "second" }, .idempotency_namespace = namespace, .idempotency_key = key, .destination_authorization_principal = "basic:owner", .generation_plan_json = try std.json.Stringify.valueAlloc(a, plan, .{}) };
    try (Request{ .admit = start }).validate(a);
    var wrong = start;
    wrong.source_kind = .query_population;
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    wrong = start;
    wrong.table_names = &.{"first"};
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    wrong = start;
    wrong.table_names = &.{ "second", "first" };
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    wrong = start;
    wrong.idempotency_key = "another-admission";
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    wrong = start;
    wrong.destination_authorization_principal = "basic:other";
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    wrong = start;
    wrong.location = "metadata://query-population";
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    var changed_plan = plan;
    changed_plan.preparing_sources = true;
    wrong = start;
    wrong.generation_plan_json = try std.json.Stringify.valueAlloc(a, changed_plan, .{});
    try std.testing.expectError(error.InvalidGenerationPublication, (Request{ .admit = wrong }).validate(a));
    const fingerprint = try jobs.requestFingerprintAlloc(a, start);
    const ref: Reference = .{ .job_id = job_id, .namespace = namespace, .key = key, .fingerprint = fingerprint, .plan_digest = try plan.digest(a) };
    const state: jobs.JobState = .{ .format_version = 1, .job_id = job_id, .enqueue_sequence = 1, .dispatch_sequence = 1, .scope = .cluster, .source_kind = .empty_generation, .backup_id = start.backup_id, .location = location, .connection = "internal", .idempotency_namespace = namespace, .idempotency_key = key, .request_fingerprint = fingerprint, .created_at_ms = 1, .updated_at_ms = 1, .expires_at_ms = 100 };
    try ref.requireState(a, state);
    var other = state;
    other.source_kind = .query_population;
    try std.testing.expectError(error.IdempotencyConflict, ref.requireState(a, other));
    other = state;
    other.location = "metadata://query-population";
    try std.testing.expectError(error.IdempotencyConflict, ref.requireState(a, other));
    const Call = @import("../system_catalog/server_call.zig").Call;
    const Fake = struct {
        cfg: struct { deployment_mode: enum { standalone, data } = .data } = .{},
        restore_job_store: struct { replicated: ?void = null } = .{},
        source: struct {
            calls: usize = 0,
            response: []const u8,
            lost_reply: bool = false,
            fn systemCatalog(self: *@This(), backing: A, context: operation.RequestContext, call: Call) ![]u8 {
                try std.testing.expect(call == .query_population_job and call.query_population_job == .admit);
                try std.testing.expectEqual(jobs.SourceKind.empty_generation, call.query_population_job.admit.source_kind);
                try call.query_population_job.validate(backing);
                self.calls += 1;
                context.markSQLSubmission();
                if (self.lost_reply) return error.MetadataMutationOutcomeUnknown;
                return backing.dupe(u8, self.response);
            }
            fn getRestoreStaging(_: @This(), _: A, _: [16]u8, _: operation.RequestContext) !?[]u8 {
                return error.LocalRestoreStoreWasUsed;
            }
        },
        fn executeQueryPopulationJob(_: *@This(), _: A, _: operation.RequestContext, _: Request) ![]u8 {
            return error.LocalRestoreStoreWasUsed;
        }
    };
    const encoded_state = try std.json.Stringify.valueAlloc(a, state, .{});
    var fake: Fake = .{ .source = .{ .response = try std.json.Stringify.valueAlloc(a, Admission{ .result = .{ .accepted = encoded_state }, .plan_digest = ref.plan_digest }, .{}) } };
    var submitted = false;
    const context: operation.RequestContext = .{ .setting_admin = true, .sql_completion_submission = &submitted };
    const accepted = try admit(&fake, a, context, start);
    try std.testing.expect(accepted.result == .accepted and submitted);
    try std.testing.expectEqual(@as(usize, 1), fake.source.calls);
    try std.testing.expectEqualSlices(u8, &ref.plan_digest, &accepted.plan_digest.?);
    fake.source.lost_reply = true;
    submitted = false;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, admit(&fake, a, context, start));
    try std.testing.expect(submitted);
    try std.testing.expectEqual(@as(usize, 2), fake.source.calls);
    // The same source cannot be relabeled or automatically resubmitted when
    // its admission response is missing or malformed.
    fake.source.lost_reply = false;
    fake.source.response = "{invalid";
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, admit(&fake, a, context, start));
    try std.testing.expectEqual(@as(usize, 3), fake.source.calls);
}
