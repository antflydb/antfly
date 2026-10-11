// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded native owner execution for the authenticated server receiver.
//! Permission comes from the receiver's configured metadata transport, never
//! from a decision, schema, physical binding or logical digest in an RPC body.
//! Results remain unsigned: root attestation and lifecycle cancellation must
//! be integrated before exposing a publication producer.
const std = @import("std");
const local = @import("antfly_local_sources");
const publication = @import("../metadata/relational_index_publication.zig");
const calls = @import("../system_catalog/server_call.zig");
const native = local.storage_db_relational_index_owner_control;
const jobs = local.storage_db_relational_index_owner_job;
const operation = local.api_operation;

pub const Request = struct {
    table_id: u64,
    group_id: u64,
    id: [16]u8,
    operation: enum { fence, reserve },

    fn lookup(self: Request, projection: @FieldType(publication.OwnerRequest, "projection")) publication.OwnerRequest {
        return .{
            .table_id = self.table_id,
            .group_id = self.group_id,
            .id = self.id,
            .operation = switch (self.operation) {
                .fence => .fence,
                .reserve => .reserve,
            },
            .projection = projection,
        };
    }
};

/// Own the decoded response, including a pending error name. An admitted
/// outcome without a receipt is durable progress, not a fenced/reserved ACK.
pub const Result = union(enum) {
    acknowledged,
    recovered: struct {
        /// Copied from the lease that recovered the committed physical cut;
        /// never substitute a later current-root observation when signing.
        root_generation: u64,
        outcome: std.json.Parsed(?native.Outcome),

        pub fn deinit(self: @This()) void {
            self.outcome.deinit();
        }
    },
    applied: std.json.Parsed(native.Outcome),

    pub fn deinit(self: *Result) void {
        switch (self.*) {
            .acknowledged => {},
            .recovered => |value| value.deinit(),
            .applied => |value| value.deinit(),
        }
        self.* = undefined;
    }

    /// Borrowed until deinit; copying an outcome does not own error_name.
    pub fn outcome(self: *const Result) ?native.Outcome {
        return switch (self.*) {
            .acknowledged => null,
            .recovered => |value| value.outcome.value,
            .applied => |value| value.value,
        };
    }
};

pub const CancellationRequest = struct { table_id: u64, group_id: u64, id: [16]u8 };

pub const ClaimRequest = struct {
    table_id: u64,
    group_id: u64,
    id: [16]u8,
    claims: @FieldType(native.ClaimCommand, "claims"),
};

pub const ClaimResult = union(enum) {
    acknowledged,
    applied: struct {
        root_generation: u64,
        outcome: std.json.Parsed(native.ClaimOutcome),
    },

    pub fn deinit(self: *ClaimResult) void {
        switch (self.*) {
            .acknowledged => {},
            .applied => |value| value.outcome.deinit(),
        }
        self.* = undefined;
    }
};

/// One bounded batch under freshly observed build permission. This is an
/// internal receiver boundary, not authenticated mounted transport or a signed
/// build acknowledgement. Local apply still fences cancellation atomically.
pub fn claimStep(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: ClaimRequest, context: operation.RequestContext) !ClaimResult {
    try context.ensureActive();
    const lookup: publication.OwnerRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .build };
    try lookup.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, lookup, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(permission.value, table_name);
    const identity = try permission.value.nativeExecutionIdentity(lookup);
    var state = try source.nativeIndexOwnerState(alloc, request.group_id, table_name, identity, context);
    defer state.deinit();
    const command = try permission.value.nativeClaims(lookup, state.value orelse return error.GenerationAdmissionPending, request.claims);
    const rooted = try source.applyNativeIndexOwnerClaimsAtRoot(alloc, request.group_id, table_name, command, context);
    errdefer rooted.outcome.deinit();
    try rooted.outcome.value.validateFor(command);
    // Preserve committed/pending progress even if cancellation arrived during
    // dispatch. Never replay a new effect to recover an uncertain result.
    return .{ .applied = .{ .root_generation = rooted.root_generation, .outcome = rooted.outcome } };
}

pub const CancellationResult = union(enum) {
    /// Only metadata's full canceled receipt can acknowledge this owner.
    acknowledged,
    intent: std.json.Parsed(native.CancellationOutcome),
    admitted: std.json.Parsed(native.CleanupOutcome),
    fenced: std.json.Parsed(native.CleanupOutcome),
    reserved: std.json.Parsed(native.CleanupOutcome),
    cleanup: std.json.Parsed(native.CleanupOutcome),
    gc: std.json.Parsed(native.CleanupMaintenanceOutcome),
    claim_gc: std.json.Parsed(native.ClaimCleanupMaintenanceOutcome),
    /// Local artifacts/claims only: cohort proof and epoch release are pending.
    retired,

    pub fn deinit(self: *CancellationResult) void {
        switch (self.*) {
            .acknowledged, .retired => {},
            .intent => |value| value.deinit(),
            .admitted, .fenced, .reserved, .cleanup => |value| value.deinit(),
            .gc => |value| value.deinit(),
            .claim_gc => |value| value.deinit(),
        }
        self.* = undefined;
    }
};

/// One bounded intent effect under independently read metadata permission.
/// Not a mounted authenticated RPC or a publication producer: neither an
/// incoming binding nor a source-selected intent is authoritative.
pub fn cancelIntentStep(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: CancellationRequest, context: operation.RequestContext) !CancellationResult {
    try context.ensureActive();
    const lookup: publication.OwnerRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .cancel };
    try lookup.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, lookup, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(permission.value, table_name);
    const intent = try permission.value.nativeCancellationIntent();
    var result = try source.cancelNativeIndexOwnerIntent(alloc, request.group_id, table_name, intent, context);
    errdefer result.deinit();
    try result.value.position.validate();
    if (!result.value.intent.eql(intent) or !result.value.position.namespace.eql(intent.fence.namespace))
        return error.PrivateIndexOwnerJobChanged;
    return .{ .intent = result };
}

/// Resume cancellation with at most one new durable owner effect per call.
/// This receiver still needs mounted authenticated RPC/signing and final global
/// retirement before production activation. Physical retirement is NOT an ACK.
pub fn cancelStep(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: CancellationRequest, context: operation.RequestContext) !CancellationResult {
    try context.ensureActive();
    var lookup: publication.OwnerRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .cancel };
    try lookup.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, lookup, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(permission.value, table_name);
    const expected_intent = try permission.value.nativeCancellationIntent();
    const identity = try permission.value.nativeExecutionIdentity(lookup);
    var state = try source.nativeIndexOwnerCancellationState(alloc, request.group_id, table_name, identity, context);
    defer state.deinit();
    try state.value.validate(identity);
    if (state.value.intent) |record| {
        if (!record.intent.eql(expected_intent)) return error.PrivateIndexOwnerJobChanged;
    } else {
        var result = try source.cancelNativeIndexOwnerIntent(alloc, request.group_id, table_name, expected_intent, context);
        errdefer result.deinit();
        try result.value.position.validate();
        if (!result.value.intent.eql(expected_intent) or !result.value.position.namespace.eql(identity.namespace)) return error.PrivateIndexOwnerJobChanged;
        return .{ .intent = result };
    }
    const binding = state.value.binding orelse {
        // Only a genuinely absent job needs schema-sized immutable inputs.
        // Independently reread cancellation permission with that projection.
        lookup.projection = .admission;
        var admission = try decision(alloc, metadata, lookup, context);
        defer admission.deinit();
        if (admission.value.disposition == .acknowledged) return .acknowledged;
        try requireTable(admission.value, table_name);
        const header = try admission.value.nativeCancellationAdmission();
        if (!header.eql(expected_intent)) return error.GenerationPublicationChanged;
        var manifest = try (admission.value.inputs orelse return error.InvalidGenerationPublication).prepareNativeManifest(alloc, admission.value.head);
        defer manifest.deinit();
        var result = try source.admitCanceledNativeIndexOwner(alloc, request.group_id, table_name, header, manifest.bytes, context);
        errdefer result.deinit();
        try requireCancellationProgress(result.value, null, header);
        return .{ .admitted = result };
    };
    if (state.value.fenced == null or state.value.reserved == null) {
        const effect: publication.CancellationControlEffect = if (state.value.fenced == null) .fenced else .reserved;
        const command = try permission.value.nativeCancellationControl(binding, effect);
        var result = try source.applyCanceledNativeIndexOwnerControl(alloc, request.group_id, table_name, command, context);
        errdefer result.deinit();
        try requireCancellationProgress(result.value, command.binding, expected_intent);
        return if (effect == .fenced) .{ .fenced = result } else .{ .reserved = result };
    }
    // Even the final hot path binds metadata's permission to the observed
    // physical allocation, not merely the caller's table/group/name.
    _ = try permission.value.nativeCancellationControl(binding, .reserved);
    if (state.value.cleanup == null) {
        var result = try source.cleanupNativeIndexOwner(alloc, request.group_id, table_name, identity, context);
        errdefer result.deinit();
        try requireCancellationProgress(result.value, binding, expected_intent);
        return .{ .cleanup = result };
    }
    if (state.value.artifacts_retired) {
        if (state.value.claims_retired) return .retired;
        var result = try source.collectNativeIndexOwnerClaimGarbage(alloc, request.group_id, table_name, identity, context);
        errdefer result.deinit();
        if (!result.value.advanced and !result.value.claims_retired) return error.InvalidPrivateIndexCancellation;
        return .{ .claim_gc = result };
    }
    var result = try source.collectNativeIndexOwnerGarbage(alloc, request.group_id, table_name, identity, context);
    errdefer result.deinit();
    if (!result.value.advanced and !result.value.artifacts_retired) return error.InvalidPrivateIndexCancellation;
    return .{ .gc = result };
}

fn requireCancellationProgress(result: native.CleanupOutcome, expected: ?jobs.Binding, intent: native.Admission) !void {
    try result.position.validate();
    _ = try result.binding.encode();
    if (result.binding.authority != .native or (expected == null and result.binding.phase != .fencing)) return error.InvalidPrivateIndexOwnerJob;
    if (!result.position.namespace.eql(intent.fence.namespace) or !native.Admission.fromBinding(result.binding).eql(intent)) return error.PrivateIndexOwnerJobChanged;
    if (expected) |binding| if (!std.mem.eql(u8, &try binding.encode(), &try result.binding.encode())) return error.PrivateIndexOwnerJobChanged;
}

fn decision(alloc: std.mem.Allocator, metadata: anytype, request: publication.OwnerRequest, context: operation.RequestContext) !std.json.Parsed(publication.OwnerDecision) {
    try context.ensureActive();
    var authority = context;
    authority.index_publication_authority = true;
    const bytes = try metadata.systemCatalog(alloc, authority, calls.Call{ .relational_index_owner_decision = request });
    defer alloc.free(bytes);
    if (bytes.len == 0 or bytes.len > publication.max_command_bytes) return error.InvalidGenerationPublication;
    var parsed = try std.json.parseFromSlice(publication.OwnerDecision, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    try parsed.value.requireRequest(request);
    if (parsed.value.owner.owner.authority != .native) return error.UnsupportedNativeIndexOwner;
    return parsed;
}

fn requireTable(value: publication.OwnerDecision, table_name: []const u8) !void {
    const dispatch = value.dispatch orelse return error.InvalidGenerationPublication;
    if (!std.mem.eql(u8, dispatch.table_name, table_name)) return error.GenerationPublicationChanged;
}

/// At most one new owner effect per call. Admission yields without fencing, so
/// cancellation and deadlines cannot disguise committed progress as an aborted
/// multi-effect request. Retry recovery neither recompiles schema-sized inputs
/// nor invents a new cut. Fresh permission is an observation, not a lease:
/// cancellation still requires a durable late-admission tombstone protocol.
pub fn step(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: Request, context: operation.RequestContext) !Result {
    try context.ensureActive();
    const hot_request = request.lookup(.permission);
    try hot_request.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, hot_request, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    const identity = try permission.value.nativeExecutionIdentity(hot_request);
    try requireTable(permission.value, table_name);
    var state = try source.nativeIndexOwnerState(alloc, request.group_id, table_name, identity, context);
    defer state.deinit();
    if (state.value) |binding| {
        const effect: @FieldType(native.ReceiptRequest, "effect") = switch (request.operation) {
            .fence => .fenced,
            .reserve => .reserved,
        };
        var recovered = try source.nativeIndexOwnerReceiptAtRoot(alloc, request.group_id, table_name, native.ReceiptRequest{ .identity = identity, .effect = effect }, context);
        if (recovered.outcome.value) |value| {
            errdefer recovered.deinit();
            _ = try permission.value.nativeControl(hot_request, value.binding);
            return .{ .recovered = .{ .root_generation = recovered.root_generation, .outcome = recovered.outcome } };
        }
        recovered.deinit();
        const command = try permission.value.nativeControl(hot_request, binding);
        // The source rechecks activity immediately before compiled dispatch,
        // and returns committed pending outcomes even if cancellation follows.
        return .{ .applied = try source.applyNativeIndexOwnerControl(alloc, request.group_id, table_name, command, context) };
    }
    if (request.operation != .fence) return error.GenerationAdmissionPending;
    // Fetch large immutable inputs only for a genuinely absent admission. This
    // second independent read also catches cancellation after cold acquisition.
    var admission = try decision(alloc, metadata, request.lookup(.admission), context);
    defer admission.deinit();
    if (admission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(admission.value, table_name);
    const header = try admission.value.nativeAdmission();
    const inputs = admission.value.inputs orelse return error.InvalidGenerationPublication;
    var manifest = try inputs.prepareNativeManifest(alloc, admission.value.head);
    defer manifest.deinit();
    return .{ .applied = try source.admitNativeIndexOwner(alloc, request.group_id, table_name, header, manifest.bytes, context) };
}

// Synthetic driver observations exercise receiver sequencing and ownership,
// never committed cut correctness or root attestation. Those require the
// physical-owner/C-ABI and distributed fault gates, not these component tests.
const TestDriver = struct {
    value: publication.OwnerDecision,
    binding: ?jobs.Binding = null,
    receipt_result: ?native.Outcome = null,
    permission_reads: usize = 0,
    admission_reads: usize = 0,
    inspections: usize = 0,
    recoveries: usize = 0,
    root_generation: u64 = 9,
    admissions: usize = 0,
    controls: usize = 0,
    claim_batches: usize = 0,
    cancellations: usize = 0,
    cancel_state: ?jobs.CancellationState = null,
    cleanups: usize = 0,
    gc_pages: usize = 0,
    claim_gc_pages: usize = 0,
    foreign_cancellation: bool = false,
    cancel_admission: bool = false,
    fail_control: bool = false,
    pending: bool = false,
    cancel_after_commit: ?*std.atomic.Value(bool) = null,

    pub fn systemCatalog(self: *TestDriver, alloc: std.mem.Allocator, context: operation.RequestContext, call: calls.Call) ![]u8 {
        try std.testing.expect(context.index_publication_authority);
        const request = call.relational_index_owner_decision;
        var value = self.value;
        switch (request.projection) {
            .permission => {
                self.permission_reads += 1;
                value.inputs = null;
            },
            .admission => {
                self.admission_reads += 1;
                if (self.cancel_admission) value.head = try value.head.cancel(value.head.revision, .requested);
            },
        }
        return std.json.Stringify.valueAlloc(alloc, value, .{});
    }

    fn parsed(comptime T: type, alloc: std.mem.Allocator, value: T) !std.json.Parsed(T) {
        const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
        defer alloc.free(encoded);
        return std.json.parseFromSlice(T, alloc, encoded, .{ .allocate = .alloc_always });
    }

    pub fn nativeIndexOwnerState(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, _: native.Identity, _: operation.RequestContext) !std.json.Parsed(?jobs.Binding) {
        self.inspections += 1;
        return parsed(?jobs.Binding, alloc, self.binding);
    }

    pub fn nativeIndexOwnerCancellationState(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, identity: native.Identity, context: operation.RequestContext) !std.json.Parsed(jobs.CancellationState) {
        try context.ensureActive();
        self.inspections += 1;
        if (self.cancel_state == null) self.cancel_state = .{
            .source = .{ .kind = .native, .namespace = local.storage_db_online_source_contract.namespaceBytes(identity.namespace) },
        };
        return parsed(jobs.CancellationState, alloc, self.cancel_state.?);
    }

    pub fn nativeIndexOwnerReceiptAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, _: native.ReceiptRequest, _: operation.RequestContext) !@FieldType(Result, "recovered") {
        self.recoveries += 1;
        return .{ .root_generation = self.root_generation, .outcome = try parsed(?native.Outcome, alloc, self.receipt_result) };
    }

    pub fn applyNativeIndexOwnerClaimsAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, command: native.ClaimCommand, context: operation.RequestContext) !@FieldType(ClaimResult, "applied") {
        try context.ensureActive();
        try command.validate();
        self.claim_batches += 1;
        if (self.cancel_after_commit) |signal| signal.store(true, .release);
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.ClaimOutcome, alloc, .{
            .binding = command.binding,
            .position = .{ .namespace = command.binding.fence.namespace, .sequence = 3 },
            .claims_added = command.claims.len,
            .duplicate = false,
            .replication_pending = self.pending,
            .error_name = if (self.pending) "ReplicationOutboxPending" else null,
        }) };
    }

    pub fn admitNativeIndexOwner(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, header: native.Admission, manifest: []const u8, _: operation.RequestContext) !std.json.Parsed(native.Outcome) {
        self.admissions += 1;
        try std.testing.expect(manifest.len != 0);
        self.binding = .{
            .id = header.id,
            .plan_digest = header.plan_digest,
            .owner_digest = header.owner_digest,
            .ordinal = header.ordinal,
            .authority = .native,
            .fence = header.fence,
            .integrity_before = header.fence.catalog_digest,
            .integrity_reserved = header.reservation_digest,
            .integrity_candidate = header.candidate_digest,
            .schema_before = @splat(3),
            .physical_before = null,
            .physical_reserved = .{ .revision = 2, .schema_version = 1, .blob_digest = @splat(4) },
            .physical_candidate = .{ .revision = 2, .schema_version = 2, .blob_digest = @splat(5) },
            .index = .{ .generation = 2, .slot = 0 },
            .inputs_digest = @splat(6),
        };
        if (self.cancel_after_commit) |signal| signal.store(true, .release);
        return parsed(native.Outcome, alloc, .{
            .binding = self.binding.?,
            .position = .{ .namespace = header.fence.namespace, .sequence = 1 },
            .duplicate = false,
            .replication_pending = self.pending,
            .error_name = if (self.pending) "ReplicationOutboxPending" else null,
        });
    }

    pub fn cancelNativeIndexOwnerIntent(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, intent: native.Admission, context: operation.RequestContext) !std.json.Parsed(native.CancellationOutcome) {
        try context.ensureActive();
        self.cancellations += 1;
        var result: native.CancellationOutcome = .{
            .intent = intent,
            .position = .{ .namespace = intent.fence.namespace, .sequence = 3 },
            .duplicate = self.cancellations > 1,
            .replication_pending = self.pending,
            .error_name = if (self.pending) "ReplicationOutboxPending" else null,
        };
        if (self.foreign_cancellation) result.intent.owner_digest[0] ^= 1;
        if (self.cancel_state) |*state| {
            state.intent = .{ .intent = result.intent, .position = result.position };
            state.source.sequence = result.position.sequence;
        }
        if (self.cancel_after_commit) |signal| signal.store(true, .release);
        return parsed(native.CancellationOutcome, alloc, result);
    }

    pub fn admitCanceledNativeIndexOwner(self: *TestDriver, alloc: std.mem.Allocator, group: u64, table: []const u8, header: native.Admission, manifest: []const u8, context: operation.RequestContext) !std.json.Parsed(native.CleanupOutcome) {
        try context.ensureActive();
        var admitted = try self.admitNativeIndexOwner(alloc, group, table, header, manifest, context);
        defer admitted.deinit();
        const state = &self.cancel_state.?;
        state.source.sequence += 1;
        const position: @FieldType(native.CleanupOutcome, "position") = .{ .namespace = header.fence.namespace, .sequence = state.source.sequence };
        state.binding = admitted.value.binding;
        state.admitted = position;
        return parsed(native.CleanupOutcome, alloc, .{ .binding = admitted.value.binding, .position = position, .duplicate = false, .replication_pending = self.pending, .error_name = if (self.pending) "ReplicationOutboxPending" else null });
    }

    pub fn applyCanceledNativeIndexOwnerControl(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, command: native.Command, context: operation.RequestContext) !std.json.Parsed(native.CleanupOutcome) {
        try context.ensureActive();
        self.controls += 1;
        if (self.fail_control) return error.ReceiverControlFailed;
        const state = &self.cancel_state.?;
        state.source.sequence += 1;
        const position: @FieldType(native.CleanupOutcome, "position") = .{ .namespace = command.binding.fence.namespace, .sequence = state.source.sequence };
        switch (command.effect) {
            .fenced => state.fenced = position,
            .reserved => {
                state.reserved = position;
                state.binding.?.phase = .reserved;
            },
            .admitted => return error.InvalidPrivateIndexOwnerJob,
        }
        return parsed(native.CleanupOutcome, alloc, .{ .binding = command.binding, .position = position, .duplicate = false });
    }

    pub fn cleanupNativeIndexOwner(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, identity: native.Identity, context: operation.RequestContext) !std.json.Parsed(native.CleanupOutcome) {
        try context.ensureActive();
        self.cleanups += 1;
        const state = &self.cancel_state.?;
        state.source.sequence += 1;
        const position: @FieldType(native.CleanupOutcome, "position") = .{ .namespace = identity.namespace, .sequence = state.source.sequence };
        state.cleanup = position;
        state.claim_allocation = 1;
        state.claims_retired = true;
        return parsed(native.CleanupOutcome, alloc, .{ .binding = state.binding.?, .position = position, .duplicate = false });
    }

    pub fn collectNativeIndexOwnerGarbage(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, _: native.Identity, context: operation.RequestContext) !std.json.Parsed(native.CleanupMaintenanceOutcome) {
        try context.ensureActive();
        self.gc_pages += 1;
        self.cancel_state.?.artifacts_retired = self.gc_pages == 2;
        return parsed(native.CleanupMaintenanceOutcome, alloc, .{ .advanced = true, .artifacts_retired = self.cancel_state.?.artifacts_retired });
    }

    pub fn collectNativeIndexOwnerClaimGarbage(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, _: native.Identity, context: operation.RequestContext) !std.json.Parsed(native.ClaimCleanupMaintenanceOutcome) {
        try context.ensureActive();
        self.claim_gc_pages += 1;
        self.cancel_state.?.claims_retired = true;
        return parsed(native.ClaimCleanupMaintenanceOutcome, alloc, .{ .advanced = true, .claims_retired = true });
    }

    pub fn applyNativeIndexOwnerControl(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, command: native.Command, _: operation.RequestContext) !std.json.Parsed(native.Outcome) {
        self.controls += 1;
        if (self.fail_control) return error.ReceiverControlFailed;
        try std.testing.expect(command.inputs_b64 == null);
        return parsed(native.Outcome, alloc, .{
            .binding = command.binding,
            .position = .{ .namespace = command.binding.fence.namespace, .sequence = 2 },
            .duplicate = false,
        });
    }
};

test "relational index publication native cancellation receiver resumes one bounded effect and never acknowledges physical GC" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    @constCast(plan.owners)[0].receipt_authority = .native;
    var prepared = try plan.prepare(alloc);
    defer prepared.deinit();
    var dispatch = try std.json.parseFromSlice(publication.Dispatch, alloc, prepared.dispatch[0], .{});
    defer dispatch.deinit();
    var inputs = try std.json.parseFromSlice(publication.Inputs, alloc, prepared.inputs, .{});
    defer inputs.deinit();
    const permission: publication.OwnerDecision = .{
        .metadata_group_id = 13,
        .metadata_incarnation = @splat('1'),
        .head = try prepared.head.cancel(prepared.head.revision, .requested),
        .owner = prepared.owners[0],
        .disposition = .execute,
        .dispatch = dispatch.value,
        .inputs = inputs.value,
    };
    const request: CancellationRequest = .{ .table_id = plan.before.table_id, .group_id = plan.owners[0].range.group_id, .id = plan.id };
    var driver: TestDriver = .{ .value = permission, .pending = true };
    for ([_]std.meta.Tag(CancellationResult){ .intent, .admitted, .fenced, .reserved, .cleanup, .gc, .gc, .retired }, 0..) |expected, ordinal| {
        const effects = driver.cancellations + driver.admissions + driver.controls + driver.cleanups + driver.gc_pages;
        var result = try cancelStep(alloc, &driver, &driver, plan.before.name, request, .{});
        defer result.deinit();
        try std.testing.expectEqual(expected, std.meta.activeTag(result));
        const after = driver.cancellations + driver.admissions + driver.controls + driver.cleanups + driver.gc_pages;
        try std.testing.expectEqual(@as(usize, if (expected == .retired) 0 else 1), after - effects);
        if (ordinal == 0) try std.testing.expect(result.intent.value.replication_pending);
        driver.pending = false;
    }
    try std.testing.expectEqual(@as(usize, 8), driver.permission_reads);
    try std.testing.expectEqual(@as(usize, 1), driver.admission_reads);
    try std.testing.expectEqual(@as(usize, 2), driver.gc_pages);
    try std.testing.expectEqual(@as(u64, 7), driver.cancel_state.?.source.sequence);
    driver.cancel_state.?.claims_retired = false;
    var claim_page = try cancelStep(alloc, &driver, &driver, plan.before.name, request, .{});
    defer claim_page.deinit();
    try std.testing.expect(claim_page == .claim_gc);
    try std.testing.expectEqual(@as(usize, 1), driver.claim_gc_pages);
    try std.testing.expectEqual(@as(u64, 7), driver.cancel_state.?.source.sequence);
    const inspections = driver.inspections;
    driver.cancel_state.?.source.sequence = 6;
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, cancelStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    try std.testing.expectEqual(inspections + 1, driver.inspections);
    try std.testing.expectEqual(@as(usize, 2), driver.gc_pages);
    driver.cancel_state.?.source.sequence = 7;
    driver.cancel_state.?.claim_allocation = null;
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, cancelStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver.cancel_state.?.claim_allocation = 0;
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, cancelStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver.cancel_state.?.claim_allocation = 1;
    const cleanup_cut = driver.cancel_state.?.cleanup;
    driver.cancel_state.?.cleanup = null;
    try std.testing.expectError(error.InvalidPrivateIndexCancellation, cancelStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver.cancel_state.?.cleanup = cleanup_cut;
    try std.testing.expectEqual(@as(usize, 2), driver.gc_pages);
    driver.cancel_state.?.intent.?.intent.owner_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, cancelStep(alloc, &driver, &driver, plan.before.name, request, .{}));

    var signal = std.atomic.Value(bool).init(false);
    const context: operation.RequestContext = .{ .cancellation = operation.CancellationToken.fromAtomic(&signal) };
    driver = .{ .value = permission, .pending = true, .cancel_after_commit = &signal };
    var pending_result = try cancelStep(alloc, &driver, &driver, plan.before.name, request, context);
    defer pending_result.deinit();
    try std.testing.expect(pending_result == .intent and pending_result.intent.value.replication_pending);
    try std.testing.expectError(error.Canceled, cancelStep(alloc, &driver, &driver, plan.before.name, request, context));
    try std.testing.expectEqual(@as(usize, 1), driver.cancellations);
    signal.store(false, .release);
    driver.cancel_after_commit = null;
    driver.value.inputs = null;
    try std.testing.expectError(error.InvalidGenerationPublication, cancelStep(alloc, &driver, &driver, plan.before.name, request, context));
    try std.testing.expectEqual(@as(usize, 0), driver.admissions);
}

test "relational index publication native cancellation receiver uses fixed permission and preserves pending intent" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    @constCast(plan.owners)[0].receipt_authority = .native;
    var prepared = try plan.prepare(alloc);
    defer prepared.deinit();
    var dispatch = try std.json.parseFromSlice(publication.Dispatch, alloc, prepared.dispatch[0], .{});
    defer dispatch.deinit();
    const permission: publication.OwnerDecision = .{
        .metadata_group_id = 13,
        .metadata_incarnation = @splat('1'),
        .head = try prepared.head.cancel(prepared.head.revision, .requested),
        .owner = prepared.owners[0],
        .disposition = .execute,
        .dispatch = dispatch.value,
    };
    const request: CancellationRequest = .{ .table_id = plan.before.table_id, .group_id = plan.owners[0].range.group_id, .id = plan.id };
    var signal = std.atomic.Value(bool).init(false);
    const context: operation.RequestContext = .{ .cancellation = operation.CancellationToken.fromAtomic(&signal) };
    var driver: TestDriver = .{ .value = permission, .pending = true, .cancel_after_commit = &signal };
    {
        var result = try cancelIntentStep(alloc, &driver, &driver, plan.before.name, request, context);
        defer result.deinit();
        try std.testing.expect(signal.load(.acquire));
        try std.testing.expect(result == .intent);
        try std.testing.expect(result.intent.value.replication_pending);
        try std.testing.expectEqualStrings("ReplicationOutboxPending", result.intent.value.error_name.?);
        try std.testing.expectEqual(@as(usize, 1), driver.cancellations);
        try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
        try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
        try std.testing.expectEqual(@as(usize, 0), driver.inspections);
        try std.testing.expectEqual(@as(usize, 0), driver.admissions);
    }
    try std.testing.expectError(error.Canceled, cancelIntentStep(alloc, &driver, &driver, plan.before.name, request, context));
    try std.testing.expectEqual(@as(usize, 1), driver.cancellations);
    signal.store(false, .release);
    driver.cancel_after_commit = null;
    driver.pending = false;
    {
        var result = try cancelIntentStep(alloc, &driver, &driver, plan.before.name, request, context);
        defer result.deinit();
        try std.testing.expect(result == .intent);
        try std.testing.expect(result.intent.value.duplicate);
        try std.testing.expect(!result.intent.value.replication_pending);
    }
    driver.foreign_cancellation = true;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, cancelIntentStep(alloc, &driver, &driver, plan.before.name, request, context));
    driver = .{ .value = permission };
    try std.testing.expectError(error.GenerationPublicationChanged, cancelIntentStep(alloc, &driver, &driver, "wrong table", request, context));
    try std.testing.expectEqual(@as(usize, 0), driver.cancellations);
    var foreign = request;
    foreign.group_id += 1;
    try std.testing.expectError(error.GenerationPublicationChanged, cancelIntentStep(alloc, &driver, &driver, plan.before.name, foreign, context));
    try std.testing.expectEqual(@as(usize, 0), driver.cancellations);
    driver.value.head = prepared.head;
    try std.testing.expectError(error.GenerationPublicationChanged, cancelIntentStep(alloc, &driver, &driver, plan.before.name, request, context));
    try std.testing.expectEqual(@as(usize, 0), driver.cancellations);
}

test "relational index publication claim receiver binds fresh build permission without schema projections" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    @constCast(plan.owners)[0].receipt_authority = .native;
    var prepared = try plan.prepare(alloc);
    defer prepared.deinit();
    var dispatch = try std.json.parseFromSlice(publication.Dispatch, alloc, prepared.dispatch[0], .{});
    defer dispatch.deinit();
    var driver: TestDriver = .{ .value = .{
        .metadata_group_id = 13,
        .metadata_incarnation = @splat('1'),
        .head = prepared.head,
        .owner = prepared.owners[0],
        .disposition = .execute,
        .dispatch = dispatch.value,
    } };
    var inputs = try std.json.parseFromSlice(publication.Inputs, alloc, prepared.inputs, .{});
    defer inputs.deinit();
    driver.value.inputs = inputs.value;
    const header = try driver.value.nativeAdmission();
    driver.value.inputs = null;
    var admitted = try driver.admitNativeIndexOwner(alloc, plan.owners[0].range.group_id, plan.before.name, header, "synthetic", .{});
    defer admitted.deinit();
    driver.binding.?.phase = .reserved;
    const progress = local.storage_db_relational_index_publication_progress;
    inline for (.{ progress.Action.fenced, progress.Action.reserved }, 1..) |action, sequence| {
        const next = try progress.acknowledge(driver.value.head, driver.value.owner, driver.value.head.revision, .{
            .owner = driver.value.owner.owner,
            .action = action,
            .position = .{ .native = .{ .namespace = header.fence.namespace, .sequence = sequence } },
            .candidate_digest = prepared.head.candidate_digest,
            .reservation_digest = prepared.head.reservation_digest,
            .evidence_digest = @splat(9),
        });
        driver.value.head = next.head;
        driver.value.owner = next.owner;
    }
    const permission = driver.value;
    const binding = driver.binding;
    const request: ClaimRequest = .{
        .table_id = plan.before.table_id,
        .group_id = plan.owners[0].range.group_id,
        .id = plan.id,
        .claims = &.{.{ .tuple = "receiver\x00\xff", .parent_table = plan.before.name, .parent_key = "row", .schema_version = 2 }},
    };
    var result = try claimStep(alloc, &driver, &driver, plan.before.name, request, .{});
    defer result.deinit();
    try std.testing.expect(result == .applied);
    try std.testing.expectEqual(@as(usize, 1), driver.claim_batches);
    try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
    try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
    try std.testing.expectEqual(@as(u64, 9), result.applied.root_generation);
    driver.root_generation += 1;
    try std.testing.expectEqual(@as(u64, 9), result.applied.root_generation);
    driver = .{ .value = permission, .binding = binding };
    try std.testing.expectError(error.GenerationPublicationChanged, claimStep(alloc, &driver, &driver, "wrong table", request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    driver.value.head = prepared.head;
    driver.value.owner = prepared.owners[0];
    try std.testing.expectError(error.GenerationPublicationChanged, claimStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver = .{ .value = permission };
    try std.testing.expectError(error.GenerationAdmissionPending, claimStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver.binding = binding;
    driver.binding.?.owner_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, claimStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.claim_batches);
    var signal = std.atomic.Value(bool).init(true);
    const context: operation.RequestContext = .{ .cancellation = operation.CancellationToken.fromAtomic(&signal) };
    driver = .{ .value = permission, .binding = binding };
    try std.testing.expectError(error.Canceled, claimStep(alloc, &driver, &driver, plan.before.name, request, context));
    try std.testing.expectEqual(@as(usize, 0), driver.permission_reads);
    signal.store(false, .release);
    driver.pending = true;
    driver.cancel_after_commit = &signal;
    var pending = try claimStep(alloc, &driver, &driver, plan.before.name, request, context);
    defer pending.deinit();
    try std.testing.expect(signal.load(.acquire));
    try std.testing.expect(pending.applied.outcome.value.replication_pending);
    try std.testing.expectEqualStrings("ReplicationOutboxPending", pending.applied.outcome.value.error_name.?);
}

test "relational index publication native receiver stages one effect recovers pending and rejects stale permission" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const plan = try publication.testing.plan(arena.allocator());
    @constCast(plan.owners)[0].receipt_authority = .native;
    var prepared = try plan.prepare(alloc);
    defer prepared.deinit();
    var dispatch = try std.json.parseFromSlice(publication.Dispatch, alloc, prepared.dispatch[0], .{});
    defer dispatch.deinit();
    var inputs = try std.json.parseFromSlice(publication.Inputs, alloc, prepared.inputs, .{});
    defer inputs.deinit();
    const permission: publication.OwnerDecision = .{
        .metadata_group_id = 13,
        .metadata_incarnation = @splat('1'),
        .head = prepared.head,
        .owner = prepared.owners[0],
        .disposition = .execute,
        .dispatch = dispatch.value,
        .inputs = inputs.value,
    };
    const request: Request = .{ .id = plan.id, .group_id = plan.owners[0].range.group_id, .table_id = plan.before.table_id, .operation = .fence };
    var driver: TestDriver = .{ .value = permission };
    {
        var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
        defer result.deinit();
        try std.testing.expect(result == .applied);
        try std.testing.expect(result.outcome().?.receipt == null);
        try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
        try std.testing.expectEqual(@as(usize, 1), driver.admission_reads);
        try std.testing.expectEqual(@as(usize, 1), driver.admissions);
        try std.testing.expectEqual(@as(usize, 0), driver.controls);
    }
    {
        var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), driver.controls);
        try std.testing.expectEqual(@as(usize, 1), driver.admission_reads);
        try std.testing.expectEqual(@as(usize, 1), driver.admissions);
    }
    driver.fail_control = true;
    try std.testing.expectError(error.ReceiverControlFailed, step(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver.fail_control = false;
    // The failing control above must not double-free the empty recovery result.
    driver.controls = 1;
    var pending_name = "ReplicationOutboxPending".*;
    driver.receipt_result = .{
        .binding = driver.binding.?,
        .position = .{ .namespace = driver.binding.?.fence.namespace, .sequence = 2 },
        .duplicate = true,
        .replication_pending = true,
        .error_name = &pending_name,
    };
    {
        var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
        defer result.deinit();
        @memset(&pending_name, 'x');
        try std.testing.expect(result == .recovered);
        try std.testing.expectEqual(@as(u64, 9), result.recovered.root_generation);
        driver.root_generation = 10;
        try std.testing.expectEqual(@as(u64, 9), result.recovered.root_generation);
        try std.testing.expect(result.outcome().?.replication_pending);
        try std.testing.expectEqualStrings("ReplicationOutboxPending", result.outcome().?.error_name.?);
        try std.testing.expectEqual(@as(usize, 1), driver.controls);
        try std.testing.expectEqual(@as(usize, 1), driver.admission_reads);
    }
    driver = .{ .value = permission, .cancel_admission = true };
    try std.testing.expectError(error.GenerationPublicationChanged, step(alloc, &driver, &driver, plan.before.name, request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.admissions);
    driver = .{ .value = permission };
    try std.testing.expectError(error.GenerationPublicationChanged, step(alloc, &driver, &driver, "wrong table", request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    var reserve = request;
    reserve.operation = .reserve;
    // Reject the phase before an owner read, not merely an absent local job.
    try std.testing.expectError(error.GenerationPublicationChanged, step(alloc, &driver, &driver, plan.before.name, reserve, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    driver = .{ .value = permission };
    const progress = local.storage_db_relational_index_publication_progress;
    const fenced = try progress.acknowledge(permission.head, permission.owner, permission.head.revision, .{
        .owner = permission.owner.owner,
        .action = .fenced,
        .position = .{ .native = .{ .namespace = plan.owners[0].namespace, .sequence = 1 } },
        .candidate_digest = permission.head.candidate_digest,
        .reservation_digest = permission.head.reservation_digest,
        .evidence_digest = @splat(9),
    });
    driver.value.head = fenced.head;
    driver.value.owner = fenced.owner;
    driver.value.disposition = .acknowledged;
    driver.value.dispatch = null;
    driver.value.inputs = null;
    {
        var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
        defer result.deinit();
        try std.testing.expect(result == .acknowledged);
        try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    }
    driver.value.disposition = .execute;
    driver.value.dispatch = dispatch.value;
    try std.testing.expectError(error.GenerationAdmissionPending, step(alloc, &driver, &driver, plan.before.name, reserve, .{}));
    var signal = std.atomic.Value(bool).init(true);
    const canceled: operation.RequestContext = .{ .cancellation = operation.CancellationToken.fromAtomic(&signal) };
    driver = .{ .value = permission };
    try std.testing.expectError(error.Canceled, step(alloc, &driver, &driver, plan.before.name, request, canceled));
    try std.testing.expectError(error.DeadlineExceeded, step(alloc, &driver, &driver, plan.before.name, request, .{ .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 0), driver.permission_reads);
    signal.store(false, .release);
    driver = .{ .value = permission, .pending = true, .cancel_after_commit = &signal };
    {
        var result = try step(alloc, &driver, &driver, plan.before.name, request, canceled);
        defer result.deinit();
        try std.testing.expect(signal.load(.acquire));
        try std.testing.expect(result.outcome().?.replication_pending);
        try std.testing.expectEqualStrings("ReplicationOutboxPending", result.outcome().?.error_name.?);
        try std.testing.expectEqual(@as(usize, 0), driver.controls);
    }
    const Check = struct {
        fn run(a: std.mem.Allocator, value: publication.OwnerDecision, req: Request, table: []const u8) !void {
            var fresh: TestDriver = .{ .value = value };
            var result = try step(a, &fresh, &fresh, table, req, .{});
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ permission, request, plan.before.name });
}
