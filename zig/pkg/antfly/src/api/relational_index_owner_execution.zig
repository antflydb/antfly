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
//! Optional attestations use pinned metadata authority and the applying lease.
//! Metadata must still independently validate publication authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const publication = @import("../metadata/relational_index_publication.zig");
const calls = @import("../system_catalog/server_call.zig");
const native = local.storage_db_relational_index_owner_control;
const jobs = local.storage_db_relational_index_owner_job;
const source_delivery = @import("../metadata/relational_index_source_delivery.zig");
const operation = local.api_operation;

pub const SigningFailure = enum { identity_unavailable, authority_changed, signing_failed };
const SigningContext = struct { view: publication.NativeSigningView, expected_revision: u64, request: Request };
const SigningState = struct {
    context: ?SigningContext = null,
    ack: ?publication.Ack = null,
    failure: ?SigningFailure = null,
};

pub const Request = struct {
    table_id: u64,
    group_id: u64,
    id: [16]u8,
    operation: enum { fence, reserve, build, source },
    build_budget: ?local.storage_db_relational_index_jobs.Budget = null,

    pub fn validate(self: Request, group_id: u64) !void {
        if (self.group_id != group_id) return error.GenerationPublicationChanged;
        if (self.operation != .build and self.build_budget != null) return error.InvalidGenerationPublication;
        if (self.build_budget) |budget| try budget.validate();
        try self.lookup(.permission).validate();
    }

    fn lookup(self: Request, projection: @FieldType(publication.OwnerRequest, "projection")) publication.OwnerRequest {
        return .{
            .table_id = self.table_id,
            .group_id = self.group_id,
            .id = self.id,
            .operation = switch (self.operation) {
                .fence => .fence,
                .reserve => .reserve,
                .build, .source => .build,
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
        signing: SigningState = .{},

        pub fn deinit(self: @This()) void {
            self.outcome.deinit();
        }
    },
    applied: std.json.Parsed(native.Outcome),
    controlled: struct { root_generation: u64, outcome: std.json.Parsed(native.Outcome), signing: SigningState = .{} },
    building: struct { root_generation: u64, outcome: std.json.Parsed(native.BuildResponse) },
    source: SourceResult,

    pub fn deinit(self: *Result) void {
        switch (self.*) {
            .acknowledged => {},
            .recovered => |value| value.deinit(),
            .applied => |value| value.deinit(),
            .controlled => |value| value.outcome.deinit(),
            .building => |value| value.outcome.deinit(),
            .source => |*value| value.deinit(),
        }
        self.* = undefined;
    }

    /// Borrowed until deinit; copying an outcome does not own error_name.
    pub fn outcome(self: *const Result) ?native.Outcome {
        return switch (self.*) {
            .acknowledged, .building, .source => null,
            .recovered => |value| value.outcome.value,
            .applied => |value| value.value,
            .controlled => |value| value.outcome.value,
        };
    }

    /// Admission alone has no publication receipt. Never reconstruct a root
    /// for signing from current routing after this result's lease was released.
    pub fn rootGeneration(self: *const Result) ?u64 {
        return switch (self.*) {
            .recovered => |value| value.root_generation,
            .controlled => |value| value.root_generation,
            .building => |value| value.root_generation,
            .source => |value| value.root_generation,
            .acknowledged, .applied => null,
        };
    }

    fn signingState(self: *Result) ?*SigningState {
        return switch (self.*) {
            .controlled => |*value| &value.signing,
            .recovered => |*value| &value.signing,
            .acknowledged, .applied, .building, .source => null,
        };
    }

    pub fn needsSigning(self: *Result) bool {
        if (self.* == .source) return self.source.needsSigning();
        const state = self.signingState() orelse return false;
        const value = self.outcome() orelse return false;
        return state.context != null and state.ack == null and value.receipt != null;
    }

    /// Signing is post-commit decoration, never another physical effect. The
    /// caller must preserve the outcome if the local identity is unavailable.
    pub fn signNative(self: *Result, root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        if (self.* == .source) return self.source.signNative(root, node_id, store_id, reporter_incarnation);
        const state = self.signingState() orelse return error.NativeIndexSigningUnavailable;
        const context = state.context orelse return error.NativeIndexSigningUnavailable;
        const value = self.outcome() orelse return error.NativeIndexSigningUnavailable;
        const receipt = value.receipt orelse return error.NativeIndexSigningUnavailable;
        try (try self.response()).validate(context.request);
        try context.view.validate(context.view.identity.metadata_incarnation);
        if (node_id != context.view.identity.node_id or store_id != context.view.identity.store_id or
            reporter_incarnation != context.view.reporter_incarnation) return error.IndexOwnerReceiptChanged;
        const proof = try (publication.RootProof{
            .identity = context.view.identity,
            .reporter_incarnation = reporter_incarnation,
            .authority = .{ .native = context.view.binding_digest },
            .root_generation = self.rootGeneration() orelse return error.NativeIndexSigningUnavailable,
            .signature = undefined,
        }).sign(root, try receipt.digest());
        const ack: publication.Ack = .{ .table_id = receipt.owner.namespace.table_id, .expected_revision = context.expected_revision, .signed = .{ .receipt = receipt, .proof = proof } };
        try ack.validate();
        state.ack = ack;
        state.failure = null;
    }

    pub fn signingFailed(self: *Result, failure: SigningFailure) void {
        if (self.* == .source) {
            self.source.signed_source = null;
            self.source.signing_failure = failure;
            return;
        }
        const state = self.signingState() orelse return;
        if (state.ack == null) state.failure = failure;
    }

    /// Borrowed response; retain this result until serialization completes.
    pub fn response(self: *const Result) !ControlResponse {
        return switch (self.*) {
            .acknowledged => .acknowledged,
            .applied => |value| .{ .admitted = value.value },
            .controlled => |value| .{ .controlled = .{ .root_generation = value.root_generation, .outcome = value.outcome.value, .signed_ack = value.signing.ack, .signing_failure = value.signing.failure } },
            .recovered => |value| .{ .recovered = .{ .root_generation = value.root_generation, .outcome = value.outcome.value orelse return error.InvalidPrivateIndexNativeReceipt, .signed_ack = value.signing.ack, .signing_failure = value.signing.failure } },
            .building => |value| .{ .building = .{ .root_generation = value.root_generation, .outcome = value.outcome.value } },
            .source => |*value| .{ .source = value.response() },
        };
    }
};

pub const control_remaining_ms_header = "X-Antfly-Index-Control-Remaining-Ms";
pub const max_control_request_bytes = 4096;

/// Internal progress only. Neither the applying-root generation nor an unsigned
/// logical receipt grants metadata publication authority to a remote caller.
pub const ControlResponse = union(enum) {
    acknowledged,
    admitted: native.Outcome,
    controlled: RootedOutcome,
    recovered: RootedOutcome,
    building: struct { root_generation: u64, outcome: native.BuildResponse },
    source: SourceResponse,

    pub const RootedOutcome = struct {
        root_generation: u64,
        outcome: native.Outcome,
        signed_ack: ?publication.Ack = null,
        signing_failure: ?SigningFailure = null,
    };

    pub fn validate(self: ControlResponse, request: Request) !void {
        try request.validate(request.group_id);
        const outcome: native.Outcome = switch (self) {
            .acknowledged => return,
            .source => |value| return value.validate(request),
            .building => |value| {
                if (request.operation != .build or value.root_generation == 0) return error.InvalidPrivateIndexNativeReceipt;
                const binding = switch (value.outcome) {
                    .advanced => |item| item.binding,
                    .complete => |item| item.binding,
                };
                if (binding.fence.namespace.table_id != request.table_id) return error.InvalidPrivateIndexNativeReceipt;
                const build = try native.BuildRequest.init(.{ .id = request.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = request.group_id }, binding, request.build_budget orelse .{});
                try value.outcome.validateFor(build);
                return;
            },
            .admitted => |value| blk: {
                if (request.operation != .fence or value.receipt != null or value.binding.phase != .fencing)
                    return error.InvalidPrivateIndexNativeReceipt;
                break :blk value;
            },
            .controlled, .recovered => |value| blk: {
                if (value.root_generation == 0) return error.InvalidPrivateIndexNativeReceipt;
                if (value.signing_failure != null and value.outcome.receipt == null) return error.InvalidPrivateIndexNativeReceipt;
                if (value.signed_ack) |ack| {
                    if (value.signing_failure != null or ack.table_id != request.table_id or ack.signed.proof.root_generation != value.root_generation or
                        ack.signed.proof.authority != .native) return error.InvalidPrivateIndexNativeReceipt;
                    try ack.validate();
                    const receipt = value.outcome.receipt orelse return error.InvalidPrivateIndexNativeReceipt;
                    if (!std.mem.eql(u8, &try receipt.digest(), &try ack.signed.receipt.digest())) return error.InvalidPrivateIndexNativeReceipt;
                    try (publication.NativeSigningView{ .identity = ack.signed.proof.identity, .reporter_incarnation = ack.signed.proof.reporter_incarnation, .binding_digest = ack.signed.proof.authority.native }).validate(ack.signed.proof.identity.metadata_incarnation);
                }
                break :blk value.outcome;
            },
        };
        const binding = outcome.binding;
        try outcome.validateRecovery(.{
            .identity = .{ .id = request.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = request.group_id },
            .effect = switch (request.operation) {
                .fence => .fenced,
                .reserve => .reserved,
                .build, .source => return error.InvalidPrivateIndexNativeReceipt,
            },
        });
        if (binding.fence.namespace.table_id != request.table_id) return error.InvalidPrivateIndexNativeReceipt;
    }
};

/// Read-only signed source transport. Envelope validation is not body
/// validation: callers must use decodeSigned before consuming logical rows.
/// A valid signature is not current enrollment/placement authority. Durable
/// source ACK acceptance must independently revalidate that metadata cut.
pub const SourceResponse = struct {
    root_generation: u64,
    observation: native.SourceWire,
    signed_source: ?publication.RootProof = null,
    signing_failure: ?SigningFailure = null,

    fn identity(self: SourceResponse, request: Request) native.Identity {
        const b = self.observation.binding;
        return .{ .id = request.id, .plan_digest = b.plan_digest, .owner_digest = b.owner_digest, .ordinal = b.ordinal, .namespace = b.fence.namespace, .owner_group_id = request.group_id };
    }

    pub fn validate(self: SourceResponse, request: Request) !void {
        try request.validate(request.group_id);
        if (request.operation != .source or self.root_generation == 0 or self.observation.binding.fence.namespace.table_id != request.table_id) return error.InvalidPrivateIndexClaimReceipt;
        try self.observation.requireIdentity(self.identity(request));
        if (self.signed_source) |proof| {
            if (self.signing_failure != null or proof.root_generation != self.root_generation or proof.authority != .native) return error.InvalidPrivateIndexClaimReceipt;
            (publication.NativeSigningView{ .identity = proof.identity, .reporter_incarnation = proof.reporter_incarnation, .binding_digest = proof.authority.native }).validate(proof.identity.metadata_incarnation) catch return error.InvalidPrivateIndexClaimReceipt;
            proof.verify(try self.observation.headerCommitment()) catch return error.InvalidPrivateIndexClaimReceipt;
        }
    }

    pub fn decodeSigned(self: SourceResponse, alloc: std.mem.Allocator, request: Request) !native.SourceRead {
        try self.validate(request);
        _ = self.signed_source orelse return error.InvalidPrivateIndexClaimReceipt;
        // validate authenticated the header commitment. Decode checks the
        // canonical body against that exact header; no second signature/hash
        // pass over reconstructed logical rows is necessary.
        return self.observation.decode(alloc, self.identity(request));
    }
};

pub const SourceResult = struct {
    root_generation: u64,
    outcome: native.SourceRead,
    wire: std.json.Parsed(native.SourceWire),
    signing: ?struct { view: publication.NativeSigningView, digest: ?[32]u8, root_generation: u64 } = null,
    signed_source: ?publication.RootProof = null,
    signing_failure: ?SigningFailure = null,

    pub fn deinit(self: *SourceResult) void {
        self.outcome.deinit();
        self.wire.deinit();
        self.* = undefined;
    }

    pub fn needsSigning(self: *const SourceResult) bool {
        return self.signing != null and self.signing.?.digest != null and self.signed_source == null;
    }

    pub fn signNative(self: *SourceResult, root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        const pinned = self.signing orelse return error.NativeIndexSigningUnavailable;
        const digest = try self.outcome.digest();
        try pinned.view.validate(pinned.view.identity.metadata_incarnation);
        if (pinned.digest == null or node_id != pinned.view.identity.node_id or store_id != pinned.view.identity.store_id or reporter_incarnation != pinned.view.reporter_incarnation or
            self.root_generation != pinned.root_generation or !std.mem.eql(u8, &digest, &pinned.digest.?) or !std.mem.eql(u8, &digest, &try self.wire.value.headerCommitment())) return error.IndexOwnerReceiptChanged;
        self.signed_source = try (publication.RootProof{ .identity = pinned.view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = pinned.view.binding_digest }, .root_generation = self.root_generation, .signature = undefined }).sign(root, digest);
        self.signing_failure = null;
    }

    pub fn response(self: *const SourceResult) SourceResponse {
        return .{ .root_generation = self.root_generation, .observation = self.wire.value, .signed_source = self.signed_source, .signing_failure = self.signing_failure };
    }
};

pub const ControlPort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Result,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, Request, operation.RequestContext) anyerror!Result };
    const BoundaryAbi = local.runtime_callback_abi.Boundary(VTable);

    pub fn execute(self: ControlPort, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: Request, context: operation.RequestContext) !Result {
        try context.ensureActive();
        try request.validate(group_id);
        var result = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        errdefer result.deinit();
        try (try result.response()).validate(request);
        return result;
    }
};

pub const CancellationRequest = struct { table_id: u64, group_id: u64, id: [16]u8 };

pub const claim_remaining_ms_header = "X-Antfly-Index-Claim-Remaining-Ms";
pub const max_claim_budget_ms: u32 = 30_000;

/// Exact private control failures, not arbitrary peer-selected error names.
/// Corruption is not a pending build, and stale/canceled owners retain their
/// identity so callers can recover metadata without replaying a new effect.
pub fn claimHttpFailure(status: u16, body: []const u8) ?anyerror {
    switch (status) {
        409 => inline for (.{ error.UniqueConstraintViolation, error.GenerationPublicationChanged, error.PrivateIndexOwnerJobChanged, error.PrivateIndexPublicationCanceled }) |err| {
            if (std.mem.eql(u8, body, @errorName(err))) return err;
        },
        408 => inline for (.{ error.Canceled, error.Cancelled, error.DeadlineExceeded }) |err| {
            if (std.mem.eql(u8, body, @errorName(err))) return err;
        },
        503 => inline for (.{ error.InvalidPrivateIndexClaimReceipt, error.InvalidPrivateIndexClaimInventory, error.InvalidPrivateIndexOwnerJob, error.InvalidPrivateIndexInputs, error.InvalidPrivateIndexNativeReceipt, error.PrivateIndexJobRequired, error.PrivateIndexRootRecoveryRequired, error.PrivateIndexPublicationRequired, error.PrivateIndexCancellationPending, error.PrivateIndexCancellationRequired, error.ImmutableRelationalIndexDefinitionConflict, error.InvalidIndexPublicationCandidate, error.InvalidRelationalIndexProgress, error.MissingRelationalIndexDefinitions, error.RelationalIndexCatalogSchemaMismatch, error.InvalidPrivateIndexCancellation, error.ResourceBudgetExceeded, error.PrivateIndexClaimBacklogFull }) |err| {
            if (std.mem.eql(u8, body, @errorName(err))) return err;
        },
        else => {},
    }
    return null;
}

pub const ClaimRequest = struct {
    table_id: u64,
    group_id: u64,
    id: [16]u8,
    claims: @FieldType(native.ClaimCommand, "claims") = &.{},
    closure: ?source_delivery.Request = null,

    /// Borrowed request for one deterministic source partition. This does not
    /// replace the receiver's fresh permission and physical binding checks.
    pub fn fromDelivery(plan: *const jobs.private_claims.delivery.Plan, cohort: anytype, group_index: usize) !ClaimRequest {
        if (!std.mem.eql(u8, &plan.cohort_digest, &cohort.digest)) return error.PrivateIndexOwnerJobChanged;
        try cohort.requireBinding(plan.source);
        const values = try plan.groupClaims(group_index);
        const owner = cohort.targets[plan.groups[group_index].target_index].owner;
        const result: ClaimRequest = .{ .table_id = owner.namespace.table_id, .group_id = owner.group_id, .id = owner.plan_id, .claims = values };
        try result.validate(result.group_id);
        return result;
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !ClaimRequest {
        const wire = try std.json.innerParse(struct { table_id: u64, group_id: u64, id: [16]u8, claims: ?native.ClaimCommand.BoundedClaims = null, closure: ?source_delivery.Request = null }, alloc, source, options);
        return .{ .table_id = wire.table_id, .group_id = wire.group_id, .id = wire.id, .claims = if (wire.claims) |values| values.items else &.{}, .closure = wire.closure };
    }

    pub fn jsonStringify(self: ClaimRequest, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("table_id");
        try jw.write(self.table_id);
        try jw.objectField("group_id");
        try jw.write(self.group_id);
        try jw.objectField("id");
        try jw.write(self.id);
        if (self.closure) |closure| {
            try jw.objectField("closure");
            try jw.write(closure);
        } else {
            try jw.objectField("claims");
            try jw.write(self.claims);
        }
        try jw.endObject();
    }

    pub fn validate(self: ClaimRequest, group_id: u64) !void {
        if (self.group_id != group_id) return error.GenerationPublicationChanged;
        try (publication.OwnerRequest{ .table_id = self.table_id, .group_id = self.group_id, .id = self.id, .operation = .build }).validate();
        if (self.closure) |closure| {
            if (self.claims.len != 0) return error.InvalidPrivateIndexClaimReceipt;
            try closure.validate();
        } else try (native.ClaimCommand.BoundedClaims{ .items = self.claims }).validate();
    }
};

/// Borrowed wire view; the owning ClaimResult must outlive serialization.
/// A physical progress response is never a signed build-completion receipt.
pub const ClaimResponse = union(enum) {
    acknowledged,
    closed: struct { data: source_delivery.Data, root_generation: u64, proof: ?publication.RootProof = null, signing_failure: ?SigningFailure = null },
    applied: struct {
        root_generation: u64,
        outcome: native.ClaimOutcome,
        signed_delivery: ?publication.RootProof = null,
        signing_failure: ?SigningFailure = null,
    },

    /// Transport success or an acknowledged phase is insufficient to retire
    /// source debt. Return only exact committed, signed target evidence; the
    /// consumer must still verify current metadata authority independently.
    pub fn requireSignedDelivery(self: ClaimResponse, request: ClaimRequest) !publication.SignedClaimDelivery {
        try self.validate(request);
        if (self != .applied) return error.InvalidPrivateIndexClaimReceipt;
        const value = self.applied;
        return .{
            .outcome = value.outcome,
            .proof = value.signed_delivery orelse return error.InvalidPrivateIndexClaimReceipt,
        };
    }

    pub fn validate(self: ClaimResponse, request: ClaimRequest) !void {
        try request.validate(request.group_id);
        switch (self) {
            .acknowledged => {},
            .closed => |value| {
                const wanted = request.closure orelse return error.InvalidPrivateIndexClaimReceipt;
                if (value.root_generation == 0 or value.data.source.fence.owner_group_id != request.group_id or value.data.source.fence.namespace.table_id != request.table_id or
                    !std.mem.eql(u8, &value.data.source.id, &request.id) or !std.mem.eql(u8, &value.data.header.digest(), &wanted.batch_digest) or
                    std.mem.readInt(u64, value.data.header.bytes[104..112], .little) != wanted.ordinal or value.data.parts.items.len != wanted.targets.items.len) return error.InvalidPrivateIndexClaimReceipt;
                _ = try value.data.digest();
                for (wanted.targets.items) |target| {
                    if (target.index >= value.data.parts.items.len or !std.mem.eql(u8, &try target.signed.commitment(), &try value.data.parts.items[target.index].target.commitment())) return error.InvalidPrivateIndexClaimReceipt;
                }
                if (value.proof) |proof| {
                    if (value.signing_failure != null or proof.root_generation != value.root_generation) return error.InvalidPrivateIndexClaimReceipt;
                    try (source_delivery.Signed{ .data = value.data, .proof = proof }).verify();
                }
            },
            .applied => |value| {
                if (request.closure != null) return error.InvalidPrivateIndexClaimReceipt;
                const binding = value.outcome.binding;
                if (value.root_generation == 0 or !std.mem.eql(u8, &binding.id, &request.id) or
                    binding.fence.owner_group_id != request.group_id or binding.fence.namespace.table_id != request.table_id)
                    return error.InvalidPrivateIndexClaimReceipt;
                if (value.signed_delivery) |proof| {
                    if (value.signing_failure != null or proof.root_generation != value.root_generation or proof.authority != .native)
                        return error.InvalidPrivateIndexClaimReceipt;
                    try (publication.SignedClaimDelivery{ .outcome = value.outcome, .proof = proof }).verifyFor(.{ .binding = binding, .claims = request.claims });
                } else try value.outcome.validateFor(.{ .binding = binding, .claims = request.claims });
            },
        }
    }
};

pub const ClaimPort = struct {
    ptr: *anyopaque,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, ClaimRequest, operation.RequestContext) anyerror!ClaimResult,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,
    const VTable = struct { execute_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, u64, ClaimRequest, operation.RequestContext) anyerror!ClaimResult };
    const BoundaryAbi = local.runtime_callback_abi.Boundary(VTable);

    pub fn execute(self: ClaimPort, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, request: ClaimRequest, context: operation.RequestContext) !ClaimResult {
        try context.ensureActive();
        try request.validate(group_id);
        var result = try BoundaryAbi.call("execute_fn", self.boundary_dispatch, self.execute_fn, .{ self.ptr, alloc, table_name, group_id, request, context });
        errdefer result.deinit();
        try result.response().validate(request);
        return result;
    }
};

pub const ClaimResult = union(enum) {
    acknowledged,
    closed: struct { root_generation: u64, pinned_root_generation: u64, owned: source_delivery.Owned, view: publication.NativeSigningView, expected_digest: [32]u8, proof: ?publication.RootProof = null, signing_failure: ?SigningFailure = null },
    applied: struct {
        root_generation: u64,
        outcome: std.json.Parsed(native.ClaimOutcome),
        signing: ?struct { view: publication.NativeSigningView, delivery_digest: ?[32]u8, root_generation: u64 } = null,
        signed_delivery: ?publication.RootProof = null,
        signing_failure: ?SigningFailure = null,
    },

    pub fn needsSigning(self: *const ClaimResult) bool {
        return switch (self.*) {
            .acknowledged => false,
            .closed => |value| value.proof == null,
            .applied => |value| value.signing != null and value.signed_delivery == null and
                !value.outcome.value.replication_pending and value.outcome.value.error_name == null,
        };
    }

    /// Post-commit decoration of exact delivery, never publication authority.
    /// Current enrollment/placement and complete source coverage still need
    /// independent verification before a source batch may be consumed.
    pub fn signNative(self: *ClaimResult, root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        if (self.* == .closed) {
            const closed = &self.closed;
            try closed.view.validate(closed.view.identity.metadata_incarnation);
            const actual = try closed.owned.data.digest();
            if (closed.root_generation != closed.pinned_root_generation or !std.mem.eql(u8, &actual, &closed.expected_digest) or node_id != closed.view.identity.node_id or store_id != closed.view.identity.store_id or reporter_incarnation != closed.view.reporter_incarnation) return error.IndexOwnerReceiptChanged;
            closed.proof = try (publication.RootProof{ .identity = closed.view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = closed.view.binding_digest }, .root_generation = closed.root_generation, .signature = undefined }).sign(root, actual);
            closed.signing_failure = null;
            return;
        }
        if (self.* != .applied) return error.NativeIndexSigningUnavailable;
        const value = &self.applied;
        const context = value.signing orelse return error.NativeIndexSigningUnavailable;
        try context.view.validate(context.view.identity.metadata_incarnation);
        const delivery_digest = try value.outcome.value.deliveryDigest();
        if (node_id != context.view.identity.node_id or store_id != context.view.identity.store_id or
            reporter_incarnation != context.view.reporter_incarnation or
            value.root_generation != context.root_generation or context.delivery_digest == null or
            !std.mem.eql(u8, &context.delivery_digest.?, &delivery_digest)) return error.IndexOwnerReceiptChanged;
        value.signed_delivery = try (publication.RootProof{
            .identity = context.view.identity,
            .reporter_incarnation = reporter_incarnation,
            .authority = .{ .native = context.view.binding_digest },
            .root_generation = value.root_generation,
            .signature = undefined,
        }).sign(root, delivery_digest);
        value.signing_failure = null;
    }

    pub fn signingFailed(self: *ClaimResult, failure: SigningFailure) void {
        if (self.* == .closed) {
            self.closed.proof = null;
            self.closed.signing_failure = failure;
        }
        if (self.* == .applied) {
            self.applied.signed_delivery = null;
            self.applied.signing_failure = failure;
        }
    }

    pub fn deinit(self: *ClaimResult) void {
        switch (self.*) {
            .acknowledged => {},
            .closed => |*value| value.owned.deinit(),
            .applied => |value| value.outcome.deinit(),
        }
        self.* = undefined;
    }

    pub fn response(self: *const ClaimResult) ClaimResponse {
        return switch (self.*) {
            .acknowledged => .acknowledged,
            .closed => |*value| .{ .closed = .{ .root_generation = value.root_generation, .data = value.owned.data, .proof = value.proof, .signing_failure = value.signing_failure } },
            .applied => |value| .{ .applied = .{ .root_generation = value.root_generation, .outcome = value.outcome.value, .signed_delivery = value.signed_delivery, .signing_failure = value.signing_failure } },
        };
    }
};

/// One bounded batch under freshly observed build permission. This is an
/// internal receiver boundary, not authenticated mounted transport or a signed
/// build acknowledgement. Local apply still fences cancellation atomically.
pub fn claimStep(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: ClaimRequest, context: operation.RequestContext) !ClaimResult {
    try context.ensureActive();
    try request.validate(request.group_id);
    var groups: [source_delivery.max_parts]u64 = undefined;
    var group_count: usize = 0;
    if (request.closure) |closure| {
        for (closure.targets.items, groups[0..closure.targets.items.len]) |target, *group| group.* = target.signed.outcome.binding.fence.owner_group_id;
        std.mem.sort(u64, groups[0..closure.targets.items.len], {}, std.sort.asc(u64));
        for (groups[0..closure.targets.items.len]) |group| if (group_count == 0 or groups[group_count - 1] != group) {
            groups[group_count] = group;
            group_count += 1;
        };
    }
    const lookup: publication.OwnerRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .build, .claim_groups = if (request.closure != null) .{ .items = groups[0..group_count] } else null };
    try lookup.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, lookup, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(permission.value, table_name);
    const identity = try permission.value.nativeExecutionIdentity(lookup);
    if (request.closure) |closure| {
        const view = permission.value.native_signing orelse return error.IndexOwnerReceiptChanged;
        for (closure.targets.items) |target| {
            const proof = target.signed.proof;
            if (proof.authority != .native or proof.reporter_incarnation != view.reporter_incarnation or !std.mem.eql(u8, &try proof.identity.digest(), &try view.identity.digest()) or
                !std.mem.eql(u8, &proof.authority.native, &view.binding_digest)) return error.IndexOwnerReceiptChanged;
        }
        var rooted = try source.readNativeIndexClaimSourceAtRoot(alloc, request.group_id, table_name, identity, context);
        defer rooted.deinit();
        try rooted.outcome.requireIdentity(identity);
        _ = try permission.value.nativeBuild(lookup, rooted.outcome.binding, .{});
        if (rooted.root_generation == 0) return error.InvalidPrivateIndexClaimReceipt;
        if (rooted.outcome.batch) |batch| if (!std.mem.eql(u8, table_name, batch.scope.table_name)) return error.PrivateIndexOwnerJobChanged;
        const routes = permission.value.claim_routes orelse return error.InvalidCatalogRecord;
        var subset = try jobs.private_claims.delivery.Subset.init(alloc, permission.value.owner.owner, permission.value.head.owner_count, routes.items);
        defer subset.deinit();
        var owned = try source_delivery.prepare(alloc, &rooted.outcome, &subset, closure, context);
        errdefer owned.deinit();
        return .{ .closed = .{ .root_generation = rooted.root_generation, .pinned_root_generation = rooted.root_generation, .owned = owned, .view = view, .expected_digest = try owned.data.digest() } };
    }
    var state = try source.nativeIndexOwnerState(alloc, request.group_id, table_name, identity, context);
    defer state.deinit();
    const command = try permission.value.nativeClaims(lookup, state.value orelse return error.PrivateIndexRootRecoveryRequired, request.claims);
    const rooted = try source.applyNativeIndexOwnerClaimsAtRoot(alloc, request.group_id, table_name, command, context);
    errdefer rooted.outcome.deinit();
    try rooted.outcome.value.validateFor(command);
    // Preserve committed/pending progress even if cancellation arrived during
    // dispatch. Never replay a new effect to recover an uncertain result.
    return .{ .applied = .{
        .root_generation = rooted.root_generation,
        .outcome = rooted.outcome,
        .signing = if (permission.value.native_signing) |view| .{
            .view = view,
            .delivery_digest = if (rooted.outcome.value.replication_pending or rooted.outcome.value.error_name != null) null else try rooted.outcome.value.deliveryDigest(),
            .root_generation = rooted.root_generation,
        } else null,
    } };
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
    try request.validate(request.group_id);
    const hot_request = request.lookup(.permission);
    try hot_request.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, hot_request, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) {
        const grant = permission.value.attestation orelse return .acknowledged;
        if (!std.mem.eql(u8, grant.table_name, table_name)) return error.GenerationPublicationChanged;
        try context.ensureActive();
        const receipt_request: native.ReceiptRequest = .{ .identity = grant.identity, .effect = switch (request.operation) {
            .fence => .fenced,
            .reserve => .reserved,
            .build, .source => return error.InvalidGenerationPublication,
        } };
        var recovered = try source.nativeIndexOwnerReceiptAtRoot(alloc, request.group_id, table_name, receipt_request, context);
        errdefer recovered.deinit();
        const outcome = recovered.outcome.value orelse return error.PrivateIndexRootRecoveryRequired;
        try outcome.validateRecovery(receipt_request);
        const receipt = outcome.receipt orelse return error.PrivateIndexRootRecoveryRequired;
        if (!std.meta.eql(receipt.owner, permission.value.owner.owner) or
            !std.mem.eql(u8, &receipt.candidate_digest, &permission.value.head.candidate_digest) or
            !std.mem.eql(u8, &receipt.reservation_digest, &permission.value.head.reservation_digest) or
            !std.mem.eql(u8, &try receipt.digest(), &permission.value.owner.receipts[@backingInt(receipt.action)])) return error.PrivateIndexRootRecoveryRequired;
        return .{ .recovered = .{ .root_generation = recovered.root_generation, .outcome = recovered.outcome, .signing = .{ .context = .{ .view = grant.signing, .expected_revision = permission.value.head.revision, .request = request } } } };
    }
    const identity = try permission.value.nativeExecutionIdentity(hot_request);
    try requireTable(permission.value, table_name);
    const signing: SigningState = .{ .context = if (permission.value.native_signing) |view| .{ .view = view, .expected_revision = permission.value.head.revision, .request = request } else null };
    if (request.operation == .source) {
        var rooted = try source.readNativeIndexClaimSourceAtRoot(alloc, request.group_id, table_name, identity, context);
        errdefer rooted.deinit();
        try rooted.outcome.requireIdentity(identity);
        _ = try permission.value.nativeBuild(hot_request, rooted.outcome.binding, .{});
        if (rooted.root_generation == 0) return error.InvalidPrivateIndexClaimReceipt;
        const attestable = !rooted.outcome.replication_pending and rooted.outcome.batch != null and rooted.outcome.batch.?.header.bytes[184] != @backingInt(jobs.private_claims.SourceStream.State.failed);
        if (rooted.outcome.batch) |batch| if (!std.mem.eql(u8, table_name, batch.scope.table_name)) return error.PrivateIndexOwnerJobChanged;
        return .{ .source = .{ .root_generation = rooted.root_generation, .outcome = rooted.outcome, .wire = rooted.wire, .signing = if (permission.value.native_signing) |view| .{ .view = view, .digest = if (attestable) try rooted.outcome.digest() else null, .root_generation = rooted.root_generation } else null } };
    }
    var state = try source.nativeIndexOwnerState(alloc, request.group_id, table_name, identity, context);
    defer state.deinit();
    if (request.operation == .build) {
        const build = try permission.value.nativeBuild(hot_request, state.value orelse return error.PrivateIndexRootRecoveryRequired, request.build_budget orelse .{});
        const rooted = try source.stepNativeIndexOwnerBuildAtRoot(alloc, request.group_id, table_name, build, context);
        errdefer rooted.outcome.deinit();
        try rooted.outcome.value.validateFor(build);
        if (rooted.root_generation == 0) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .building = .{ .root_generation = rooted.root_generation, .outcome = rooted.outcome } };
    }
    if (state.value) |binding| {
        const effect: @FieldType(native.ReceiptRequest, "effect") = switch (request.operation) {
            .fence => .fenced,
            .reserve => .reserved,
            .build, .source => return error.InvalidGenerationPublication,
        };
        var recovered = try source.nativeIndexOwnerReceiptAtRoot(alloc, request.group_id, table_name, native.ReceiptRequest{ .identity = identity, .effect = effect }, context);
        if (recovered.outcome.value) |value| {
            errdefer recovered.deinit();
            try value.validateRecovery(.{ .identity = identity, .effect = effect });
            _ = try permission.value.nativeControl(hot_request, value.binding);
            return .{ .recovered = .{ .root_generation = recovered.root_generation, .outcome = recovered.outcome, .signing = signing } };
        }
        recovered.deinit();
        const command = try permission.value.nativeControl(hot_request, binding);
        // The source rechecks activity immediately before compiled dispatch,
        // and returns committed pending outcomes even if cancellation follows.
        const controlled = try source.applyNativeIndexOwnerControlAtRoot(alloc, request.group_id, table_name, command, context);
        errdefer controlled.outcome.deinit();
        try controlled.outcome.value.validateFor(command);
        return .{ .controlled = .{ .root_generation = controlled.root_generation, .outcome = controlled.outcome, .signing = signing } };
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
    produce_receipt: bool = false,
    pending: bool = false,
    build_pages: usize = 0,
    source_reads: usize = 0,
    cancel_after_commit: ?*std.atomic.Value(bool) = null,

    pub fn systemCatalog(self: *TestDriver, alloc: std.mem.Allocator, context: operation.RequestContext, call: calls.Call) ![]u8 {
        try std.testing.expect(context.index_publication_authority);
        const request = call.relational_index_owner_decision;
        var value = self.value;
        var routes: [source_delivery.max_parts]jobs.private_claims.delivery.Target = undefined;
        if (request.claim_groups) |groups| {
            for (groups.items, routes[0..groups.items.len]) |group, *route| {
                if (group != value.owner.owner.group_id) return error.GenerationPublicationChanged;
                route.* = .{ .owner = value.owner.owner, .start = value.dispatch.?.owner.range.start_key, .end = value.dispatch.?.owner.range.end_key orelse "" };
            }
            value.claim_routes = .{ .items = routes[0..groups.items.len] };
        }
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
            .claims_digest = try command.digest(),
            .position = .{ .namespace = command.binding.fence.namespace, .sequence = 3 },
            .claims_added = command.claims.len,
            .duplicate = false,
            .replication_pending = self.pending,
            .error_name = if (self.pending) "ReplicationOutboxPending" else null,
        }) };
    }

    pub fn stepNativeIndexOwnerBuildAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, request: native.BuildRequest, context: operation.RequestContext) !@FieldType(Result, "building") {
        try context.ensureActive();
        const binding = self.binding orelse return error.PrivateIndexRootRecoveryRequired;
        try request.requireBinding(binding);
        self.build_pages += 1;
        if (self.cancel_after_commit) |signal| signal.store(true, .release);
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .advanced = .{ .binding = binding, .page_digest = @splat(9), .position = .{ .namespace = binding.fence.namespace, .sequence = 4 }, .duplicate = false, .replication_pending = self.pending, .error_name = if (self.pending) "ReplicationOutboxPending" else null } }) };
    }

    pub fn readNativeIndexClaimSourceAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, table_name: []const u8, identity: native.Identity, context: operation.RequestContext) !SourceResult {
        try context.ensureActive();
        const binding = self.binding orelse return error.PrivateIndexRootRecoveryRequired;
        self.source_reads += 1;
        const Source = jobs.private_claims.SourceStream;
        var prepared = try Source.PreparedSource.init(alloc, binding, .{ .table_name = table_name, .generation = @splat(1), .schema_version = binding.physical_candidate.schema_version, .tuple_fingerprint = @splat(2), .nulls_not_distinct = false }, @splat(9), .ready, &.{.{ .key = "row", .tuple = "logical\x00\xff\x80" }});
        defer prepared.deinit();
        var header = prepared.header;
        std.mem.writeInt(u64, header[104..112], 1, .little);
        @memcpy(header[112..145], &try (local.storage_db_receipt_position.Position{ .native = .{ .namespace = binding.fence.namespace, .sequence = 4 } }).encode());
        std.crypto.hash.Blake3.hash(header[0..288], header[288..320], .{});
        const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(prepared.payload.len));
        defer alloc.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, prepared.payload);
        var wire = try parsed(native.SourceWire, alloc, .{ .binding = binding, .header = if (self.pending) null else try Source.Header.decode(&header, binding, 1), .payload_b64 = if (self.pending) null else encoded, .replication_pending = self.pending, .error_name = if (self.pending) "ReplicationOutboxPending" else null });
        errdefer wire.deinit();
        return .{ .root_generation = self.root_generation, .outcome = try wire.value.decode(alloc, identity), .wire = wire };
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

    pub fn applyNativeIndexOwnerControlAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, command: native.Command, _: operation.RequestContext) !@FieldType(Result, "controlled") {
        self.controls += 1;
        if (self.fail_control) return error.ReceiverControlFailed;
        try std.testing.expect(command.inputs_b64 == null);
        const position: local.storage_db_receipt_position.Native = .{ .namespace = command.binding.fence.namespace, .sequence = 2 };
        var receipt: ?local.storage_db_relational_index_publication_progress.Receipt = null;
        if (self.produce_receipt) {
            // Synthetic consistency witness, not proof of a durable effect.
            var binding_digest: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(&try command.binding.encode(), &binding_digest, .{});
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update("antfly:index-owner-native-effect:v1");
            hash.update(&binding_digest);
            hash.update(&.{@backingInt(command.effect)});
            var evidence: [32]u8 = undefined;
            hash.final(&evidence);
            receipt = .{
                .owner = .{ .plan_id = command.binding.id, .plan_digest = command.binding.plan_digest, .ordinal = command.binding.ordinal, .group_id = command.binding.fence.owner_group_id, .namespace = command.binding.fence.namespace, .authority = .native, .fence_digest = command.binding.owner_digest },
                .action = switch (command.effect) {
                    .fenced => .fenced,
                    .reserved => .reserved,
                    .admitted => unreachable,
                },
                .position = .{ .native = position },
                .candidate_digest = command.binding.integrity_candidate,
                .reservation_digest = command.binding.integrity_reserved,
                .evidence_digest = evidence,
            };
        }
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.Outcome, alloc, .{
            .binding = command.binding,
            .position = position,
            .duplicate = false,
            .receipt = receipt,
        }) };
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

test "relational index publication claim HTTP errors preserve conflicts corruption and cancellation" {
    inline for (.{ .{ 409, error.UniqueConstraintViolation }, .{ 409, error.PrivateIndexOwnerJobChanged }, .{ 409, error.PrivateIndexPublicationCanceled }, .{ 503, error.InvalidPrivateIndexClaimReceipt }, .{ 503, error.PrivateIndexJobRequired }, .{ 503, error.ResourceBudgetExceeded }, .{ 503, error.PrivateIndexClaimBacklogFull }, .{ 408, error.DeadlineExceeded } }) |entry| {
        try std.testing.expectEqual(entry[1], claimHttpFailure(entry[0], @errorName(entry[1])).?);
    }
    try std.testing.expect(claimHttpFailure(200, "PrivateIndexOwnerJobChanged") == null);
    try std.testing.expect(claimHttpFailure(503, "UniqueConstraintViolation") == null);
    try std.testing.expect(claimHttpFailure(503, "UnknownPeerError") == null);
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
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .public_key = pair.public_key.toBytes(), .root_incarnation = 7 };
    const root_binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7 };
    driver.value.native_signing = .{
        .identity = .{ .metadata_incarnation = root_binding.metadata_incarnation, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
        .reporter_incarnation = 4,
        .binding_digest = try root_binding.digest(),
    };
    const permission = driver.value;
    const binding = driver.binding;
    const request: ClaimRequest = .{
        .table_id = plan.before.table_id,
        .group_id = plan.owners[0].range.group_id,
        .id = plan.id,
        .claims = &.{.{ .tuple = "receiver\x00\xff", .parent_table = plan.before.name, .parent_key = "row", .schema_version = 2 }},
    };
    {
        const delivery = jobs.private_claims.delivery;
        const Source = jobs.private_claims.SourceStream;
        var cohort = try delivery.Cohort.init(alloc, &.{.{ .owner = permission.owner.owner }});
        defer cohort.deinit();
        const scope: Source.Scope = .{ .table_name = plan.before.name, .generation = @splat(1), .schema_version = 2, .tuple_fingerprint = @splat(2), .nulls_not_distinct = false };
        const rows = [_]Source.Row{.{ .key = request.claims[0].parent_key, .tuple = request.claims[0].tuple }};
        var source = try Source.PreparedSource.init(alloc, binding.?, scope, @splat(9), .ready, &rows);
        defer source.deinit();
        var source_header = source.header;
        std.mem.writeInt(u64, source_header[104..112], 1, .little);
        @memcpy(source_header[112..145], &try (local.storage_db_receipt_position.Position{ .native = .{ .namespace = binding.?.fence.namespace, .sequence = 4 } }).encode());
        std.crypto.hash.Blake3.hash(source_header[0..288], source_header[288..320], .{});
        const prepared_delivery = try delivery.Plan.prepare(&cohort, binding.?, .{ .header = try Source.Header.decode(&source_header, binding.?, 1), .scope = scope, .rows = &rows });
        const generated = try ClaimRequest.fromDelivery(&prepared_delivery, &cohort, 0);
        try std.testing.expectEqual(request.table_id, generated.table_id);
        try std.testing.expectEqual(request.group_id, generated.group_id);
        try std.testing.expectEqual(request.id, generated.id);
        try std.testing.expectEqual(try (native.ClaimCommand{ .binding = binding.?, .claims = request.claims }).digest(), try (native.ClaimCommand{ .binding = binding.?, .claims = generated.claims }).digest());
    }
    {
        const first = request.claims[0];
        var second = first;
        second.tuple = "another-tuple";
        const command: native.ClaimCommand = .{ .binding = binding.?, .claims = &.{ first, second, first } };
        try std.testing.expectEqual(@as(usize, 2), (try command.commitment()).count);
        var reordered = command;
        reordered.claims = &.{ second, first };
        try std.testing.expectEqual(try command.digest(), try reordered.digest());
        reordered.claims = &.{first};
        try std.testing.expect(!std.mem.eql(u8, &try command.digest(), &try reordered.digest()));
        second = first;
        second.parent_key = "conflicting-owner";
        reordered.claims = &.{ first, second };
        try std.testing.expectError(error.UniqueConstraintViolation, reordered.digest());
        const Claim = @TypeOf(first);
        var full: [128]Claim = undefined;
        for (&full, 0..) |*claim, i| {
            claim.* = first;
            claim.tuple = try std.fmt.allocPrint(arena.allocator(), "tuple-{d}", .{i});
            claim.parent_key = try std.fmt.allocPrint(arena.allocator(), "row-{d}", .{i});
        }
        reordered.claims = &full;
        const maximum = try reordered.commitment();
        try std.testing.expectEqual(@as(usize, 128), maximum.count);
        std.mem.reverse(Claim, &full);
        try std.testing.expectEqual(maximum.digest, try reordered.digest());
        var overflow: [129]Claim = undefined;
        @memcpy(overflow[0..128], &full);
        overflow[128] = full[0];
        reordered.claims = &overflow;
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, reordered.commitment());
    }
    var result = try claimStep(alloc, &driver, &driver, plan.before.name, request, .{});
    defer result.deinit();
    try std.testing.expect(result == .applied);
    try std.testing.expectEqual(@as(usize, 1), driver.claim_batches);
    try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
    try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
    try std.testing.expectEqual(@as(u64, 9), result.applied.root_generation);
    driver.root_generation += 1;
    try std.testing.expectEqual(@as(u64, 9), result.applied.root_generation);
    try std.testing.expect(result.needsSigning());
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 3, 5));
    result.applied.outcome.value.position.sequence += 1;
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 3, 4));
    result.applied.outcome.value.position.sequence -= 1;
    result.applied.root_generation += 1;
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 3, 4));
    result.applied.root_generation -= 1;
    try result.signNative(root, 2, 3, 4);
    try std.testing.expect(!result.needsSigning());
    try result.response().validate(request);
    const signed = result.response().applied;
    const delivery = try result.response().requireSignedDelivery(request);
    try std.testing.expectEqual(try signed.outcome.deliveryDigest(), try delivery.outcome.deliveryDigest());
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, @as(ClaimResponse, .acknowledged).requireSignedDelivery(request));
    var unsigned = signed;
    unsigned.signed_delivery = null;
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (ClaimResponse{ .applied = unsigned }).requireSignedDelivery(request));
    const not_a_publication: publication.SignedReceipt = .{
        .receipt = .{
            .owner = permission.owner.owner,
            .action = .validated,
            .position = .{ .native = signed.outcome.position },
            .candidate_digest = permission.head.candidate_digest,
            .reservation_digest = permission.head.reservation_digest,
            .evidence_digest = signed.outcome.claims_digest,
        },
        .proof = signed.signed_delivery.?,
    };
    try std.testing.expectError(error.InvalidIndexOwnerReceipt, not_a_publication.verifySignature());
    try std.testing.expectEqual(@as(u64, 9), signed.signed_delivery.?.root_generation);
    var bad = signed;
    bad.signed_delivery.?.signature[0] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (ClaimResponse{ .applied = bad }).validate(request));
    bad = signed;
    bad.root_generation += 1;
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (ClaimResponse{ .applied = bad }).validate(request));
    bad = signed;
    bad.outcome.position.sequence += 1;
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (ClaimResponse{ .applied = bad }).validate(request));
    bad = signed;
    bad.outcome.claims_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (ClaimResponse{ .applied = bad }).validate(request));
    bad = signed;
    bad.outcome.claims_added = 2;
    var duplicates = request;
    duplicates.claims = &.{ request.claims[0], request.claims[0] };
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (ClaimResponse{ .applied = bad }).validate(duplicates));
    result.signingFailed(.identity_unavailable);
    try std.testing.expect(result.applied.signed_delivery == null);
    try std.testing.expectEqual(signed.outcome.position, result.applied.outcome.value.position);
    try result.signNative(root, 2, 3, 4);
    driver = .{ .value = permission, .binding = binding };
    try std.testing.expectError(error.GenerationPublicationChanged, claimStep(alloc, &driver, &driver, "wrong table", request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    driver.value.head = prepared.head;
    driver.value.owner = prepared.owners[0];
    try std.testing.expectError(error.GenerationPublicationChanged, claimStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    driver = .{ .value = permission };
    try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, claimStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.claim_batches);
    try std.testing.expectEqual(@as(usize, 0), driver.admissions);
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
    try std.testing.expect(!pending.needsSigning());
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, pending.signNative(root, 2, 3, 4));
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, pending.response().requireSignedDelivery(request));

    const build_control: Request = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .build, .build_budget = .{ .records = 7 } };
    driver = .{ .value = permission, .binding = binding };
    {
        var built = try step(alloc, &driver, &driver, plan.before.name, build_control, .{});
        defer built.deinit();
        try std.testing.expect(built == .building);
        try std.testing.expectEqual(@as(usize, 1), driver.build_pages);
        try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
        try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
        try std.testing.expectEqual(@as(usize, 0), driver.admissions);
        try std.testing.expect(!built.needsSigning());
        try (try built.response()).validate(build_control);
        driver.root_generation += 1;
        try std.testing.expectEqual(@as(?u64, 9), built.rootGeneration());
        var wrong_control = build_control;
        wrong_control.operation = .reserve;
        wrong_control.build_budget = null;
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, (try built.response()).validate(wrong_control));
    }
    driver = .{ .value = permission, .binding = binding };
    try std.testing.expectError(error.GenerationPublicationChanged, step(alloc, &driver, &driver, "wrong table", build_control, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    driver = .{ .value = permission };
    try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, step(alloc, &driver, &driver, plan.before.name, build_control, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.admissions);
    driver = .{ .value = permission, .binding = binding };
    driver.binding.?.owner_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, step(alloc, &driver, &driver, plan.before.name, build_control, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.build_pages);
    signal.store(true, .release);
    driver = .{ .value = permission, .binding = binding };
    try std.testing.expectError(error.Canceled, step(alloc, &driver, &driver, plan.before.name, build_control, context));
    try std.testing.expectEqual(@as(usize, 0), driver.permission_reads);
    signal.store(false, .release);
    driver.pending = true;
    driver.cancel_after_commit = &signal;
    var pending_build = try step(alloc, &driver, &driver, plan.before.name, build_control, context);
    defer pending_build.deinit();
    try std.testing.expect(signal.load(.acquire));
    try std.testing.expect(pending_build.building.outcome.value.advanced.replication_pending);
    try (try pending_build.response()).validate(build_control);
    {
        const source_request: Request = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .source };
        signal.store(false, .release);
        driver = .{ .value = permission, .binding = binding };
        var source_result = try step(alloc, &driver, &driver, plan.before.name, source_request, .{});
        defer source_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
        try std.testing.expectEqual(@as(usize, 0), driver.inspections);
        try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
        try std.testing.expect(source_result.needsSigning());
        try std.testing.expectError(error.IndexOwnerReceiptChanged, source_result.signNative(root, 2, 3, 5));
        source_result.source.root_generation += 1;
        try std.testing.expectError(error.IndexOwnerReceiptChanged, source_result.signNative(root, 2, 3, 4));
        source_result.source.root_generation -= 1;
        try source_result.signNative(root, 2, 3, 4);
        const response = (try source_result.response()).source;
        const encoded_response = try std.json.Stringify.valueAlloc(alloc, try source_result.response(), .{});
        defer alloc.free(encoded_response);
        var roundtrip = try std.json.parseFromSlice(ControlResponse, alloc, encoded_response, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        defer roundtrip.deinit();
        var decoded = try roundtrip.value.source.decodeSigned(alloc, source_request);
        defer decoded.deinit();
        try std.testing.expectEqualStrings("logical\x00\xff\x80", decoded.batch.?.rows()[0].tuple);
        try std.testing.expectEqual(@as(u64, 9), response.signed_source.?.root_generation);
        var bad_source = response;
        bad_source.signed_source.?.signature[0] ^= 1;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, bad_source.decodeSigned(alloc, source_request));
        bad_source = response;
        bad_source.signed_source = signed.signed_delivery;
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, bad_source.decodeSigned(alloc, source_request));
        bad_source = response;
        bad_source.signed_source.?.authority.native[0] ^= 1;
        bad_source.signed_source = try bad_source.signed_source.?.sign(root, try response.observation.headerCommitment());
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, bad_source.decodeSigned(alloc, source_request));
        bad_source = response;
        const bad_payload = try alloc.dupe(u8, bad_source.observation.payload_b64.?);
        defer alloc.free(bad_payload);
        bad_payload[0] = if (bad_payload[0] == 'A') 'B' else 'A';
        bad_source.observation.payload_b64 = bad_payload;
        try std.testing.expectError(error.InvalidPrivateIndexClaimInventory, bad_source.decodeSigned(alloc, source_request));
        {
            const row = decoded.batch.?.rows()[0];
            const target_command: native.ClaimCommand = .{ .binding = decoded.binding, .claims = &.{.{ .tuple = row.tuple, .parent_table = decoded.batch.?.scope.table_name, .parent_key = row.key, .schema_version = decoded.binding.physical_candidate.schema_version }} };
            const target_outcome: native.ClaimOutcome = .{ .binding = decoded.binding, .claims_digest = try target_command.digest(), .position = .{ .namespace = decoded.binding.fence.namespace, .sequence = 3 }, .duplicate = false, .claims_added = 1 };
            const target_signed: publication.SignedClaimDelivery = .{ .outcome = target_outcome, .proof = try response.signed_source.?.sign(root, try target_outcome.deliveryDigest()) };
            const close_request: ClaimRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .closure = .{ .ordinal = 1, .batch_digest = decoded.batch.?.header.digest(), .targets = .{ .items = &.{.{ .index = 0, .signed = target_signed }} } } };
            const encoded_close = try std.json.Stringify.valueAlloc(alloc, close_request, .{});
            defer alloc.free(encoded_close);
            var parsed_close = try std.json.parseFromSlice(ClaimRequest, alloc, encoded_close, .{ .allocate = .alloc_always });
            defer parsed_close.deinit();
            driver = .{ .value = permission, .binding = binding };
            var closed = try claimStep(alloc, &driver, &driver, plan.before.name, parsed_close.value, .{});
            defer closed.deinit();
            try std.testing.expect(closed == .closed and closed.needsSigning());
            try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
            try std.testing.expectEqual(@as(usize, 0), driver.inspections);
            try std.testing.expectEqual(@as(usize, 0), driver.claim_batches);
            closed.closed.root_generation += 1;
            try std.testing.expectError(error.IndexOwnerReceiptChanged, closed.signNative(root, 2, 3, 4));
            closed.closed.root_generation -= 1;
            try closed.signNative(root, 2, 3, 4);
            try closed.response().validate(close_request);
            try std.testing.expectEqual(native.ClaimCommand.memberDigest(target_command.claims[0]), closed.closed.owned.data.parts.items[0].inventory_xor);
            var missing = close_request;
            missing.closure.?.targets.items = &.{};
            try std.testing.expectError(error.InvalidIndexPublicationCandidate, claimStep(alloc, &driver, &driver, plan.before.name, missing, .{}));
            var bad_close = close_request;
            bad_close.closure.?.batch_digest[0] ^= 1;
            try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, claimStep(alloc, &driver, &driver, plan.before.name, bad_close, .{}));
        }
        driver.pending = true;
        var pending_source = try step(alloc, &driver, &driver, plan.before.name, source_request, .{});
        defer pending_source.deinit();
        try std.testing.expect(!pending_source.needsSigning());
        try std.testing.expect(pending_source.source.outcome.batch == null);
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (try pending_source.response()).source.decodeSigned(alloc, source_request));
    }
}

test "relational index publication native receiver signs pinned authority and preserves progress on signing failure" {
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
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .public_key = pair.public_key.toBytes(), .root_incarnation = 7 };
    const binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7 };
    const view: publication.NativeSigningView = .{
        .identity = .{ .metadata_incarnation = binding.metadata_incarnation, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
        .reporter_incarnation = 4,
        .binding_digest = try binding.digest(),
    };
    const permission: publication.OwnerDecision = .{ .metadata_group_id = 13, .metadata_incarnation = binding.metadata_incarnation, .head = prepared.head, .owner = prepared.owners[0], .disposition = .execute, .dispatch = dispatch.value, .inputs = inputs.value, .native_signing = view };
    const request: Request = .{ .id = plan.id, .group_id = plan.owners[0].range.group_id, .table_id = plan.before.table_id, .operation = .fence };
    var driver: TestDriver = .{ .value = permission, .produce_receipt = true };
    {
        var admitted = try step(alloc, &driver, &driver, plan.before.name, request, .{});
        defer admitted.deinit();
        try std.testing.expect(!admitted.needsSigning());
        try std.testing.expectError(error.NativeIndexSigningUnavailable, admitted.signNative(root, 2, 3, 4));
    }
    var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
    defer result.deinit();
    try std.testing.expect(result.needsSigning());
    const unsigned = result.outcome().?;
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 3, 5));
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 8, 3, 4));
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 8, 4));
    var replaced = root;
    replaced.root_incarnation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, result.signNative(replaced, 2, 3, 4));
    result.signingFailed(.authority_changed);
    const failed = try result.response();
    try failed.validate(request);
    try std.testing.expectEqual(SigningFailure.authority_changed, failed.controlled.signing_failure.?);
    try std.testing.expectEqualDeep(unsigned, result.outcome().?);
    // A later routing lookup or permission read cannot relabel this effect.
    driver.root_generation = 10;
    driver.value.head.revision += 1;
    try result.signNative(root, 2, 3, 4);
    const response = try result.response();
    try response.validate(request);
    const ack = response.controlled.signed_ack.?;
    try std.testing.expectEqual(@as(u64, 9), ack.signed.proof.root_generation);
    try std.testing.expectEqual(permission.head.revision, ack.expected_revision);
    try std.testing.expect(response.controlled.signing_failure == null);
    try std.testing.expect(!result.needsSigning());
    const bytes = try std.json.Stringify.valueAlloc(alloc, response, .{});
    defer alloc.free(bytes);
    var wire = try std.json.parseFromSlice(ControlResponse, alloc, bytes, .{ .ignore_unknown_fields = false });
    defer wire.deinit();
    try wire.value.validate(request);
    var invalid = response;
    invalid.controlled.root_generation += 1;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, invalid.validate(request));
    invalid = response;
    invalid.controlled.signing_failure = .signing_failed;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, invalid.validate(request));
    invalid = response;
    invalid.controlled.outcome.receipt = null;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, invalid.validate(request));
    driver.value = permission;
    driver.receipt_result = unsigned;
    var recovered = try step(alloc, &driver, &driver, plan.before.name, request, .{});
    defer recovered.deinit();
    try std.testing.expect(recovered == .recovered and recovered.needsSigning());
    driver.root_generation = 11;
    try recovered.signNative(root, 2, 3, 4);
    const recovered_response = try recovered.response();
    try recovered_response.validate(request);
    try std.testing.expectEqual(@as(u64, 10), recovered_response.recovered.signed_ack.?.signed.proof.root_generation);
    try std.testing.expectEqual(@as(usize, 1), driver.controls);
    {
        const progress = local.storage_db_relational_index_publication_progress;
        const accepted = try progress.acknowledge(permission.head, permission.owner, permission.head.revision, unsigned.receipt.?);
        var owner = accepted.owner;
        owner.attestations[0] = try ack.signed.commitment();
        const readonly: publication.OwnerDecision = .{
            .metadata_group_id = permission.metadata_group_id,
            .metadata_incarnation = permission.metadata_incarnation,
            .head = accepted.head,
            .owner = owner,
            .disposition = .acknowledged,
            .attestation = .{ .table_name = plan.before.name, .identity = try dispatch.value.nativeIdentity(owner.owner), .signing = view },
        };
        try readonly.requireRequest(request.lookup(.permission));
        try std.testing.expectError(error.InvalidGenerationPublication, readonly.nativeExecutionIdentity(request.lookup(.permission)));
        try std.testing.expectError(error.InvalidGenerationPublication, readonly.nativeControl(request.lookup(.permission), unsigned.binding));
        var attest_driver: TestDriver = .{ .value = readonly, .receipt_result = unsigned, .root_generation = 27 };
        var renewed = try step(alloc, &attest_driver, &attest_driver, plan.before.name, request, .{});
        defer renewed.deinit();
        try std.testing.expect(renewed == .recovered and renewed.needsSigning());
        try std.testing.expectEqual(@as(usize, 1), attest_driver.recoveries);
        try std.testing.expectEqual(@as(usize, 0), attest_driver.inspections);
        try std.testing.expectEqual(@as(usize, 0), attest_driver.admissions);
        try std.testing.expectEqual(@as(usize, 0), attest_driver.controls);
        attest_driver.root_generation = 28;
        try renewed.signNative(root, 2, 3, 4);
        const refreshed_response = try renewed.response();
        try refreshed_response.validate(request);
        try std.testing.expectEqual(@as(u64, 27), refreshed_response.recovered.signed_ack.?.signed.proof.root_generation);
        try std.testing.expectEqual(accepted.head.revision, refreshed_response.recovered.signed_ack.?.expected_revision);
        try std.testing.expectEqualDeep(unsigned, renewed.outcome().?);
        const reads = attest_driver.recoveries;
        try std.testing.expectError(error.GenerationPublicationChanged, step(alloc, &attest_driver, &attest_driver, "wrong table", request, .{}));
        try std.testing.expectEqual(reads, attest_driver.recoveries);
        attest_driver.receipt_result = null;
        try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, step(alloc, &attest_driver, &attest_driver, plan.before.name, request, .{}));
        var different_cut = unsigned;
        different_cut.position.sequence += 1;
        different_cut.receipt.?.position.native.sequence += 1;
        try different_cut.validateRecovery(.{ .identity = readonly.attestation.?.identity, .effect = .fenced });
        attest_driver.receipt_result = different_cut;
        try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, step(alloc, &attest_driver, &attest_driver, plan.before.name, request, .{}));
        try std.testing.expectEqual(@as(usize, 0), attest_driver.admissions);
        try std.testing.expectEqual(@as(usize, 0), attest_driver.controls);
        try std.testing.expectEqual(error.PrivateIndexRootRecoveryRequired, claimHttpFailure(503, "PrivateIndexRootRecoveryRequired").?);
    }
    driver.receipt_result.?.receipt = null;
    driver.receipt_result.?.replication_pending = true;
    driver.receipt_result.?.error_name = "ReplicationOutboxPending";
    var pending = try step(alloc, &driver, &driver, plan.before.name, request, .{});
    defer pending.deinit();
    try std.testing.expect(!pending.needsSigning());
    try std.testing.expectError(error.NativeIndexSigningUnavailable, pending.signNative(root, 2, 3, 4));
    try (try pending.response()).validate(request);
    try std.testing.expectEqual(@as(usize, 1), driver.controls);
    driver.value.native_signing.?.binding_digest[0] ^= 1;
    const inspections = driver.inspections;
    try std.testing.expectError(error.InvalidCatalogRecord, step(alloc, &driver, &driver, plan.before.name, request, .{}));
    try std.testing.expectEqual(inspections, driver.inspections);
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
        try (try result.response()).validate(request);
        try std.testing.expect((try result.response()) == .admitted);
        try std.testing.expect(result.outcome().?.receipt == null);
        try std.testing.expect(result.rootGeneration() == null);
        try std.testing.expectEqual(@as(usize, 1), driver.permission_reads);
        try std.testing.expectEqual(@as(usize, 1), driver.admission_reads);
        try std.testing.expectEqual(@as(usize, 1), driver.admissions);
        try std.testing.expectEqual(@as(usize, 0), driver.controls);
    }
    {
        var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
        defer result.deinit();
        try std.testing.expect(result == .controlled);
        try (try result.response()).validate(request);
        var invalid = try result.response();
        invalid.controlled.root_generation = 0;
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, invalid.validate(request));
        invalid = try result.response();
        invalid.controlled.outcome.binding.id[0] ^= 1;
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, invalid.validate(request));
        var foreign_request = request;
        foreign_request.table_id += 1;
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, (try result.response()).validate(foreign_request));
        foreign_request = request;
        foreign_request.operation = .reserve;
        const admitted: ControlResponse = .{ .admitted = result.controlled.outcome.value };
        try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, admitted.validate(foreign_request));
        try std.testing.expectEqual(@as(?u64, 9), result.rootGeneration());
        driver.root_generation = 10;
        try std.testing.expectEqual(@as(?u64, 9), result.rootGeneration());
        driver.root_generation = 9;
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
        try (try result.response()).validate(request);
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
