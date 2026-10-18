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
const delivery_journal = @import("../metadata/relational_index_delivery_journal.zig");
const operation = local.api_operation;

pub const DeliveryProducer = @import("relational_index_delivery_producer.zig").Producer;

test {
    _ = @import("http_client.zig");
}

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
    operation: enum { fence, reserve, build, source, validate, install, release, cancel, cancel_complete, cancel_release },
    build_budget: ?local.storage_db_relational_index_jobs.Budget = null,

    pub fn validate(self: Request, group_id: u64) !void {
        if (self.group_id != group_id) return error.GenerationPublicationChanged;
        if (self.operation != .build and self.operation != .validate and self.operation != .install and self.build_budget != null) return error.InvalidGenerationPublication;
        if (self.build_budget) |budget| try budget.validate();
        try self.lookup(.permission).validate();
    }

    pub fn lookup(self: Request, projection: @FieldType(publication.OwnerRequest, "projection")) publication.OwnerRequest {
        return .{
            .table_id = self.table_id,
            .group_id = self.group_id,
            .id = self.id,
            .operation = switch (self.operation) {
                .fence => .fence,
                .reserve => .reserve,
                .build, .source => .build,
                .validate => .validate,
                .install => .install,
                .release => .release,
                .cancel, .cancel_complete => .cancel,
                .cancel_release => .cancel_release,
            },
            .projection = projection,
        };
    }
};

/// Own the decoded response, including a pending error name. An admitted
/// outcome without a receipt is durable progress, not a fenced/reserved ACK.
pub const Result = union(enum) {
    acknowledged,
    raft: RaftControlResponse,
    raft_build: RaftBuildControlResponse,
    raft_source: RaftSourceResult,
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
    building: struct {
        root_generation: u64,
        expected_revision: u64 = 0,
        outcome: std.json.Parsed(native.BuildResponse),
        audit_signing: ?publication.NativeSigningView = null,
        signed_audit: ?publication.SignedAudit = null,
        signed_receipt: ?publication.SignedReceipt = null,
        signing_failure: ?SigningFailure = null,
    },
    source: SourceResult,
    installing: InstallationResult,
    releasing: ReleaseResult,
    canceling: struct { request: Request, result: CancellationResult },

    pub fn deinit(self: *Result) void {
        switch (self.*) {
            .acknowledged, .raft, .raft_build => {},
            .recovered => |value| value.deinit(),
            .applied => |value| value.deinit(),
            .controlled => |value| value.outcome.deinit(),
            .building => |value| value.outcome.deinit(),
            .source => |*value| value.deinit(),
            .raft_source => |*value| value.wire.deinit(),
            .installing => |*value| value.outcome.deinit(),
            .releasing => |*value| value.outcome.deinit(),
            .canceling => |*value| value.result.deinit(),
        }
        self.* = undefined;
    }

    /// Borrowed until deinit; copying an outcome does not own error_name.
    pub fn outcome(self: *const Result) ?native.Outcome {
        return switch (self.*) {
            .acknowledged, .raft, .raft_build, .building, .source, .raft_source, .installing, .releasing, .canceling => null,
            .recovered => |value| value.outcome.value,
            .applied => |value| value.value,
            .controlled => |value| value.outcome.value,
        };
    }

    /// Admission alone has no publication receipt. Never reconstruct a root
    /// for signing from current routing after this result's lease was released.
    pub fn rootGeneration(self: *const Result) ?u64 {
        return switch (self.*) {
            .raft => |value| value.root_generation,
            .raft_build => |value| value.root_generation,
            .recovered => |value| value.root_generation,
            .controlled => |value| value.root_generation,
            .building => |value| value.root_generation,
            .source => |value| value.root_generation,
            .raft_source => |value| value.root_generation,
            .installing => |value| value.root_generation,
            .releasing => |value| value.root_generation,
            .canceling => |value| switch (value.result) {
                .completed => |item| item.root_generation,
                .released => |item| item.root_generation,
                else => null,
            },
            .acknowledged, .applied => null,
        };
    }

    fn signingState(self: *Result) ?*SigningState {
        return switch (self.*) {
            .controlled => |*value| &value.signing,
            .recovered => |*value| &value.signing,
            .acknowledged, .raft, .raft_build, .applied, .building, .source, .raft_source, .installing, .releasing, .canceling => null,
        };
    }

    pub fn needsSigning(self: *Result) bool {
        if (self.* == .canceling) return switch (self.canceling.result) {
            .completed => |*value| value.needsSigning(),
            .released => |*value| value.needsSigning(),
            else => false,
        };
        if (self.* == .installing) return self.installing.needsSigning();
        if (self.* == .releasing) return self.releasing.needsSigning();
        if (self.* == .source) return self.source.needsSigning();
        if (self.* == .building) {
            const value = self.building;
            if (value.audit_signing == null or value.signed_audit != null or value.signed_receipt != null or value.signing_failure != null) return false;
            const audit = value.outcome.value.physicalAudit() orelse return false;
            _ = audit.readyDigest() catch return false;
            if (value.outcome.value == .validated and value.outcome.value.validated.logical.receipt == null) return false;
            if (value.outcome.value == .validated and value.expected_revision == 0) return false;
            return true;
        }
        const state = self.signingState() orelse return false;
        const value = self.outcome() orelse return false;
        return state.context != null and state.ack == null and value.receipt != null;
    }

    /// Signing is post-commit decoration, never another physical effect. The
    /// caller must preserve the outcome if the local identity is unavailable.
    pub fn signNative(self: *Result, root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        if (self.* == .canceling) return switch (self.canceling.result) {
            .completed => |*value| value.signNative(root, node_id, store_id, reporter_incarnation),
            .released => |*value| value.signNative(root, node_id, store_id, reporter_incarnation),
            else => error.NativeIndexSigningUnavailable,
        };
        if (self.* == .installing) return self.installing.signNative(root, node_id, store_id, reporter_incarnation);
        if (self.* == .releasing) return self.releasing.signNative(root, node_id, store_id, reporter_incarnation);
        if (self.* == .source) return self.source.signNative(root, node_id, store_id, reporter_incarnation);
        if (self.* == .building) {
            const value = &self.building;
            const view = value.audit_signing orelse return error.NativeIndexSigningUnavailable;
            if (value.root_generation == 0) return error.NativeIndexSigningUnavailable;
            const audit = value.outcome.value.physicalAudit() orelse return error.NativeIndexSigningUnavailable;
            try view.validate(audit.expected.metadata_incarnation);
            if (node_id != view.identity.node_id or store_id != view.identity.store_id or reporter_incarnation != view.reporter_incarnation or
                audit.audit.root_incarnation != view.identity.root_incarnation) return error.IndexOwnerReceiptChanged;
            const digest = try audit.readyDigest();
            const proof = try (publication.RootProof{ .identity = view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = view.binding_digest }, .root_generation = value.root_generation, .signature = undefined }).sign(root, digest);
            const signed: publication.SignedAudit = .{ .outcome_digest = digest, .proof = proof };
            _ = try signed.verifiedDigest(audit);
            const logical: ?publication.SignedReceipt = if (value.outcome.value == .validated) receipt: {
                if (value.expected_revision == 0 or value.expected_revision == std.math.maxInt(u64)) return error.NativeIndexSigningUnavailable;
                const sealed = value.outcome.value.validated.logical;
                try sealed.validate();
                if (!std.meta.eql(sealed.command, try audit.logicalCommand())) return error.InvalidPrivateIndexNativeReceipt;
                const committed = sealed.receipt orelse return error.NativeIndexSigningUnavailable;
                const receipt_proof = try (publication.RootProof{ .identity = view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = view.binding_digest }, .root_generation = value.root_generation, .signature = undefined }).sign(root, try committed.digest());
                const signed_receipt: publication.SignedReceipt = .{ .receipt = committed, .proof = receipt_proof };
                try signed_receipt.verifySignature();
                break :receipt signed_receipt;
            } else null;
            // Publish both decorations only after both signatures succeed.
            value.signed_audit = signed;
            value.signed_receipt = logical;
            value.signing_failure = null;
            return;
        }
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
        if (self.* == .canceling) {
            switch (self.canceling.result) {
                .completed => |*value| value.signingFailed(failure),
                .released => |*value| value.signingFailed(failure),
                else => {},
            }
            return;
        }
        if (self.* == .releasing) {
            if (self.releasing.signed_receipt == null) self.releasing.signing_failure = failure;
            return;
        }
        if (self.* == .installing) {
            if (self.installing.signed_receipt == null) self.installing.signing_failure = failure;
            return;
        }
        if (self.* == .building) {
            if (self.building.signed_audit == null and self.building.signed_receipt == null) self.building.signing_failure = failure;
            return;
        }
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
            .raft => |value| .{ .raft = value },
            .raft_build => |value| .{ .raft_build = value },
            .applied => |value| .{ .admitted = value.value },
            .controlled => |value| .{ .controlled = .{ .root_generation = value.root_generation, .outcome = value.outcome.value, .signed_ack = value.signing.ack, .signing_failure = value.signing.failure } },
            .recovered => |value| .{ .recovered = .{ .root_generation = value.root_generation, .outcome = value.outcome.value orelse return error.InvalidPrivateIndexNativeReceipt, .signed_ack = value.signing.ack, .signing_failure = value.signing.failure } },
            .building => |value| .{ .building = .{ .root_generation = value.root_generation, .expected_revision = value.expected_revision, .outcome = value.outcome.value, .signed_audit = value.signed_audit, .signed_receipt = value.signed_receipt, .signing_failure = value.signing_failure } },
            .source => |*value| .{ .source = value.response() },
            .raft_source => |*value| .{ .raft_source = value.response() },
            .installing => |*value| .{ .installing = value.response() },
            .releasing => |*value| .{ .releasing = value.response() },
            .canceling => |*value| .{ .canceling = try value.result.response(value.request) },
        };
    }
};

pub const ReleaseControlResponse = struct {
    root_generation: u64,
    expected_revision: u64,
    outcome: native.ReleaseOutcome,
    signed_receipt: ?publication.SignedReceipt = null,
    signing_failure: ?SigningFailure = null,

    pub fn validate(self: @This(), request: Request) !void {
        if (request.operation != .release or self.root_generation == 0 or self.expected_revision == 0 or self.expected_revision == std.math.maxInt(u64)) return error.InvalidPrivateIndexNativeReceipt;
        try self.outcome.validate();
        const binding = self.outcome.intent.installed.command.validation.command.binding;
        if (binding.fence.namespace.table_id != request.table_id or binding.fence.owner_group_id != request.group_id or
            !std.mem.eql(u8, &binding.id, &request.id)) return error.InvalidPrivateIndexNativeReceipt;
        if (self.signing_failure != null and (self.signed_receipt != null or self.outcome.receipt == null)) return error.InvalidPrivateIndexNativeReceipt;
        if (self.signed_receipt) |signed| {
            if (signed.proof.root_generation != self.root_generation or
                !std.meta.eql(signed.receipt, self.outcome.receipt orelse return error.InvalidPrivateIndexNativeReceipt)) return error.InvalidPrivateIndexNativeReceipt;
            try signed.verifySignature();
        }
    }

    fn ack(self: @This(), request: Request) !?publication.Ack {
        try self.validate(request);
        const signed = self.signed_receipt orelse return null;
        const result: publication.Ack = .{ .table_id = request.table_id, .expected_revision = self.expected_revision, .signed = signed };
        try result.validate();
        return result;
    }
};

const ReleaseResult = struct {
    root_generation: u64,
    expected_revision: u64,
    outcome: std.json.Parsed(native.ReleaseOutcome),
    signing: publication.NativeSigningView,
    signed_receipt: ?publication.SignedReceipt = null,
    signing_failure: ?SigningFailure = null,

    fn response(self: *const @This()) ReleaseControlResponse {
        return .{ .root_generation = self.root_generation, .expected_revision = self.expected_revision, .outcome = self.outcome.value, .signed_receipt = self.signed_receipt, .signing_failure = self.signing_failure };
    }
    fn needsSigning(self: *const @This()) bool {
        return self.outcome.value.receipt != null and self.signed_receipt == null and self.signing_failure == null;
    }
    fn signNative(self: *@This(), root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        if (!self.needsSigning() or self.root_generation == 0 or self.expected_revision == 0 or self.expected_revision == std.math.maxInt(u64)) return error.NativeIndexSigningUnavailable;
        try self.outcome.value.validate();
        const view = self.signing;
        try view.validate(self.outcome.value.intent.installed.command.validation.command.expected.metadata_incarnation);
        if (node_id != view.identity.node_id or store_id != view.identity.store_id or reporter_incarnation != view.reporter_incarnation) return error.IndexOwnerReceiptChanged;
        const receipt = self.outcome.value.receipt.?;
        const proof = try (publication.RootProof{ .identity = view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = view.binding_digest }, .root_generation = self.root_generation, .signature = undefined }).sign(root, try receipt.digest());
        const signed: publication.SignedReceipt = .{ .receipt = receipt, .proof = proof };
        try signed.verifySignature();
        self.signed_receipt = signed;
        self.signing_failure = null;
    }
};

pub const control_remaining_ms_header = "X-Antfly-Index-Control-Remaining-Ms";
pub const max_control_request_bytes = 4096;

pub const InstallationControlResponse = struct {
    root_generation: u64,
    expected_revision: u64,
    outcome: native.InstallationResponse,
    signed_audit: ?publication.SignedAudit = null,
    signed_receipt: ?publication.SignedReceipt = null,
    signing_failure: ?SigningFailure = null,

    pub fn validate(self: @This(), request: Request) !void {
        if (request.operation != .install or self.root_generation == 0 or self.expected_revision == 0 or self.expected_revision == std.math.maxInt(u64)) return error.InvalidPrivateIndexNativeReceipt;
        const physical = self.outcome.physicalAudit();
        const binding = physical.binding;
        if (binding.fence.namespace.table_id != request.table_id) return error.InvalidPrivateIndexNativeReceipt;
        var build = try native.BuildRequest.init(.{ .id = request.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = request.group_id }, binding, request.build_budget orelse .{});
        build.target_expectation = physical.expected;
        try physical.validateFor(build);
        if (self.outcome == .installed) try self.outcome.validateFor(.{ .intent = self.outcome.installed.logical.command.intent(), .budget = build.budget });
        if ((self.signed_audit != null) != (self.signed_receipt != null)) return error.InvalidPrivateIndexNativeReceipt;
        if (self.signing_failure != null and (self.signed_audit != null or self.outcome != .installed or self.outcome.installed.logical.receipt == null)) return error.InvalidPrivateIndexNativeReceipt;
        if (self.signed_receipt) |signed| {
            if (self.outcome != .installed or signed.proof.root_generation != self.root_generation or
                !std.meta.eql(self.outcome.installed.logical.receipt orelse return error.InvalidPrivateIndexNativeReceipt, signed.receipt)) return error.InvalidPrivateIndexNativeReceipt;
            try (publication.InstallationWitness{ .outcome = physical, .command = self.outcome.installed.logical.command, .signed = self.signed_audit.? }).verifyFor(signed);
        }
    }

    pub fn ack(self: @This(), request: Request) !?publication.Ack {
        try self.validate(request);
        const signed = self.signed_receipt orelse return null;
        const value = self.outcome.installed;
        const result: publication.Ack = .{ .table_id = request.table_id, .expected_revision = self.expected_revision, .signed = signed, .installation = .{ .outcome = value.physical, .command = value.logical.command, .signed = self.signed_audit.? } };
        try result.validate();
        return result;
    }
};

const InstallationResult = struct {
    root_generation: u64,
    expected_revision: u64,
    outcome: std.json.Parsed(native.InstallationResponse),
    signing: publication.NativeSigningView,
    signed_audit: ?publication.SignedAudit = null,
    signed_receipt: ?publication.SignedReceipt = null,
    signing_failure: ?SigningFailure = null,

    fn response(self: *const @This()) InstallationControlResponse {
        return .{ .root_generation = self.root_generation, .expected_revision = self.expected_revision, .outcome = self.outcome.value, .signed_audit = self.signed_audit, .signed_receipt = self.signed_receipt, .signing_failure = self.signing_failure };
    }
    fn needsSigning(self: *const @This()) bool {
        return self.outcome.value == .installed and self.outcome.value.installed.logical.receipt != null and self.signed_audit == null and self.signed_receipt == null and self.signing_failure == null;
    }
    fn signNative(self: *@This(), root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        if (!self.needsSigning() or self.root_generation == 0 or self.expected_revision == 0 or self.expected_revision == std.math.maxInt(u64)) return error.NativeIndexSigningUnavailable;
        const value = self.outcome.value.installed;
        const view = self.signing;
        try view.validate(value.physical.expected.metadata_incarnation);
        if (node_id != view.identity.node_id or store_id != view.identity.store_id or reporter_incarnation != view.reporter_incarnation or
            value.physical.audit.root_incarnation != view.identity.root_incarnation) return error.IndexOwnerReceiptChanged;
        try self.outcome.value.validateFor(.{ .intent = value.logical.command.intent(), .budget = .{} });
        const template: publication.RootProof = .{ .identity = view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = view.binding_digest }, .root_generation = self.root_generation, .signature = undefined };
        const digest = try value.physical.readyDigest();
        const signed_audit: publication.SignedAudit = .{ .outcome_digest = digest, .proof = try template.sign(root, digest) };
        const receipt = value.logical.receipt orelse return error.NativeIndexSigningUnavailable;
        const signed_receipt: publication.SignedReceipt = .{ .receipt = receipt, .proof = try template.sign(root, try receipt.digest()) };
        try (publication.InstallationWitness{ .outcome = value.physical, .command = value.logical.command, .signed = signed_audit }).verifyFor(signed_receipt);
        self.signed_audit = signed_audit;
        self.signed_receipt = signed_receipt;
        self.signing_failure = null;
    }
};

/// Internal progress only. Neither the applying-root generation nor an unsigned
/// logical receipt grants metadata publication authority to a remote caller.
/// A Raft receipt is never projected into a native clock or native outcome.
/// The observation owns no slices and remains valid after the owner lease ends.
pub const RaftControlResponse = struct {
    root_generation: u64,
    observation: local.storage_db_relational_index_raft_contract.Observation,
    signed_ack: ?publication.Ack = null,
    signing_failure: ?SigningFailure = null,

    pub fn validate(self: @This(), request: Request) !void {
        try request.validate(request.group_id);
        if ((request.operation != .fence and request.operation != .reserve) or self.root_generation == 0) return error.InvalidControlReceiptPosition;
        const binding = self.observation.binding orelse return error.PrivateIndexRootRecoveryRequired;
        if (binding.fence.namespace.table_id != request.table_id) return error.PrivateIndexOwnerJobChanged;
        try self.observation.validate(.{ .id = request.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = request.group_id });
        const receipt = if (request.operation == .fence) self.observation.fenced else self.observation.reserved;
        if (self.signed_ack) |ack| {
            if (self.signing_failure != null or ack.table_id != request.table_id or ack.signed.proof.authority != .raft or
                ack.signed.proof.root_generation != self.root_generation or !std.meta.eql(ack.signed.receipt, receipt orelse return error.InvalidControlReceiptPosition)) return error.InvalidControlReceiptPosition;
            try ack.validate();
        }
        if (self.signing_failure != null and receipt == null) return error.InvalidControlReceiptPosition;
    }
};

pub const RaftBuildControlResponse = struct {
    root_generation: u64,
    progress: local.storage_db_relational_index_raft_contract.BuildProgress,

    pub fn validate(self: @This(), request: Request) !void {
        try request.validate(request.group_id);
        if (request.operation != .build or self.root_generation == 0 or self.progress.binding.fence.namespace.table_id != request.table_id) return error.InvalidRelationalIndexProgress;
        const b = self.progress.binding;
        const build = try local.storage_db_relational_index_raft_contract.BuildRequest.initRaft(.{ .id = request.id, .plan_digest = b.plan_digest, .owner_digest = b.owner_digest, .ordinal = b.ordinal, .namespace = b.fence.namespace, .owner_group_id = request.group_id }, b, request.build_budget orelse .{});
        try self.progress.validateFor(build);
    }
};

pub const ControlResponse = union(enum) {
    acknowledged,
    raft: RaftControlResponse,
    raft_build: RaftBuildControlResponse,
    admitted: native.Outcome,
    controlled: RootedOutcome,
    recovered: RootedOutcome,
    building: struct { root_generation: u64, expected_revision: u64 = 0, outcome: native.BuildResponse, signed_audit: ?publication.SignedAudit = null, signed_receipt: ?publication.SignedReceipt = null, signing_failure: ?SigningFailure = null },
    source: SourceResponse,
    raft_source: SourceResponse,
    installing: InstallationControlResponse,
    releasing: ReleaseControlResponse,
    canceling: CancellationControlResponse,

    pub fn cancellationAck(self: @This(), request: Request) !?publication.Ack {
        try self.validate(request);
        return if (self == .canceling) self.canceling.signed_ack else null;
    }

    pub fn releaseAck(self: @This(), request: Request) !?publication.Ack {
        try self.validate(request);
        if (self != .releasing) return null;
        return self.releasing.ack(request);
    }

    pub fn installationAck(self: @This(), request: Request) !?publication.Ack {
        try self.validate(request);
        if (self != .installing) return null;
        return self.installing.ack(request);
    }

    /// A captured metadata revision is a CAS hint, never authority. The
    /// accepting metadata transaction rechecks scope, signatures and liveness.
    pub fn validationAck(self: @This(), request: Request) !?publication.Ack {
        try self.validate(request);
        if (self != .building or self.building.outcome != .validated) return null;
        const value = self.building;
        const receipt = value.signed_receipt orelse return null;
        const witness = value.signed_audit orelse return null;
        const ack: publication.Ack = .{ .table_id = request.table_id, .expected_revision = value.expected_revision, .signed = receipt, .validation = .{ .outcome = value.outcome.validated.physical, .signed = witness } };
        try ack.validate();
        return ack;
    }

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
            .raft => |value| return value.validate(request),
            .raft_build => |value| return value.validate(request),
            .source => |value| return value.validate(request),
            .raft_source => |value| return value.validateRaft(request),
            .installing => |value| return value.validate(request),
            .releasing => |value| return value.validate(request),
            .canceling => |value| return value.validate(request),
            .building => |value| {
                if ((request.operation != .build and request.operation != .validate) or value.root_generation == 0) return error.InvalidPrivateIndexNativeReceipt;
                const binding = switch (value.outcome) {
                    .advanced => |item| item.binding,
                    .complete => |item| item.binding,
                    .inventory => |item| item.binding,
                    .generation => |item| item.binding,
                    .audit => |item| item.binding,
                    .validated => |item| item.physical.binding,
                };
                if (binding.fence.namespace.table_id != request.table_id) return error.InvalidPrivateIndexNativeReceipt;
                var build = try native.BuildRequest.init(.{ .id = request.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = request.group_id }, binding, request.build_budget orelse .{});
                if (request.operation == .validate) {
                    build.target_expectation = switch (value.outcome) {
                        .inventory => |item| item.record.expected,
                        .generation => |item| item.record.expected,
                        .audit => |item| item.expected,
                        .validated => |item| item.physical.expected,
                        else => return error.InvalidPrivateIndexNativeReceipt,
                    };
                } else if (value.outcome == .inventory or value.outcome == .generation or value.outcome.physicalAudit() != null) return error.InvalidPrivateIndexNativeReceipt;
                try value.outcome.validateFor(build);
                if ((value.signed_audit != null or value.signed_receipt != null or value.signing_failure != null) and value.outcome.physicalAudit() == null) return error.InvalidPrivateIndexNativeReceipt;
                if (value.signed_audit) |signed| {
                    if (value.signing_failure != null or signed.proof.root_generation != value.root_generation) return error.InvalidPrivateIndexNativeReceipt;
                    _ = try signed.verifiedDigest(value.outcome.physicalAudit().?);
                }
                if (value.outcome == .validated and (value.signed_audit != null) != (value.signed_receipt != null)) return error.InvalidPrivateIndexNativeReceipt;
                if (value.signed_receipt) |signed| {
                    if (value.expected_revision == 0 or value.expected_revision == std.math.maxInt(u64)) return error.InvalidPrivateIndexNativeReceipt;
                    if (value.signing_failure != null or value.outcome != .validated or value.signed_audit == null or
                        !std.meta.eql(value.outcome.validated.logical.receipt orelse return error.InvalidPrivateIndexNativeReceipt, signed.receipt)) return error.InvalidPrivateIndexNativeReceipt;
                    try signed.verifySignature();
                    const physical_proof = value.signed_audit.?.proof;
                    if (!std.meta.eql(signed.proof.identity, physical_proof.identity) or
                        !std.meta.eql(signed.proof.authority, physical_proof.authority) or
                        signed.proof.reporter_incarnation != physical_proof.reporter_incarnation or
                        signed.proof.root_generation != physical_proof.root_generation) return error.InvalidPrivateIndexNativeReceipt;
                }
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
                .build, .source, .validate, .install, .release, .cancel, .cancel_complete, .cancel_release => return error.InvalidPrivateIndexNativeReceipt,
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
        return self.validateAuthority(request, .native);
    }

    pub fn validateRaft(self: SourceResponse, request: Request) !void {
        return self.validateAuthority(request, .raft);
    }

    fn validateAuthority(self: SourceResponse, request: Request, authority: local.storage_db_relational_index_publication_progress.Authority) !void {
        try request.validate(request.group_id);
        if (request.operation != .source or self.root_generation == 0 or self.observation.binding.fence.namespace.table_id != request.table_id) return error.InvalidPrivateIndexClaimReceipt;
        if (authority == .native) try self.observation.requireIdentity(self.identity(request)) else try self.observation.requireRaftIdentity(self.identity(request));
        if (self.signed_source) |proof| {
            if (self.signing_failure != null or proof.root_generation != self.root_generation or (proof.authority == .native) != (authority == .native)) return error.InvalidPrivateIndexClaimReceipt;
            if (authority == .native) (publication.NativeSigningView{ .identity = proof.identity, .reporter_incarnation = proof.reporter_incarnation, .binding_digest = proof.authority.native }).validate(proof.identity.metadata_incarnation) catch return error.InvalidPrivateIndexClaimReceipt;
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

    pub fn decodeSignedRaft(self: SourceResponse, alloc: std.mem.Allocator, request: Request) !native.SourceRead {
        try self.validateRaft(request);
        _ = self.signed_source orelse return error.InvalidPrivateIndexClaimReceipt;
        return self.observation.decodeRaft(alloc, self.identity(request));
    }
};

pub const RaftSourceResult = struct {
    root_generation: u64,
    wire: std.json.Parsed(native.SourceWire),
    signed_source: ?publication.RootProof = null,
    signing_failure: ?SigningFailure = null,

    pub fn response(self: *const @This()) SourceResponse {
        return .{ .root_generation = self.root_generation, .observation = self.wire.value, .signed_source = self.signed_source, .signing_failure = self.signing_failure };
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
        if (self.outcome.binding.authority != .native or self.wire.value.binding.authority != .native) return error.InvalidPrivateIndexClaimReceipt;
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
        503 => inline for (.{ error.InvalidPrivateIndexClaimReceipt, error.InvalidPrivateIndexClaimInventory, error.InvalidPrivateIndexOwnerJob, error.InvalidPrivateIndexInputs, error.InvalidPrivateIndexNativeReceipt, error.PrivateIndexJobRequired, error.PrivateIndexRootRecoveryRequired, error.PrivateIndexPublicationRequired, error.PrivateIndexCancellationPending, error.PrivateIndexCancellationRequired, error.ImmutableRelationalIndexDefinitionConflict, error.InvalidIndexPublicationCandidate, error.InvalidRelationalIndexProgress, error.InvalidRelationalIndexValidation, error.PrivateIndexTargetValidationRequired, error.MissingRelationalIndexDefinitions, error.RelationalIndexCatalogSchemaMismatch, error.InvalidPrivateIndexCancellation, error.ResourceBudgetExceeded, error.PrivateIndexClaimBacklogFull, error.MetadataReplicationPending }) |err| {
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
    consume: ?ConsumptionRequest = null,

    /// Expected batch only, never caller-provided metadata or physical authority.
    pub const ConsumptionRequest = struct {
        ordinal: u64,
        batch_digest: [32]u8,
        pub fn validate(self: ConsumptionRequest) !void {
            if (self.ordinal == 0 or std.mem.allEqual(u8, &self.batch_digest, 0)) return error.InvalidPrivateIndexClaimReceipt;
        }
    };

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
        const wire = try std.json.innerParse(struct { table_id: u64, group_id: u64, id: [16]u8, claims: ?native.ClaimCommand.BoundedClaims = null, closure: ?source_delivery.Request = null, consume: ?ConsumptionRequest = null }, alloc, source, options);
        if (wire.consume != null and (wire.claims != null or wire.closure != null)) return error.UnexpectedToken;
        return .{ .table_id = wire.table_id, .group_id = wire.group_id, .id = wire.id, .claims = if (wire.claims) |values| values.items else &.{}, .closure = wire.closure, .consume = wire.consume };
    }

    pub fn jsonStringify(self: ClaimRequest, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("table_id");
        try jw.write(self.table_id);
        try jw.objectField("group_id");
        try jw.write(self.group_id);
        try jw.objectField("id");
        try jw.write(self.id);
        if (self.consume) |consume| {
            try jw.objectField("consume");
            try jw.write(consume);
        } else if (self.closure) |closure| {
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
        if (self.consume) |consume| {
            if (self.claims.len != 0 or self.closure != null) return error.InvalidPrivateIndexClaimReceipt;
            return consume.validate();
        }
        if (self.closure) |closure| {
            if (self.claims.len != 0) return error.InvalidPrivateIndexClaimReceipt;
            try closure.validate();
        } else try (native.ClaimCommand.BoundedClaims{ .items = self.claims }).validate();
    }
};

test "relational index publication native source consumption request excludes caller authority and mixed operations" {
    const alloc = std.testing.allocator;
    const request: ClaimRequest = .{ .table_id = 101, .group_id = 7, .id = @splat(1), .consume = .{ .ordinal = 1, .batch_digest = @splat(2) } };
    try request.validate(7);
    const bytes = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(bytes);
    var decoded = try std.json.parseFromSlice(ClaimRequest, alloc, bytes, .{ .ignore_unknown_fields = false });
    defer decoded.deinit();
    try std.testing.expectEqual(request.consume, decoded.value.consume);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "binding") == null);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "claims") == null);
    var mixed = request;
    mixed.closure = .{ .ordinal = 1, .batch_digest = @splat(2), .targets = .{ .items = &.{} } };
    try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, mixed.validate(7));
    // Use an otherwise valid claim so decoding reaches the operation union
    // check rather than failing the independent empty-claims bound first.
    const mixed_claims: @FieldType(native.ClaimCommand, "claims") = &.{.{ .tuple = "tuple", .parent_table = "rows", .parent_key = "key", .schema_version = 1 }};
    const mixed_json = try std.json.Stringify.valueAlloc(alloc, .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .consume = request.consume, .claims = mixed_claims }, .{});
    defer alloc.free(mixed_json);
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(ClaimRequest, alloc, mixed_json, .{ .ignore_unknown_fields = false }));
}

/// Borrowed wire view; the owning ClaimResult must outlive serialization.
/// A physical progress response is never a signed build-completion receipt.
pub const ClaimResponse = union(enum) {
    acknowledged,
    consumed: struct { root_generation: u64, outcome: native.ConsumeOutcome, proof: ?publication.RootProof = null, signing_failure: ?SigningFailure = null },
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

    pub fn requireSignedConsumption(self: ClaimResponse, request: ClaimRequest) !delivery_journal.Consumed {
        try self.validate(request);
        if (self != .consumed) return error.InvalidPrivateIndexClaimReceipt;
        const value = self.consumed;
        if (value.outcome.replication_pending or value.outcome.error_name != null) return error.InvalidPrivateIndexClaimReceipt;
        const result = try delivery_journal.Consumed.fromRecord(value.outcome.record.?, value.proof orelse return error.InvalidPrivateIndexClaimReceipt);
        try result.verify();
        return result;
    }

    pub fn validate(self: ClaimResponse, request: ClaimRequest) !void {
        try request.validate(request.group_id);
        switch (self) {
            .acknowledged => {},
            .consumed => |value| {
                const wanted = request.consume orelse return error.InvalidPrivateIndexClaimReceipt;
                const binding = value.outcome.binding;
                try value.outcome.validateFor(.{ .binding = binding, .accepted = value.outcome.accepted });
                if (value.root_generation == 0 or binding.fence.owner_group_id != request.group_id or binding.fence.namespace.table_id != request.table_id or
                    !std.mem.eql(u8, &binding.id, &request.id) or value.outcome.accepted.ordinal != wanted.ordinal or
                    !std.mem.eql(u8, &value.outcome.accepted.batch_digest, &wanted.batch_digest)) return error.InvalidPrivateIndexClaimReceipt;
                if (value.proof) |proof| {
                    if (value.signing_failure != null or proof.root_generation != value.root_generation or value.outcome.replication_pending or value.outcome.error_name != null) return error.InvalidPrivateIndexClaimReceipt;
                    try (try delivery_journal.Consumed.fromRecord(value.outcome.record.?, proof)).verify();
                }
            },
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
                if (request.closure != null or request.consume != null) return error.InvalidPrivateIndexClaimReceipt;
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
    consumed: struct {
        root_generation: u64,
        pinned_root_generation: u64,
        outcome: std.json.Parsed(native.ConsumeOutcome),
        view: publication.NativeSigningView,
        expected_digest: ?[32]u8,
        proof: ?publication.RootProof = null,
        signing_failure: ?SigningFailure = null,
    },
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
            .consumed => |value| value.expected_digest != null and value.proof == null and !value.outcome.value.replication_pending and value.outcome.value.error_name == null,
            .closed => |value| value.proof == null,
            .applied => |value| value.signing != null and value.signed_delivery == null and
                !value.outcome.value.replication_pending and value.outcome.value.error_name == null,
        };
    }

    /// Post-commit decoration of exact delivery, never publication authority.
    /// Current enrollment/placement and complete source coverage still need
    /// independent verification before a source batch may be consumed.
    pub fn signNative(self: *ClaimResult, root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
        if (self.* == .consumed) {
            const value = &self.consumed;
            try value.view.validate(value.view.identity.metadata_incarnation);
            const outcome = value.outcome.value;
            try outcome.validateFor(.{ .binding = outcome.binding, .accepted = outcome.accepted });
            if (outcome.replication_pending or outcome.error_name != null) return error.NativeIndexSigningUnavailable;
            const actual = try outcome.record.?.prefix().digest();
            if (value.expected_digest == null or value.root_generation != value.pinned_root_generation or
                !std.mem.eql(u8, &actual, &value.expected_digest.?) or !std.mem.eql(u8, &outcome.accepted.metadata_incarnation, &value.view.identity.metadata_incarnation) or
                node_id != value.view.identity.node_id or store_id != value.view.identity.store_id or reporter_incarnation != value.view.reporter_incarnation) return error.IndexOwnerReceiptChanged;
            value.proof = try (publication.RootProof{ .identity = value.view.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = value.view.binding_digest }, .root_generation = value.root_generation, .signature = undefined }).sign(root, actual);
            value.signing_failure = null;
            return;
        }
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
        if (self.* == .consumed) {
            self.consumed.proof = null;
            self.consumed.signing_failure = failure;
        }
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
            .consumed => |value| value.outcome.deinit(),
            .applied => |value| value.outcome.deinit(),
        }
        self.* = undefined;
    }

    pub fn response(self: *const ClaimResult) ClaimResponse {
        return switch (self.*) {
            .acknowledged => .acknowledged,
            .consumed => |value| .{ .consumed = .{ .root_generation = value.root_generation, .outcome = value.outcome.value, .proof = value.proof, .signing_failure = value.signing_failure } },
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
    if (request.consume) |consume| {
        const view = permission.value.native_signing orelse return error.IndexOwnerReceiptChanged;
        var authority = context;
        authority.index_publication_authority = true;
        const query: delivery_journal.Query = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id };
        const bytes = try metadata.systemCatalog(alloc, authority, calls.Call{ .relational_index_source_accept_status = query });
        defer alloc.free(bytes);
        if (bytes.len == 0 or bytes.len > 16 * 1024) return error.InvalidPrivateIndexClaimReceipt;
        var status = try std.json.parseFromSlice(delivery_journal.Status, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        defer status.deinit();
        try context.ensureActive();
        if (!std.meta.eql(status.value.query, query) or status.value.replication_pending) return error.InvalidPrivateIndexClaimReceipt;
        const head = status.value.head orelse return error.InvalidPrivateIndexClaimReceipt;
        _ = try head.encode();
        if (head.ordinal != consume.ordinal or !std.mem.eql(u8, &head.batch_digest, &consume.batch_digest) or
            !std.mem.eql(u8, &head.metadata_incarnation, &permission.value.metadata_incarnation)) return error.InvalidPrivateIndexClaimReceipt;
        var rooted = try source.readNativeIndexClaimSourceAtRoot(alloc, request.group_id, table_name, identity, context);
        defer rooted.deinit();
        try rooted.outcome.requireIdentity(identity);
        _ = try permission.value.nativeBuild(lookup, rooted.outcome.binding, .{});
        if (rooted.root_generation == 0 or rooted.outcome.replication_pending or rooted.outcome.error_name != null) return error.InvalidPrivateIndexClaimReceipt;
        const accepted = head.accepted();
        // Exact retry uses the retained original header after body deletion.
        // It never consumes the next queued batch on behalf of an old request.
        if (rooted.outcome.consumed) |record| {
            if (record.accepted.ordinal == consume.ordinal) {
                if (!std.mem.eql(u8, &try record.accepted.encode(), &try accepted.encode())) return error.InvalidPrivateIndexClaimReceipt;
            } else {
                const batch = rooted.outcome.batch orelse return error.InvalidPrivateIndexClaimReceipt;
                if (!std.mem.eql(u8, batch.scope.table_name, table_name)) return error.PrivateIndexOwnerJobChanged;
                try accepted.requireFor(rooted.outcome.binding, batch.header);
            }
        } else {
            const batch = rooted.outcome.batch orelse return error.InvalidPrivateIndexClaimReceipt;
            if (!std.mem.eql(u8, batch.scope.table_name, table_name)) return error.PrivateIndexOwnerJobChanged;
            try accepted.requireFor(rooted.outcome.binding, batch.header);
        }
        const command: native.ConsumeCommand = .{ .binding = rooted.outcome.binding, .accepted = accepted };
        const applied = try source.consumeNativeIndexSourceAtRoot(alloc, request.group_id, table_name, command, context);
        errdefer applied.outcome.deinit();
        try applied.outcome.value.validateFor(command);
        if (applied.root_generation == 0) return error.InvalidPrivateIndexClaimReceipt;
        return .{ .consumed = .{ .root_generation = applied.root_generation, .pinned_root_generation = rooted.root_generation, .outcome = applied.outcome, .view = view, .expected_digest = if (applied.outcome.value.replication_pending or applied.outcome.value.error_name != null) null else try applied.outcome.value.record.?.prefix().digest() } };
    }
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
    const rooted = source.applyNativeIndexOwnerClaimsAtRoot(alloc, request.group_id, table_name, command, context) catch |err| switch (err) {
        // A delayed NEW claim after inventory freeze is stale owner work,
        // not a transient receiver outage. Reuse the transport's typed 409
        // contract; exact duplicates are still accepted by native storage.
        error.PrivateIndexTargetSealed => return error.PrivateIndexOwnerJobChanged,
        else => return err,
    };
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

pub const CancellationCompletionResult = CancellationCheckpointResult(false);
pub const CancellationReleaseResult = CancellationCheckpointResult(true);
fn CancellationCheckpointResult(comptime releasing: bool) type {
    return struct {
        root_generation: u64,
        pinned_root_generation: u64,
        expected_revision: u64,
        metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
        signing: publication.NativeSigningView,
        outcome: std.json.Parsed(if (releasing) native.CancellationReleaseOutcome else native.CancellationCompletionOutcome),
        signed_receipt: ?publication.SignedReceipt = null,
        signing_failure: ?SigningFailure = null,

        pub fn needsSigning(self: *const @This()) bool {
            return self.outcome.value.receipt != null and !self.outcome.value.replication_pending and self.outcome.value.error_name == null and self.signed_receipt == null and self.signing_failure == null;
        }
        pub fn signNative(self: *@This(), root: local.storage_db_root_signing_identity.State, node_id: u64, store_id: u64, reporter_incarnation: u64) !void {
            if (!self.needsSigning()) return error.NativeIndexSigningUnavailable;
            try self.outcome.value.validate();
            try self.signing.validate(self.metadata_incarnation);
            if (self.root_generation == 0 or self.root_generation != self.pinned_root_generation or
                node_id != self.signing.identity.node_id or store_id != self.signing.identity.store_id or reporter_incarnation != self.signing.reporter_incarnation) return error.IndexOwnerReceiptChanged;
            const receipt = self.outcome.value.receipt.?;
            const proof = try (publication.RootProof{ .identity = self.signing.identity, .reporter_incarnation = reporter_incarnation, .authority = .{ .native = self.signing.binding_digest }, .root_generation = self.root_generation, .signature = undefined }).sign(root, try receipt.digest());
            const signed: publication.SignedReceipt = .{ .receipt = receipt, .proof = proof };
            if (releasing) try signed.verifySignature() else try (publication.CancellationWitness{ .record = self.outcome.value.record orelse return error.InvalidPrivateIndexCancellation }).verifyFor(signed);
            self.signed_receipt = signed;
        }
        pub fn signingFailed(self: *@This(), failure: SigningFailure) void {
            if (self.signed_receipt == null) self.signing_failure = failure;
        }
        pub fn ack(self: *const @This()) !?publication.Ack {
            const signed = self.signed_receipt orelse return null;
            if (self.signing_failure != null or self.root_generation != self.pinned_root_generation or signed.proof.root_generation != self.root_generation) return error.IndexOwnerReceiptChanged;
            try self.outcome.value.validate();
            const binding = if (releasing) self.outcome.value.intent.canceled.binding else self.outcome.value.binding;
            const result: publication.Ack = .{ .table_id = binding.fence.namespace.table_id, .expected_revision = self.expected_revision, .signed = signed, .cancellation = if (releasing) null else .{ .record = self.outcome.value.record orelse return error.InvalidPrivateIndexCancellation } };
            try result.validate();
            return result;
        }
    };
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
    completed: CancellationCompletionResult,
    released: CancellationReleaseResult,
    /// Local artifacts/claims only: cohort proof and epoch release are pending.
    retired,

    /// Compact progress for the existing private control transport. Only a
    /// complete signed ACK carries authority; unsigned progress never does.
    pub fn response(self: *const @This(), request: Request) !CancellationControlResponse {
        var result: CancellationControlResponse = .{ .step = std.meta.activeTag(self.*), .request = .{ .id = request.id, .table_id = request.table_id, .group_id = request.group_id } };
        switch (self.*) {
            .acknowledged => return error.InvalidPrivateIndexNativeReceipt,
            .intent => |value| {
                result.position = value.value.position;
                result.replication_pending = value.value.replication_pending;
                result.error_name = value.value.error_name;
            },
            .admitted, .fenced, .reserved, .cleanup => |value| {
                result.position = value.value.position;
                result.replication_pending = value.value.replication_pending;
                result.error_name = value.value.error_name;
            },
            .completed => |value| {
                try value.outcome.value.validate();
                result.position = value.outcome.value.position;
                result.replication_pending = value.outcome.value.replication_pending;
                result.error_name = value.outcome.value.error_name;
                result.root_generation = value.root_generation;
                result.signed_ack = try value.ack();
                result.signing_failure = value.signing_failure;
            },
            .released => |value| {
                try value.outcome.value.validate();
                result.position = value.outcome.value.position;
                result.replication_pending = value.outcome.value.replication_pending;
                result.error_name = value.outcome.value.error_name;
                result.root_generation = value.root_generation;
                result.signed_ack = try value.ack();
                result.signing_failure = value.signing_failure;
            },
            .gc, .claim_gc, .retired => {},
        }
        try result.validate(request);
        return result;
    }

    pub fn deinit(self: *CancellationResult) void {
        switch (self.*) {
            .acknowledged, .retired => {},
            .intent => |value| value.deinit(),
            .admitted, .fenced, .reserved, .cleanup => |value| value.deinit(),
            .gc => |value| value.deinit(),
            .claim_gc => |value| value.deinit(),
            .completed => |value| value.outcome.deinit(),
            .released => |value| value.outcome.deinit(),
        }
        self.* = undefined;
    }
};

pub const CancellationControlResponse = struct {
    request: CancellationRequest,
    step: std.meta.Tag(CancellationResult),
    position: ?local.storage_db_receipt_position.Native = null,
    root_generation: u64 = 0,
    replication_pending: bool = false,
    error_name: ?[]const u8 = null,
    signed_ack: ?publication.Ack = null,
    signing_failure: ?SigningFailure = null,

    pub fn validate(self: @This(), request: Request) !void {
        if (self.request.table_id != request.table_id or self.request.group_id != request.group_id or !std.mem.eql(u8, &self.request.id, &request.id) or self.step == .acknowledged) return error.InvalidPrivateIndexNativeReceipt;
        const terminal = self.step == .completed or self.step == .released;
        if (self.replication_pending and self.error_name == null) return error.InvalidPrivateIndexNativeReceipt;
        if (self.step == .retired and (self.position != null or self.replication_pending or self.error_name != null)) return error.InvalidPrivateIndexNativeReceipt;
        if ((if (self.step == .completed) request.operation != .cancel_complete else if (self.step == .released) request.operation != .cancel_release else request.operation != .cancel) or
            terminal != (self.root_generation != 0) or (!terminal and (self.signed_ack != null or self.signing_failure != null))) return error.InvalidPrivateIndexNativeReceipt;
        if (self.position) |cut| {
            try cut.validate();
            if (cut.namespace.table_id != request.table_id) return error.InvalidPrivateIndexNativeReceipt;
        } else if (terminal or self.signed_ack != null) return error.InvalidPrivateIndexNativeReceipt;
        if (self.signed_ack) |ack| {
            try ack.validate();
            const receipt = ack.signed.receipt;
            const expected_action: local.storage_db_relational_index_publication_progress.Action = if (self.step == .completed) .canceled else .cancel_released;
            if (self.replication_pending or self.error_name != null or self.signing_failure != null or
                ack.table_id != request.table_id or receipt.owner.group_id != request.group_id or !std.mem.eql(u8, &receipt.owner.plan_id, &request.id) or
                receipt.owner.authority != .native or ack.signed.proof.authority != .native or
                receipt.action != expected_action or receipt.position != .native or
                !std.meta.eql(receipt.position.native, self.position.?) or ack.signed.proof.root_generation != self.root_generation) return error.InvalidPrivateIndexNativeReceipt;
        }
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
/// The existing authenticated private control port mounts this step. Completion
/// and all-owner release remain separate effects; physical GC is NOT an ACK.
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

/// One exact completion effect under freshly observed cancellation permission.
/// No inspection response alone becomes an ACK, and pending replication never
/// becomes signable. Caller submits the signed ACK in a separate bounded turn.
pub fn cancelCompletionStep(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: CancellationRequest, context: operation.RequestContext) !CancellationResult {
    try context.ensureActive();
    const lookup: publication.OwnerRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .cancel };
    try lookup.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, lookup, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(permission.value, table_name);
    const identity = try permission.value.nativeExecutionIdentity(lookup);
    const view = permission.value.native_signing orelse return error.IndexOwnerReceiptChanged;
    try view.validate(permission.value.metadata_incarnation);
    var rooted = try source.completeNativeIndexOwnerCancellationAtRoot(alloc, request.group_id, table_name, identity, view.identity.root_incarnation, context);
    errdefer rooted.deinit();
    try rooted.outcome.value.validate();
    if (rooted.root_generation == 0) return error.InvalidPrivateIndexNativeReceipt;
    try permission.value.requireRetainedNativeBinding(rooted.outcome.value.binding);
    const dispatch = permission.value.dispatch orelse return error.InvalidCatalogRecord;
    const record = rooted.outcome.value.record orelse return error.InvalidPrivateIndexCancellation;
    if (!record.cleanup.intent.eql(try permission.value.nativeCancellationIntent()) or
        record.cleanup.claim_allocation != dispatch.native_claim_allocation) return error.PrivateIndexOwnerJobChanged;
    return .{ .completed = .{ .root_generation = rooted.root_generation, .pinned_root_generation = rooted.root_generation, .expected_revision = permission.value.head.revision, .metadata_incarnation = permission.value.metadata_incarnation, .signing = view, .outcome = rooted.outcome } };
}

/// Obtain fresh all-owner permission; the source checks its accepted generation
/// on the applying lease before the terminal native effect can lift a fence.
pub fn cancelReleaseStep(alloc: std.mem.Allocator, source: anytype, metadata: anytype, table_name: []const u8, request: CancellationRequest, context: operation.RequestContext) !CancellationResult {
    try context.ensureActive();
    const lookup: publication.OwnerRequest = .{ .table_id = request.table_id, .group_id = request.group_id, .id = request.id, .operation = .cancel_release };
    try lookup.validate();
    if (table_name.len == 0) return error.InvalidGenerationPublication;
    var permission = try decision(alloc, metadata, lookup, context);
    defer permission.deinit();
    if (permission.value.disposition == .acknowledged) return .acknowledged;
    try requireTable(permission.value, table_name);
    const grant = permission.value.native_cancel_release orelse return error.InvalidCatalogRecord;
    const view = permission.value.native_signing orelse return error.IndexOwnerReceiptChanged;
    var rooted = try source.applyNativeIndexOwnerCancellationReleaseAtRoot(alloc, request.group_id, table_name, grant.intent(), grant.cleanup_proof.root_generation, view.identity.root_incarnation, context);
    errdefer rooted.deinit();
    if (rooted.root_generation != grant.cleanup_proof.root_generation) return error.IndexOwnerReceiptChanged;
    return .{ .released = .{ .root_generation = rooted.root_generation, .pinned_root_generation = grant.cleanup_proof.root_generation, .expected_revision = permission.value.head.revision, .metadata_incarnation = permission.value.metadata_incarnation, .signing = view, .outcome = rooted.outcome } };
}

fn requireCancellationProgress(result: native.CleanupOutcome, expected: ?jobs.Binding, intent: native.Admission) !void {
    try result.position.validate();
    _ = try result.binding.encode();
    if (result.binding.authority != .native or (expected == null and result.binding.phase != .fencing)) return error.InvalidPrivateIndexOwnerJob;
    if (!result.position.namespace.eql(intent.fence.namespace) or !native.Admission.fromBinding(result.binding).eql(intent)) return error.PrivateIndexOwnerJobChanged;
    if (expected) |binding| if (!std.mem.eql(u8, &try binding.encode(), &try result.binding.encode())) return error.PrivateIndexOwnerJobChanged;
}

fn decision(alloc: std.mem.Allocator, metadata: anytype, request: publication.OwnerRequest, context: operation.RequestContext) !std.json.Parsed(publication.OwnerDecision) {
    return decisionForAuthority(alloc, metadata, request, context, .native);
}

pub fn raftDecision(alloc: std.mem.Allocator, metadata: anytype, request: publication.OwnerRequest, context: operation.RequestContext) !std.json.Parsed(publication.OwnerDecision) {
    return decisionForAuthority(alloc, metadata, request, context, .raft);
}

fn decisionForAuthority(alloc: std.mem.Allocator, metadata: anytype, request: publication.OwnerRequest, context: operation.RequestContext, expected: local.storage_db_relational_index_publication_progress.Authority) !std.json.Parsed(publication.OwnerDecision) {
    try context.ensureActive();
    var authority = context;
    authority.index_publication_authority = true;
    const bytes = try metadata.systemCatalog(alloc, authority, calls.Call{ .relational_index_owner_decision = request });
    defer alloc.free(bytes);
    if (bytes.len == 0 or bytes.len > publication.max_command_bytes) return error.InvalidGenerationPublication;
    var parsed = try std.json.parseFromSlice(publication.OwnerDecision, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    errdefer parsed.deinit();
    try parsed.value.requireRequest(request);
    if (parsed.value.owner.owner.authority != expected) return error.UnsupportedNativeIndexOwner;
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
    if (request.operation == .cancel or request.operation == .cancel_complete or request.operation == .cancel_release) {
        if (comptime @hasDecl(@TypeOf(source.*), "nativeIndexOwnerCancellationState") and @hasDecl(@TypeOf(source.*), "completeNativeIndexOwnerCancellationAtRoot") and @hasDecl(@TypeOf(source.*), "applyNativeIndexOwnerCancellationReleaseAtRoot")) {
            const cancellation: CancellationRequest = .{ .id = request.id, .table_id = request.table_id, .group_id = request.group_id };
            const result = if (request.operation == .cancel_complete) try cancelCompletionStep(alloc, source, metadata, table_name, cancellation, context) else if (request.operation == .cancel_release) try cancelReleaseStep(alloc, source, metadata, table_name, cancellation, context) else try cancelStep(alloc, source, metadata, table_name, cancellation, context);
            return if (result == .acknowledged) .acknowledged else .{ .canceling = .{ .request = request, .result = result } };
        } else return error.UnsupportedNativeIndexOwner;
    }
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
            .build, .source, .validate, .install, .release, .cancel, .cancel_complete, .cancel_release => return error.InvalidGenerationPublication,
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
    if (request.operation == .install) {
        const view = permission.value.native_signing orelse return error.NativeIndexSigningUnavailable;
        const grant = permission.value.native_install orelse return error.InvalidCatalogRecord;
        // Metadata supplies the desired immutable binding. The kernel checks
        // the real reserved/installed job on the applying root; old inspection
        // APIs deliberately cannot manufacture an installed retry observation.
        const installation: native.InstallationRequest = .{ .intent = try permission.value.nativeInstallationIntent(hot_request, grant.validation.command.binding), .budget = request.build_budget orelse .{} };
        var rooted = try source.stepNativeIndexOwnerInstallationAtRoot(alloc, request.group_id, table_name, installation, view.identity.root_incarnation, context);
        errdefer rooted.outcome.deinit();
        try rooted.outcome.value.validateFor(installation);
        if (rooted.root_generation == 0 or rooted.outcome.value.physicalAudit().audit.root_incarnation != view.identity.root_incarnation) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .installing = .{ .root_generation = rooted.root_generation, .expected_revision = permission.value.head.revision, .outcome = rooted.outcome, .signing = view } };
    }
    if (request.operation == .release) {
        const view = permission.value.native_signing orelse return error.NativeIndexSigningUnavailable;
        const grant = permission.value.native_release orelse return error.InvalidCatalogRecord;
        const intent = try permission.value.nativeReleaseIntent(hot_request);
        var rooted = try source.applyNativeIndexOwnerReleaseAtRoot(alloc, request.group_id, table_name, intent, grant.installation_proof.root_generation, view.identity.root_incarnation, context);
        errdefer rooted.outcome.deinit();
        try rooted.outcome.value.validate();
        if (rooted.root_generation != grant.installation_proof.root_generation or !std.meta.eql(rooted.outcome.value.intent, intent)) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .releasing = .{ .root_generation = rooted.root_generation, .expected_revision = permission.value.head.revision, .outcome = rooted.outcome, .signing = view } };
    }
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
    if (request.operation == .build or request.operation == .validate) {
        const binding = state.value orelse return error.PrivateIndexRootRecoveryRequired;
        const build = if (request.operation == .validate) try permission.value.nativeInventoryValidation(hot_request, binding, request.build_budget orelse .{}) else try permission.value.nativeBuild(hot_request, binding, request.build_budget orelse .{});
        const rooted = try source.stepNativeIndexOwnerBuildAtRoot(alloc, request.group_id, table_name, build, if (permission.value.native_signing) |view| view.identity.root_incarnation else null, context);
        errdefer rooted.outcome.deinit();
        try rooted.outcome.value.validateFor(build);
        if (rooted.root_generation == 0) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .building = .{ .root_generation = rooted.root_generation, .expected_revision = permission.value.head.revision, .outcome = rooted.outcome, .audit_signing = permission.value.native_signing } };
    }
    if (state.value) |binding| {
        const effect: @FieldType(native.ReceiptRequest, "effect") = switch (request.operation) {
            .fence => .fenced,
            .reserve => .reserved,
            .build, .source, .validate, .install, .release, .cancel, .cancel_complete, .cancel_release => return error.InvalidGenerationPublication,
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
    const ValidationFault = enum { none, before_commit, lost_reply, pending, stale_revision, physical_only, restart, restart_committed };
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
    claims_sealed: bool = false,
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
    build_failed: bool = false,
    cancellation_requests: usize = 0,
    inventory_pages: usize = 0,
    generation_pages: usize = 0,
    audit_pages: usize = 0,
    validation_fault: ValidationFault = .none,
    validation_ack_requests: usize = 0,
    validation_acks: usize = 0,
    validation_status_calls: usize = 0,
    validation_permission_acks: usize = 0,
    validation_pending_checks: usize = 0,
    validation_digest: ?publication.Digest = null,
    validation_seal_effects: usize = 0,
    installation_response: ?native.InstallationResponse = null,
    installation_calls: usize = 0,
    release_calls: usize = 0,
    release_effects: usize = 0,
    release_outcome: ?native.ReleaseOutcome = null,
    release_ack_requests: usize = 0,
    release_acks: usize = 0,
    installation_ack_requests: usize = 0,
    installation_acks: usize = 0,
    installation_digest: ?publication.Digest = null,
    installation_proof: ?publication.RootProof = null,
    validated_outcome: ?native.ValidatedOutcome = null,
    decision_fault: ValidationFault = .none,
    decision_requests: usize = 0,
    decision_effects: usize = 0,
    decision_status_calls: usize = 0,
    decision_pending_checks: usize = 0,
    inventory_finished: bool = false,
    source_reads: usize = 0,
    source_failed: bool = false,
    acceptance_head: ?delivery_journal.Head = null,
    acceptance_calls: usize = 0,
    lose_acceptance_reply: bool = false,
    consumption_calls: usize = 0,
    completion_calls: usize = 0,
    completion_requests: usize = 0,
    completed: bool = false,
    lose_completion_reply: bool = false,
    lose_completion_before_commit: bool = false,
    consumed_record: ?jobs.private_claims.SourceStream.Consumption = null,
    cancel_after_commit: ?*std.atomic.Value(bool) = null,

    pub fn systemCatalog(self: *TestDriver, alloc: std.mem.Allocator, context: operation.RequestContext, call: calls.Call) ![]u8 {
        try std.testing.expect(context.index_publication_authority);
        if (call == .relational_index_cancel or call == .relational_index_cancel_status) {
            const request = if (call == .relational_index_cancel) call.relational_index_cancel else call.relational_index_cancel_status;
            try request.validate();
            if (call == .relational_index_cancel) {
                self.cancellation_requests += 1;
                self.value.head = try self.value.head.cancel(request.expected_revision, request.failure);
            }
            return std.json.Stringify.valueAlloc(alloc, publication.CancellationStatus{ .head = self.value.head }, .{});
        }
        if (call == .relational_index_publish_decide or call == .relational_index_publish_status) {
            const request = if (call == .relational_index_publish_decide) call.relational_index_publish_decide else call.relational_index_publish_status;
            try request.validate();
            if (call == .relational_index_publish_decide) {
                self.decision_requests += 1;
                if (self.decision_fault == .before_commit) {
                    self.decision_fault = .none;
                    return error.MetadataMutationOutcomeUnknown;
                }
                if (self.decision_fault == .stale_revision) {
                    self.decision_fault = .none;
                    self.value.head.revision += 1;
                    return error.GenerationPublicationChanged;
                }
                if (self.value.head.phase == .building) {
                    self.value.head = try self.value.head.decidePublish(request.expected_revision);
                    self.decision_effects += 1;
                    const seal = if (self.installation_response) |response| response.installed.logical.command.validation else sealed: {
                        const validated = self.validated_outcome orelse return error.TestMissingValidatedOutcome;
                        break :sealed local.storage_db_relational_index_validation.Seal{ .command = validated.logical.command, .position = .{ .native = validated.logical.position } };
                    };
                    self.value.native_install = .{ .validation = seal, .decision_digest = try publication.NativeInstallGrant.decisionDigest(self.value.metadata_group_id, self.value.metadata_incarnation, self.value.head) };
                }
                if (self.decision_fault == .lost_reply or self.decision_fault == .pending or self.decision_fault == .restart_committed) {
                    if (self.decision_fault == .pending) self.decision_pending_checks = 3;
                    self.decision_fault = .none;
                    return error.MetadataMutationOutcomeUnknown;
                }
            } else self.decision_status_calls += 1;
            const pending = self.decision_pending_checks != 0;
            if (pending) self.decision_pending_checks -= 1;
            return std.json.Stringify.valueAlloc(alloc, publication.DecisionStatus{ .head = self.value.head, .replication_pending = pending }, .{});
        }
        if (call == .relational_index_owner_ack or call == .relational_index_owner_ack_status) {
            const request = if (call == .relational_index_owner_ack) call.relational_index_owner_ack else call.relational_index_owner_ack_status;
            try request.validate();
            const installed_ack = request.signed.receipt.action == .installed;
            const released_ack = request.signed.receipt.action == .released;
            try std.testing.expect(if (installed_ack) request.installation != null else if (released_ack) request.installation == null and request.validation == null else request.signed.receipt.action == .validated and request.validation != null);
            if (call == .relational_index_owner_ack) {
                if (installed_ack) self.installation_ack_requests += 1 else if (released_ack) self.release_ack_requests += 1 else self.validation_ack_requests += 1;
                if (self.validation_fault == .stale_revision) {
                    self.validation_fault = .none;
                    self.value.head.revision += 1;
                    return error.GenerationPublicationChanged;
                }
                if (self.validation_fault == .before_commit) {
                    self.validation_fault = .none;
                    return error.MetadataMutationOutcomeUnknown;
                }
                const update = try local.storage_db_relational_index_publication_progress.acknowledge(self.value.head, self.value.owner, request.expected_revision, request.signed.receipt);
                self.value.head = update.head;
                self.value.owner = update.owner;
                self.value.owner.attestations[@backingInt(request.signed.receipt.action)] = try request.signed.commitment();
                if (installed_ack) {
                    self.installation_digest = request.installation.?.signed.outcome_digest;
                    self.installation_proof = request.signed.proof;
                    self.installation_acks += 1;
                } else if (released_ack) {
                    self.release_acks += 1;
                } else {
                    self.validation_digest = request.validation.?.signed.outcome_digest;
                    self.validation_acks += 1;
                }
                if (self.validation_fault == .lost_reply or self.validation_fault == .pending or self.validation_fault == .restart_committed) {
                    if (self.validation_fault == .pending) self.validation_pending_checks = 3;
                    self.validation_fault = .none;
                    return error.MetadataMutationOutcomeUnknown;
                }
            } else self.validation_status_calls += 1;
            const pending = self.validation_pending_checks != 0;
            if (pending) self.validation_pending_checks -= 1;
            return std.json.Stringify.valueAlloc(alloc, publication.AckStatus{ .head = self.value.head, .owner = self.value.owner, .validation_digest = self.validation_digest, .installation_digest = self.installation_digest, .replication_pending = pending }, .{});
        }
        if (call == .relational_index_source_accept_status) return std.json.Stringify.valueAlloc(alloc, delivery_journal.Status{ .query = call.relational_index_source_accept_status, .head = self.acceptance_head, .completed = self.completed }, .{});
        if (call == .relational_index_source_complete) {
            const request = call.relational_index_source_complete;
            try request.validate();
            try request.consumed.requireHead(self.acceptance_head orelse return error.InvalidPrivateIndexClaimReceipt);
            self.completion_requests += 1;
            if (self.lose_completion_before_commit) {
                self.lose_completion_before_commit = false;
                return error.MetadataMutationOutcomeUnknown;
            }
            if (!self.completed) self.completion_calls += 1;
            self.completed = true;
            if (self.lose_completion_reply) {
                self.lose_completion_reply = false;
                return error.MetadataMutationOutcomeUnknown;
            }
            return std.json.Stringify.valueAlloc(alloc, delivery_journal.Status{ .query = request.query, .head = self.acceptance_head, .completed = true }, .{});
        }
        if (call == .relational_index_source_accept) {
            const request = call.relational_index_source_accept;
            self.acceptance_head = try @import("relational_index_delivery_producer.zig").testing.acceptedHead(request, self.acceptance_head);
            self.acceptance_calls += 1;
            if (self.lose_acceptance_reply) {
                self.lose_acceptance_reply = false;
                return error.MetadataMutationOutcomeUnknown;
            }
            return std.json.Stringify.valueAlloc(alloc, delivery_journal.Status{ .query = delivery_journal.Query.from(request), .head = self.acceptance_head }, .{});
        }
        const request = call.relational_index_owner_decision;
        var value = self.value;
        if (request.operation != .install) value.native_install = null;
        if (request.operation != .release) value.native_release = null;
        if (self.release_acks != 0 and request.operation == .release) {
            value.disposition = .acknowledged;
            value.dispatch = null;
            value.inputs = null;
            value.native_signing = null;
            value.native_release = null;
        } else if (request.operation == .release) {
            const installed = self.installation_response.?.installed.logical;
            value.native_release = .{ .installed = .{ .command = installed.command, .position = .{ .native = installed.position } }, .decision_digest = try publication.NativeReleaseGrant.decisionDigest(value.metadata_group_id, value.metadata_incarnation, value.head), .installation_proof = self.installation_proof orelse value.native_release.?.installation_proof };
        } else if (self.installation_acks != 0 and request.operation == .install) {
            value.disposition = .acknowledged;
            value.dispatch = null;
            value.inputs = null;
            value.native_signing = null;
            value.native_install = null;
            value.target_expectation = null;
        } else if (self.validation_acks != 0 and (request.operation == .validate or request.operation == .build)) {
            self.validation_permission_acks += 1;
            value.disposition = .acknowledged;
            value.dispatch = null;
            value.inputs = null;
            value.native_signing = null;
            value.target_expectation = null;
        } else if (request.operation == .validate) {
            const consumed = self.consumed_record orelse return error.InvalidPrivateIndexClaimReceipt;
            const member: local.storage_db_relational_integrity_contract.Claim = .{ .tuple = "logical\x00\xff\x80", .parent_table = value.dispatch.?.table_name, .parent_key = "row", .schema_version = self.binding.?.physical_candidate.schema_version };
            value.target_expectation = .{ .metadata_incarnation = consumed.accepted.metadata_incarnation, .binding_digest = consumed.accepted.binding_digest, .generation = consumed.header.bytes[72..88].*, .completion_revision = value.head.completion_revision, .source_consumption_digest = try consumed.prefix().digest(), .source_progress_digest = consumed.header.progressDigest(), .table_name_digest = local.storage_db_relational_integrity_contract.hash(value.dispatch.?.table_name), .rows = 1, .inventory_xor = member.memberDigest(), .provenance = @splat(1) };
        }
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
        if (self.claims_sealed) return error.PrivateIndexTargetSealed;
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

    pub fn consumeNativeIndexSourceAtRoot(self: *TestDriver, alloc: std.mem.Allocator, group: u64, table: []const u8, command: native.ConsumeCommand, context: operation.RequestContext) !struct { root_generation: u64, outcome: std.json.Parsed(native.ConsumeOutcome) } {
        try context.ensureActive();
        try command.validate();
        const duplicate = self.consumed_record != null;
        if (!duplicate) {
            const binding = command.binding;
            var source = try self.readNativeIndexClaimSourceAtRoot(alloc, group, table, .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = group }, context);
            defer source.deinit();
            const header = source.outcome.batch.?.header;
            try command.accepted.requireFor(binding, header);
            self.consumed_record = .{ .accepted = command.accepted, .header = header, .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 5 } }, .previous_digest = @splat(0) };
        }
        self.consumption_calls += 1;
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.ConsumeOutcome, alloc, .{ .binding = command.binding, .accepted = command.accepted, .record = self.consumed_record, .position = self.consumed_record.?.position.native, .duplicate = duplicate }) };
    }

    pub fn stepNativeIndexOwnerInstallationAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, request: native.InstallationRequest, audit_root_incarnation: u128, context: operation.RequestContext) !struct { root_generation: u64, outcome: std.json.Parsed(native.InstallationResponse) } {
        try context.ensureActive();
        const value = self.installation_response orelse return error.TestMissingInstallationResponse;
        try value.validateFor(request);
        if (value.physicalAudit().audit.root_incarnation != audit_root_incarnation) return error.PrivateIndexAuditScopeChanged;
        self.installation_calls += 1;
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.InstallationResponse, alloc, value) };
    }

    pub fn applyNativeIndexOwnerReleaseAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, intent: @FieldType(native.ReleaseOutcome, "intent"), expected_root_generation: u64, root_incarnation: u128, context: operation.RequestContext) !struct { root_generation: u64, outcome: std.json.Parsed(native.ReleaseOutcome) } {
        try context.ensureActive();
        try intent.validate();
        if (expected_root_generation == 0 or expected_root_generation != self.root_generation or self.value.native_signing.?.identity.root_incarnation != root_incarnation) return error.PrivateIndexRootRecoveryRequired;
        const duplicate = self.release_outcome != null;
        if (!duplicate) {
            var position = intent.installed.position.native;
            position.sequence += 1;
            self.release_outcome = .{ .intent = intent, .position = position, .duplicate = false, .receipt = try intent.receipt(position) };
            self.release_effects += 1;
        }
        var outcome_release = self.release_outcome.?;
        if (!std.meta.eql(intent, outcome_release.intent)) return error.InvalidPrivateIndexNativeReceipt;
        outcome_release.duplicate = duplicate;
        self.binding = null;
        self.release_calls += 1;
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.ReleaseOutcome, alloc, outcome_release) };
    }

    pub fn stepNativeIndexOwnerBuildAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, _: []const u8, request: native.BuildRequest, audit_root_incarnation: ?u128, context: operation.RequestContext) !@FieldType(Result, "building") {
        try context.ensureActive();
        const binding = self.binding orelse return error.PrivateIndexRootRecoveryRequired;
        try request.requireBinding(binding);
        if (request.target_expectation) |expected| {
            if (self.inventory_finished) {
                if (self.generation_pages != 0) {
                    self.audit_pages += 1;
                    const address = try local.storage_db_relational_integrity_contract.Address.init(expected.generation, "logical\x00\xff\x80");
                    // Scheduling only: physical durability and signatures are
                    // separately exercised by the real LSM/root fixtures.
                    const fields: local.storage_db_relational_index_validation.Fields = .{ .phase = if (self.audit_pages == 1) .primary else .ready, .counts = @splat(@as(u64, if (self.audit_pages == 1) 0 else 1)), .chain = @splat(7), .cursor_digest = local.storage_db_relational_integrity_contract.hash("") };
                    const audit_summary: local.storage_db_relational_index_audit_checkpoint.Summary = .{ .root_incarnation = audit_root_incarnation orelse 7, .physical_root_incarnation = 11, .expectation_digest = try expected.digest(), .inventory = .{ .phase = .ready, .rows = 1, .inventory_xor = expected.inventory_xor, .claim_rows = 1, .claim_xor = expected.inventory_xor, .claim_cursor = address.claimKey(), .chain = @splat(7), .cursor = jobs.private_claims.inventoryKey(address) }, .generation = fields, .inventory_canonical_digest = @splat(9), .generation_canonical_digest = @splat(8), .source_members = 1, .canonical_position = .{ .namespace = binding.fence.namespace, .sequence = 7 }, .checkpoint_digest = @splat(9) };
                    const physical: native.AuditOutcome = .{ .binding = binding, .expected = expected, .audit = audit_summary };
                    if (fields.phase == .ready and !(self.validation_fault == .physical_only and self.audit_pages < 5)) {
                        const command = try physical.logicalCommand();
                        const position: local.storage_db_receipt_position.Native = .{ .namespace = binding.fence.namespace, .sequence = 8 };
                        const duplicate = self.validation_seal_effects != 0;
                        if (!duplicate) self.validation_seal_effects += 1;
                        const validated: native.ValidatedOutcome = .{ .physical = physical, .logical = .{ .command = command, .position = position, .duplicate = duplicate, .receipt = try command.receipt(position) } };
                        self.validated_outcome = validated;
                        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .validated = validated }) };
                    }
                    return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .audit = physical }) };
                }
                self.generation_pages += 1;
                const position: local.storage_db_receipt_position.Native = .{ .namespace = binding.fence.namespace, .sequence = 7 };
                // Scheduling-only physical progress; real durable generation
                // bytes and replay are qualified by the independent LSM tests.
                return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .generation = .{ .binding = binding, .record = .{ .expected = expected, .inventory_digest = @splat(9), .source_members = 1, .fields = .{ .phase = .ready, .counts = @splat(1), .chain = @splat(7), .cursor_digest = local.storage_db_relational_integrity_contract.hash("") }, .position = .{ .native = position }, .page_digest = @splat(8) }, .position = position, .duplicate = false } }) };
            }
            self.inventory_pages += 1;
            self.inventory_finished = true;
            const address = try local.storage_db_relational_integrity_contract.Address.init(expected.generation, "logical\x00\xff\x80");
            const position: local.storage_db_receipt_position.Native = .{ .namespace = binding.fence.namespace, .sequence = 6 };
            return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .inventory = .{ .binding = binding, .record = .{ .expected = expected, .fields = .{ .phase = .ready, .rows = 1, .inventory_xor = expected.inventory_xor, .claim_rows = 1, .claim_xor = expected.inventory_xor, .claim_cursor = address.claimKey(), .chain = @splat(7), .cursor = jobs.private_claims.inventoryKey(address) }, .position = .{ .native = position }, .page_digest = @splat(8) }, .position = position, .duplicate = false } }) };
        }
        self.build_pages += 1;
        if (self.build_failed) return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .complete = .{
            .binding = binding,
            .state = .failed,
            .failure = .invalid_row,
            .rows_scanned = 1,
            .progress_digest = @splat(9),
            .position = .{ .namespace = binding.fence.namespace, .sequence = 4 },
            .replication_pending = self.pending,
            .error_name = if (self.pending) "ReplicationOutboxPending" else null,
        } }) };
        if (self.cancel_after_commit) |signal| signal.store(true, .release);
        return .{ .root_generation = self.root_generation, .outcome = try parsed(native.BuildResponse, alloc, .{ .advanced = .{ .binding = binding, .page_digest = @splat(9), .position = .{ .namespace = binding.fence.namespace, .sequence = 4 }, .duplicate = false, .replication_pending = self.pending, .error_name = if (self.pending) "ReplicationOutboxPending" else null } }) };
    }

    pub fn readNativeIndexClaimSourceAtRoot(self: *TestDriver, alloc: std.mem.Allocator, _: u64, table_name: []const u8, identity: native.Identity, context: operation.RequestContext) !SourceResult {
        try context.ensureActive();
        const binding = self.binding orelse return error.PrivateIndexRootRecoveryRequired;
        self.source_reads += 1;
        if (self.consumed_record) |record| {
            var wire = try parsed(native.SourceWire, alloc, .{ .binding = binding, .header = null, .payload_b64 = null, .consumed = record });
            errdefer wire.deinit();
            return .{ .root_generation = self.root_generation, .outcome = try wire.value.decode(alloc, identity), .wire = wire };
        }
        const Source = jobs.private_claims.SourceStream;
        var prepared = try Source.PreparedSource.init(alloc, binding, .{ .table_name = table_name, .generation = @splat(1), .schema_version = binding.physical_candidate.schema_version, .tuple_fingerprint = @splat(2), .nulls_not_distinct = false }, @splat(9), if (self.source_failed) .failed else .ready, &.{.{ .key = "row", .tuple = "logical\x00\xff\x80" }});
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
            .schema_before = self.value.dispatch.?.native_schema_digest,
            .physical_before = null,
            .physical_reserved = .{ .revision = 2, .schema_version = 1, .blob_digest = @splat(4) },
            .physical_candidate = .{ .revision = 2, .schema_version = 2, .blob_digest = @splat(5) },
            .index = .{ .generation = 2, .slot = 0 },
            .inputs_digest = self.value.dispatch.?.native_inputs_digest,
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
            state.intent = .{ .intent = result.intent, .position = .{ .native = result.position } };
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

const TestDeliveryTransport = struct {
    driver: *TestDriver,
    root: local.storage_db_root_signing_identity.State,
    calls: usize = 0,
    signed_audits: usize = 0,
    fn localTransport(self: *@This()) @import("relational_index_local_delivery.zig").Transport {
        return .{ .control_port = .{ .ptr = self, .execute_fn = controlDirect }, .claim_port = .{ .ptr = self, .execute_fn = claimsDirect } };
    }
    fn controlDirect(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, group: u64, request: Request, context: operation.RequestContext) !Result {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try request.validate(group);
        self.calls += 1;
        var result = try step(alloc, self.driver, self.driver, table, request, context);
        errdefer result.deinit();
        if (result == .building and result.building.outcome.value == .audit and (result.building.outcome.value.audit.audit.generation == null or result.building.outcome.value.audit.audit.generation.?.phase != .ready)) {
            try std.testing.expect(!result.needsSigning());
        }
        if (result == .building and result.needsSigning()) {
            const before = try result.building.outcome.value.physicalAudit().?.readyDigest();
            try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(self.root, 2, 3, 5));
            result.signingFailed(.identity_unavailable);
            const preserved = try result.response();
            try preserved.validate(request);
            try std.testing.expect(preserved.building.signed_audit == null and preserved.building.signed_receipt == null and preserved.building.signing_failure == .identity_unavailable);
            try std.testing.expectEqual(before, try preserved.building.outcome.physicalAudit().?.readyDigest());
            result.building.signing_failure = null;
        }
        if (result.needsSigning()) try result.signNative(self.root, 2, 3, 4);
        if (result == .building and result.building.signed_audit != null) {
            self.signed_audits += 1;
            _ = try result.building.signed_audit.?.verifiedDigest(result.building.outcome.value.physicalAudit().?);
            if (result.building.outcome.value == .validated) {
                try std.testing.expect(result.building.signed_receipt != null);
                try result.building.signed_receipt.?.verifySignature();
                const response = try result.response();
                try response.validate(request);
                const ack = (try response.validationAck(request)).?;
                try std.testing.expectEqual(result.building.expected_revision, ack.expected_revision);
                try std.testing.expectEqualDeep(result.building.signed_receipt.?, ack.signed);
                var missing_revision = response;
                missing_revision.building.expected_revision = 0;
                try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, missing_revision.validationAck(request));
                var mismatched = response;
                const logical = &mismatched.building.signed_receipt.?;
                logical.proof.root_generation += 1;
                logical.proof = try logical.proof.sign(self.root, try logical.receipt.digest());
                try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, mismatched.validate(request));
                var pending_value = result.building.outcome.value;
                pending_value.validated.logical.replication_pending = true;
                pending_value.validated.logical.error_name = "ReplicationOutboxPending";
                pending_value.validated.logical.receipt = null;
                var pending: Result = .{ .building = .{ .root_generation = result.building.root_generation, .expected_revision = result.building.expected_revision, .outcome = try TestDriver.parsed(native.BuildResponse, alloc, pending_value), .audit_signing = result.building.audit_signing } };
                defer pending.deinit();
                try std.testing.expect(!pending.needsSigning());
                try std.testing.expectError(error.NativeIndexSigningUnavailable, pending.signNative(self.root, 2, 3, 4));
                try (try pending.response()).validate(request);
            }
        }
        return result;
    }
    fn claimsDirect(ptr: *anyopaque, alloc: std.mem.Allocator, table: []const u8, group: u64, request: ClaimRequest, context: operation.RequestContext) !ClaimResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try request.validate(group);
        self.calls += 1;
        var result = try claimStep(alloc, self.driver, self.driver, table, request, context);
        errdefer result.deinit();
        if (result.needsSigning()) try result.signNative(self.root, 2, 3, 4);
        return result;
    }
    pub fn control(self: *@This(), alloc: std.mem.Allocator, table: []const u8, request: Request, context: operation.RequestContext) !std.json.Parsed(ControlResponse) {
        var result = try controlDirect(self, alloc, table, request.group_id, request, context);
        defer result.deinit();
        return TestDriver.parsed(ControlResponse, alloc, try result.response());
    }
    pub fn claims(self: *@This(), alloc: std.mem.Allocator, table: []const u8, request: ClaimRequest, context: operation.RequestContext) !std.json.Parsed(ClaimResponse) {
        var result = try claimsDirect(self, alloc, table, request.group_id, request, context);
        defer result.deinit();
        return TestDriver.parsed(ClaimResponse, alloc, result.response());
    }
};

const TestSchedulerHost = struct {
    const worker = @import("../metadata/relational_index_worker.zig");
    const work = @import("../metadata/relational_index_work.zig");
    driver: *TestDriver,
    active: bool = true,
    pub fn currentAuthority(self: *@This()) !?worker.Authority {
        return if (self.active) .{ .metadata_incarnation = @splat('a'), .binding_digest = @splat(1), .writer_generation = 1 } else null;
    }
    pub fn workJson(self: *@This(), alloc: std.mem.Allocator, query: work.Query) ![]u8 {
        try query.validate();
        const value = self.driver.value;
        const response: work.Response = switch (query) {
            .discover => |after| .{ .discovered = .{ .head = if (after == 0) value.head else null, .after_table = value.head.table_id, .exhausted = after != 0 } },
            .head => |identity| blk: {
                try identity.requireHead(value.head);
                break :blk .{ .head = value.head };
            },
            .routes => |request| blk: {
                try request.identity.requireHead(value.head);
                if (request.ordinal != 0 or value.head.owner_count != 1) return error.InvalidCatalogRecord;
                break :blk .{ .routes = .{ .head = value.head, .first_ordinal = 0, .table_name = value.dispatch.?.table_name, .targets = .{ .items = &.{.{ .owner = value.owner.owner, .start = "", .end = "" }} }, .next_ordinal = null } };
            },
        };
        return std.json.Stringify.valueAlloc(alloc, response, .{});
    }
    pub fn systemCatalog(self: *@This(), alloc: std.mem.Allocator, context: operation.RequestContext, call: calls.Call) ![]u8 {
        return self.driver.systemCatalog(alloc, context, call);
    }
};

fn testSchedulerFailedBuild(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State) !void {
    const worker = @import("../metadata/relational_index_worker.zig");
    var driver: TestDriver = .{ .value = permission, .binding = binding, .build_failed = true, .pending = true };
    var host: TestSchedulerHost = .{ .driver = &driver };
    var sender: TestDeliveryTransport = .{ .driver = &driver, .root = root };
    var transport = sender.localTransport();
    var coordinator: worker.Coordinator = .{};
    defer coordinator.deinit(std.testing.io);
    var now = @import("antfly_platform").time.monotonicNs();
    // A local failure with an outstanding durable-mirror obligation must not
    // become a metadata decision or start reading/delivering source claims.
    for (0..20) |_| {
        _ = try coordinator.step(std.testing.io, alloc, null, &host, &transport, now);
        now += 300 * std.time.ns_per_ms;
    }
    try std.testing.expect(driver.build_pages != 0);
    try std.testing.expectEqual(@as(usize, 0), driver.cancellation_requests);
    driver.pending = false;
    for (0..20) |_| {
        const before = sender.calls;
        _ = try coordinator.step(std.testing.io, alloc, null, &host, &transport, now);
        try std.testing.expect(sender.calls - before <= 1);
        now += 300 * std.time.ns_per_ms;
        if (driver.cancellation_requests != 0) break;
    }
    try std.testing.expectEqual(@as(usize, 1), driver.cancellation_requests);
    try std.testing.expectEqual(.canceling, driver.value.head.phase);
    try std.testing.expectEqual(.invalid_candidate, driver.value.head.failure);
    try std.testing.expectEqual(@as(usize, 0), driver.source_reads);
    try std.testing.expectEqual(@as(usize, 0), driver.claim_batches);
    try std.testing.expectEqual(@as(usize, 0), driver.installation_calls);
    try std.testing.expectEqual(@as(usize, 0), driver.release_calls);
    try std.testing.expectEqual(@as(u16, 0), driver.value.head.counts[5]);
    try std.testing.expectEqual(@as(u16, 0), driver.value.head.counts[6]);
}

fn testSchedulerDelivery(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State) !void {
    return testSchedulerDeliveryFault(alloc, permission, binding, root, false, .none);
}
fn testSchedulerDeliveryBeforeCommit(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State) !void {
    return testSchedulerDeliveryFault(alloc, permission, binding, root, true, .none);
}
fn testSchedulerValidationFaults(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State) !void {
    for ([_]TestDriver.ValidationFault{ .before_commit, .lost_reply, .pending, .stale_revision, .physical_only, .restart, .restart_committed }) |fault| try testSchedulerDeliveryFault(alloc, permission, binding, root, false, fault);
}
fn testSchedulerDeliveryFault(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State, before_commit: bool, validation_fault: TestDriver.ValidationFault) !void {
    const worker = @import("../metadata/relational_index_worker.zig");
    var driver: TestDriver = .{ .value = permission, .binding = binding, .lose_acceptance_reply = true, .lose_completion_reply = !before_commit, .lose_completion_before_commit = before_commit, .validation_fault = validation_fault };
    // Qualify the fixture's already-completed fence/reservation receipts too;
    // ACK status must not be weakened to accept unattested logical progress.
    const view = permission.native_signing.?;
    for ([_]local.storage_db_relational_index_publication_progress.Action{ .fenced, .reserved }, 1..) |action, sequence| {
        const receipt: local.storage_db_relational_index_publication_progress.Receipt = .{ .owner = permission.owner.owner, .action = action, .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = sequence } }, .candidate_digest = permission.head.candidate_digest, .reservation_digest = permission.head.reservation_digest, .evidence_digest = @splat(9) };
        try std.testing.expectEqual(try receipt.digest(), permission.owner.receipts[@backingInt(action)]);
        const proof = try (publication.RootProof{ .identity = view.identity, .reporter_incarnation = view.reporter_incarnation, .authority = .{ .native = view.binding_digest }, .root_generation = driver.root_generation, .signature = undefined }).sign(root, try receipt.digest());
        driver.value.owner.attestations[@backingInt(action)] = try proof.commitment(try receipt.digest());
    }
    var host: TestSchedulerHost = .{ .driver = &driver };
    var sender: TestDeliveryTransport = .{ .driver = &driver, .root = root };
    var transport = sender.localTransport();
    var coordinator: worker.Coordinator = .{};
    defer coordinator.deinit(std.testing.io);
    var now = @import("antfly_platform").time.monotonicNs();
    for (0..100) |_| {
        const before = sender.calls;
        _ = try coordinator.step(std.testing.io, alloc, null, &host, &transport, now);
        try std.testing.expect(sender.calls - before <= 1);
        const resident = active: {
            for (coordinator.jobs[0].?.owners) |owner| if (owner != null) break :active true;
            break :active false;
        };
        if (driver.completion_calls == 1 and !resident) break;
        now += 300 * std.time.ns_per_ms;
    }
    try std.testing.expectEqual(@as(usize, 1), driver.build_pages);
    try std.testing.expectEqual(@as(usize, 1), driver.acceptance_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.consumption_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.completion_calls);
    try std.testing.expectEqual(@as(usize, if (before_commit) 2 else 1), driver.completion_requests);
    driver.value.head.completed_sources = driver.value.head.owner_count;
    driver.value.head.completion_revision = driver.value.head.revision;
    driver.value.owner.source_delivery = driver.acceptance_head.?.decision_digest;
    driver.value.owner.source_completed = true;
    var restarted = false;
    for (0..60) |_| {
        now += 300 * std.time.ns_per_ms;
        _ = try coordinator.step(std.testing.io, alloc, null, &host, &transport, now);
        if (!restarted and (validation_fault == .restart or validation_fault == .restart_committed)) {
            var cut = false;
            for (coordinator.jobs[0].?.owners) |owner| if (owner) |value| {
                if (value.ack != null and driver.validation_acks == @as(usize, if (validation_fault == .restart_committed) 1 else 0)) cut = true;
            };
            if (cut) {
                coordinator.deinit(std.testing.io);
                coordinator = .{};
                restarted = true;
                continue;
            }
        }
        const resident = active: {
            for (coordinator.jobs[0].?.owners) |owner| if (owner != null) break :active true;
            break :active false;
        };
        if (driver.validation_acks == 1 and !resident and (validation_fault != .restart_committed or driver.decision_effects != 0)) break;
    }
    try std.testing.expect(driver.inventory_pages != 0);
    try std.testing.expect(driver.generation_pages != 0);
    try std.testing.expect(driver.audit_pages >= 2);
    try std.testing.expect(sender.signed_audits != 0);
    try std.testing.expectEqual(@as(usize, 1), driver.validation_seal_effects);
    try std.testing.expectEqual(@as(usize, 1), driver.validation_acks);
    try std.testing.expectEqual(@as(usize, if (validation_fault == .before_commit or validation_fault == .stale_revision) 2 else 1), driver.validation_ack_requests);
    if (validation_fault == .pending) try std.testing.expect(driver.validation_status_calls >= 4);
    if (validation_fault == .lost_reply or validation_fault == .before_commit) try std.testing.expect(driver.validation_status_calls != 0);
    if (validation_fault == .physical_only) try std.testing.expect(driver.audit_pages >= 5);
    if (validation_fault == .restart or validation_fault == .restart_committed) try std.testing.expect(restarted);
    if (validation_fault == .restart_committed) {
        try std.testing.expectEqual(@as(usize, 1), driver.decision_effects);
        try std.testing.expectEqual(local.storage_db_relational_index_publication_progress.Phase.installing, driver.value.head.phase);
    }
    try std.testing.expectEqual(@as(u16, 1), driver.value.head.counts[@backingInt(local.storage_db_relational_index_publication_progress.Action.validated)]);
    try std.testing.expectEqual(@as(usize, 1), driver.build_pages);
    try std.testing.expectEqual(@as(usize, 1), driver.consumption_calls);
    try std.testing.expectEqual(if (validation_fault == .restart_committed) local.storage_db_relational_index_publication_progress.Phase.installing else .building, driver.value.head.phase);
    host.active = false;
    const before = sender.calls;
    try std.testing.expect(!try coordinator.step(std.testing.io, alloc, null, &host, &transport, now));
    for (coordinator.jobs) |job| try std.testing.expect(job == null);
    try std.testing.expectEqual(before, sender.calls);
}

fn testDeliveryProducer(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State) !void {
    return testDeliveryProducerMode(alloc, permission, binding, root, false);
}
fn testDeliveryProducerLocal(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State) !void {
    return testDeliveryProducerMode(alloc, permission, binding, root, true);
}
fn testDeliveryProducerMode(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State, comptime local_mode: bool) !void {
    const producer_module = @import("relational_index_delivery_producer.zig");
    const table = permission.dispatch.?.table_name;
    var cohort = try jobs.private_claims.delivery.Cohort.init(alloc, &.{.{ .owner = permission.owner.owner, .start = "", .end = "" }});
    defer cohort.deinit();
    var driver: TestDriver = .{ .value = permission, .binding = binding, .lose_acceptance_reply = true };
    var sender: TestDeliveryTransport = .{ .driver = &driver, .root = root };
    var local_transport = sender.localTransport();
    const transport = if (local_mode) &local_transport else &sender;
    const request: Request = .{ .table_id = binding.fence.namespace.table_id, .group_id = binding.fence.owner_group_id, .id = binding.id, .operation = .source };
    var canceled = std.atomic.Value(bool).init(false);
    const context: operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 60 * std.time.ns_per_s, .cancellation = .fromAtomic(&canceled) };
    var producer = try DeliveryProducer.init(alloc, &cohort, table, request, context);
    defer producer.deinit();
    var observed: producer_module.Result = .{ .pending = .status };
    var pending_target = false;
    var resolved = false;
    for (0..16) |_| {
        const phase = producer.phase;
        if (phase == .target and !pending_target) driver.pending = true;
        const before = sender.calls;
        observed = try producer.step(transport, &driver);
        try std.testing.expect(sender.calls - before <= 1);
        if (phase == .target and driver.pending) {
            try std.testing.expectEqual(producer_module.Phase.target, producer.phase);
            try std.testing.expectEqual(@as(usize, 0), driver.acceptance_calls);
            try std.testing.expectEqual(@as(usize, 0), driver.consumption_calls);
            driver.pending = false;
            pending_target = true;
        }
        if (producer.phase == .resolve_accept) {
            resolved = true;
            try std.testing.expectEqual(@as(usize, 0), driver.consumption_calls);
            try std.testing.expect(producer.read == null and producer.plan == null and producer.delivered == null);
        }
        if (observed == .advanced) break;
    }
    try std.testing.expect(pending_target and resolved);
    try std.testing.expect(observed == .advanced and observed.advanced.eof and observed.advanced.drained);
    try std.testing.expectEqual(@as(u64, 1), observed.advanced.consumed.rows);
    try observed.advanced.consumed.verify();
    try std.testing.expectEqual(@as(usize, 2), driver.claim_batches);
    try std.testing.expectEqual(@as(usize, 1), driver.acceptance_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.consumption_calls);
    try std.testing.expectEqual(@as(usize, 1), driver.completion_calls);
    canceled.store(true, .release);
    const before = sender.calls;
    _ = try producer.step(transport, &driver);
    try std.testing.expectEqual(before, sender.calls);
}

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
        .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower),
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
        .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower),
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
    inline for (.{ .{ 409, error.UniqueConstraintViolation }, .{ 409, error.PrivateIndexOwnerJobChanged }, .{ 409, error.PrivateIndexPublicationCanceled }, .{ 503, error.InvalidPrivateIndexClaimReceipt }, .{ 503, error.PrivateIndexJobRequired }, .{ 503, error.ResourceBudgetExceeded }, .{ 503, error.PrivateIndexClaimBacklogFull }, .{ 503, error.MetadataReplicationPending }, .{ 408, error.DeadlineExceeded } }) |entry| {
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
        .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower),
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
    const root_binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower), .node_id = 2, .store_id = 3, .root_incarnation = 7 };
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
    driver.claims_sealed = true;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, claimStep(alloc, &driver, &driver, plan.before.name, request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.claim_batches);
    driver.claims_sealed = false;
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
        try testDeliveryProducer(alloc, permission, binding.?, root);
        try std.testing.checkAllAllocationFailures(alloc, testDeliveryProducer, .{ permission, binding.?, root });
        try testDeliveryProducerLocal(alloc, permission, binding.?, root);
        try std.testing.checkAllAllocationFailures(alloc, testDeliveryProducerLocal, .{ permission, binding.?, root });
        try testSchedulerDelivery(alloc, permission, binding.?, root);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, testSchedulerDelivery, .{ permission, binding.?, root });
        try testSchedulerDeliveryBeforeCommit(alloc, permission, binding.?, root);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, testSchedulerDeliveryBeforeCommit, .{ permission, binding.?, root });
        try testSchedulerValidationFaults(alloc, permission, binding.?, root);
        try testSchedulerFailedBuild(alloc, permission, binding.?, root);
        {
            var cohort = try jobs.private_claims.delivery.Cohort.init(alloc, &.{.{ .owner = permission.owner.owner, .start = "", .end = "" }});
            defer cohort.deinit();
            var failed_driver: TestDriver = .{ .value = permission, .binding = binding, .source_failed = true };
            var transport: TestDeliveryTransport = .{ .driver = &failed_driver, .root = root };
            const producer_context: operation.RequestContext = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() + 60 * std.time.ns_per_s };
            var producer = try DeliveryProducer.init(alloc, &cohort, plan.before.name, source_request, producer_context);
            defer producer.deinit();
            _ = try producer.step(&transport, &failed_driver);
            try std.testing.expectError(error.PrivateIndexBuildFailed, producer.step(&transport, &failed_driver));
            try std.testing.expectEqual(@as(usize, 0), failed_driver.claim_batches);
            try std.testing.expectEqual(@as(usize, 0), failed_driver.acceptance_calls);
            try std.testing.expectEqual(@as(usize, 0), failed_driver.consumption_calls);
            try std.testing.expectError(error.PrivateIndexOwnerJobChanged, producer.step(&transport, &failed_driver));
            var cancel_signal = std.atomic.Value(bool).init(true);
            try std.testing.expectError(error.Canceled, DeliveryProducer.init(alloc, &cohort, plan.before.name, source_request, .{ .deadline_ns = producer_context.deadline_ns, .cancellation = .fromAtomic(&cancel_signal) }));
            try std.testing.expectError(error.DeadlineExceeded, DeliveryProducer.init(alloc, &cohort, plan.before.name, source_request, .{ .deadline_ns = 0 }));
        }
        driver.pending = true;
        var pending_source = try step(alloc, &driver, &driver, plan.before.name, source_request, .{});
        defer pending_source.deinit();
        try std.testing.expect(!pending_source.needsSigning());
        try std.testing.expect(pending_source.source.outcome.batch == null);
        try std.testing.expectError(error.InvalidPrivateIndexClaimReceipt, (try pending_source.response()).source.decodeSigned(alloc, source_request));
    }
}

fn testSchedulerInstallationFaults(alloc: std.mem.Allocator, permission: publication.OwnerDecision, binding: jobs.Binding, root: local.storage_db_root_signing_identity.State, outcome: native.InstallationResponse) !void {
    const worker = @import("../metadata/relational_index_worker.zig");
    for ([_]bool{ false, true }) |automatic_decision| for ([_]TestDriver.ValidationFault{ .none, .before_commit, .lost_reply, .pending, .stale_revision, .restart, .restart_committed }) |fault| {
        var driver: TestDriver = .{ .value = permission, .binding = binding, .installation_response = outcome, .validation_fault = if (automatic_decision) .none else fault, .decision_fault = if (automatic_decision) fault else .none };
        if (automatic_decision) {
            driver.value.head.phase = .building;
            driver.value.head.revision -= 1;
            driver.value.native_install = null;
        }
        var host: TestSchedulerHost = .{ .driver = &driver };
        var sender: TestDeliveryTransport = .{ .driver = &driver, .root = root };
        var transport = sender.localTransport();
        var coordinator: worker.Coordinator = .{};
        defer coordinator.deinit(std.testing.io);
        var now = @import("antfly_platform").time.monotonicNs();
        var restarted = false;
        for (0..90) |_| {
            const before = sender.calls;
            _ = try coordinator.step(std.testing.io, alloc, null, &host, &transport, now);
            try std.testing.expect(sender.calls - before <= 1);
            if (!restarted and (fault == .restart or fault == .restart_committed)) {
                var cut = false;
                for (coordinator.jobs) |job| if (job) |active| {
                    for (active.owners) |owner| if (owner) |value| {
                        const stage_present = if (automatic_decision) value.decision != null else value.ack != null;
                        const completed = if (automatic_decision) driver.decision_effects else driver.installation_acks;
                        if (stage_present and completed == @as(usize, if (fault == .restart_committed) 1 else 0)) cut = true;
                    };
                };
                if (cut) {
                    coordinator.deinit(std.testing.io);
                    coordinator = .{};
                    restarted = true;
                    now += 300 * std.time.ns_per_ms;
                    continue;
                }
            }
            var resident = false;
            for (coordinator.jobs) |job| if (job) |active| {
                for (active.owners) |owner| if (owner != null) {
                    resident = true;
                };
            };
            if (driver.installation_acks == 1 and !resident) break;
            now += 300 * std.time.ns_per_ms;
        }
        try std.testing.expectEqual(@as(usize, 1), driver.installation_acks);
        try std.testing.expectEqual(@as(usize, if (!automatic_decision and (fault == .before_commit or fault == .stale_revision)) 2 else 1), driver.installation_ack_requests);
        if (automatic_decision) {
            try std.testing.expectEqual(@as(usize, 1), driver.decision_effects);
            try std.testing.expectEqual(@as(usize, if (fault == .before_commit or fault == .stale_revision) 2 else 1), driver.decision_requests);
        }
        try std.testing.expectEqual(local.storage_db_relational_index_publication_progress.Phase.releasing, driver.value.head.phase);
        try std.testing.expect(driver.installation_calls != 0);
        if (fault == .restart or fault == .restart_committed) try std.testing.expect(restarted);
    };
}

fn testReleaseReceiverAndScheduler(alloc: std.mem.Allocator, permission: publication.OwnerDecision, installed_ack: publication.Ack, root: local.storage_db_root_signing_identity.State, installed: native.InstallationResponse) !void {
    const progress = local.storage_db_relational_index_publication_progress;
    var active = permission;
    const update = try progress.acknowledge(active.head, active.owner, active.head.revision, installed_ack.signed.receipt);
    active.head = update.head;
    active.owner = update.owner;
    active.owner.attestations[@backingInt(progress.Action.installed)] = try installed_ack.signed.commitment();
    active.native_install = null;
    const logical = installed.installed.logical;
    active.native_release = .{ .installed = .{ .command = logical.command, .position = .{ .native = logical.position } }, .decision_digest = try publication.NativeReleaseGrant.decisionDigest(active.metadata_group_id, active.metadata_incarnation, active.head), .installation_proof = installed_ack.signed.proof };
    const request: Request = .{ .table_id = active.head.table_id, .group_id = active.owner.owner.group_id, .id = active.head.plan_id, .operation = .release };
    var driver: TestDriver = .{ .value = active, .installation_response = installed, .root_generation = installed_ack.signed.proof.root_generation };
    driver.root_generation += 1;
    try std.testing.expectError(error.PrivateIndexRootRecoveryRequired, step(alloc, &driver, &driver, active.dispatch.?.table_name, request, .{}));
    try std.testing.expectEqual(@as(usize, 0), driver.release_calls);
    try std.testing.expectEqual(@as(usize, 0), driver.release_effects);
    driver.root_generation -= 1;
    var result = try step(alloc, &driver, &driver, active.dispatch.?.table_name, request, .{});
    defer result.deinit();
    try std.testing.expect(result == .releasing and result.needsSigning());
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
    const captured_root = driver.root_generation;
    driver.root_generation += 1;
    try std.testing.expectEqual(captured_root, result.rootGeneration().?);
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 3, 5));
    try result.signNative(root, 2, 3, 4);
    const response = try result.response();
    const ack = (try response.releaseAck(request)).?;
    try std.testing.expect(ack.validation == null and ack.installation == null);
    try std.testing.expectEqual(captured_root, ack.signed.proof.root_generation);
    driver.root_generation = captured_root;
    var retry = try step(alloc, &driver, &driver, active.dispatch.?.table_name, request, .{});
    defer retry.deinit();
    try std.testing.expect(retry.releasing.outcome.value.duplicate);
    try std.testing.expectEqualDeep(result.releasing.outcome.value.position, retry.releasing.outcome.value.position);
    retry.signingFailed(.identity_unavailable);
    try std.testing.expect(!retry.needsSigning());
    try std.testing.expect(try (try retry.response()).releaseAck(request) == null);
    driver.release_outcome.?.replication_pending = true;
    driver.release_outcome.?.error_name = "ReplicationOutboxPending";
    driver.release_outcome.?.receipt = null;
    var pending = try step(alloc, &driver, &driver, active.dispatch.?.table_name, request, .{});
    defer pending.deinit();
    try std.testing.expect(!pending.needsSigning());
    try std.testing.expectError(error.NativeIndexSigningUnavailable, pending.signNative(root, 2, 3, 4));
    try std.testing.expect(try (try pending.response()).releaseAck(request) == null);
    try std.testing.expectEqual(@as(usize, 1), driver.release_effects);

    const worker = @import("../metadata/relational_index_worker.zig");
    for ([_]TestDriver.ValidationFault{ .none, .before_commit, .lost_reply, .pending, .stale_revision, .restart, .restart_committed }) |fault| {
        var scheduled: TestDriver = .{ .value = active, .installation_response = installed, .root_generation = captured_root, .validation_fault = fault };
        var host: TestSchedulerHost = .{ .driver = &scheduled };
        var sender: TestDeliveryTransport = .{ .driver = &scheduled, .root = root };
        var transport = sender.localTransport();
        var coordinator: worker.Coordinator = .{};
        defer coordinator.deinit(std.testing.io);
        var now = @import("antfly_platform").time.monotonicNs();
        var restarted = false;
        for (0..90) |_| {
            const before = sender.calls;
            _ = try coordinator.step(std.testing.io, alloc, null, &host, &transport, now);
            try std.testing.expect(sender.calls - before <= 1);
            if (!restarted and (fault == .restart or fault == .restart_committed)) {
                var cut = false;
                for (coordinator.jobs) |job| if (job) |current| {
                    for (current.owners) |owner| if (owner) |value| {
                        if (value.ack != null and scheduled.release_acks == @as(usize, if (fault == .restart_committed) 1 else 0)) cut = true;
                    };
                };
                if (cut) {
                    coordinator.deinit(std.testing.io);
                    coordinator = .{};
                    restarted = true;
                    now += 300 * std.time.ns_per_ms;
                    continue;
                }
            }
            var resident = false;
            for (coordinator.jobs) |job| if (job) |current| {
                for (current.owners) |owner| if (owner != null) {
                    resident = true;
                };
            };
            if (scheduled.release_acks == 1 and !resident) break;
            now += 300 * std.time.ns_per_ms;
        }
        try std.testing.expectEqual(@as(usize, 1), scheduled.release_effects);
        try std.testing.expectEqual(@as(usize, 1), scheduled.release_acks);
        try std.testing.expectEqual(@as(usize, if (fault == .before_commit or fault == .stale_revision) 2 else 1), scheduled.release_ack_requests);
        try std.testing.expectEqual(progress.Phase.published, scheduled.value.head.phase);
        if (fault == .restart or fault == .restart_committed) try std.testing.expect(restarted);
    }
}

test "relational index publication installed receiver binds fresh decision and retains paired physical witnesses" {
    const alloc = std.testing.allocator;
    const progress = local.storage_db_relational_index_publication_progress;
    const integrity = local.storage_db_relational_integrity_contract;
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
    var driver: TestDriver = .{ .value = .{ .metadata_group_id = 13, .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower), .head = prepared.head, .owner = prepared.owners[0], .disposition = .execute, .dispatch = dispatch.value, .inputs = inputs.value } };
    const admission = try driver.value.nativeAdmission();
    var admitted = try driver.admitNativeIndexOwner(alloc, plan.owners[0].range.group_id, plan.before.name, admission, "synthetic", .{});
    defer admitted.deinit();
    driver.binding.?.phase = .reserved;
    const binding = driver.binding.?;
    inline for (.{ progress.Action.fenced, progress.Action.reserved }, 1..) |action, sequence| {
        const next = try progress.acknowledge(driver.value.head, driver.value.owner, driver.value.head.revision, .{ .owner = driver.value.owner.owner, .action = action, .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = sequence } }, .candidate_digest = prepared.head.candidate_digest, .reservation_digest = prepared.head.reservation_digest, .evidence_digest = @splat(9) });
        driver.value.head = next.head;
        driver.value.owner = next.owner;
    }
    driver.value.head.completed_sources = 1;
    driver.value.head.completion_revision = 1;
    driver.value.owner.source_completed = true;
    driver.value.owner.source_delivery = @splat(9);
    var build = try native.BuildRequest.init(try driver.value.nativeExecutionIdentity(.{ .table_id = plan.before.table_id, .group_id = plan.owners[0].range.group_id, .id = plan.id, .operation = .build }), binding, .{});
    build.target_expectation = .{ .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower), .binding_digest = integrity.hash(&try binding.encode()), .generation = @splat(2), .completion_revision = 1, .source_consumption_digest = @splat(3), .source_progress_digest = @splat(4), .table_name_digest = integrity.hash(plan.before.name), .rows = 1, .inventory_xor = @splat(5), .provenance = @splat(6) };
    driver.inventory_finished = true;
    driver.generation_pages = 1;
    driver.audit_pages = 1;
    var built = try driver.stepNativeIndexOwnerBuildAtRoot(alloc, plan.owners[0].range.group_id, plan.before.name, build, 7, .{});
    defer built.outcome.deinit();
    try built.outcome.value.validateFor(build);
    const validated = built.outcome.value.validated;
    const acknowledged = try progress.acknowledge(driver.value.head, driver.value.owner, driver.value.head.revision, validated.logical.receipt.?);
    driver.value.head = try acknowledged.head.decidePublish(acknowledged.head.revision);
    driver.value.owner = acknowledged.owner;
    driver.value.native_install = .{ .validation = .{ .command = validated.logical.command, .position = .{ .native = validated.logical.position } }, .decision_digest = try publication.NativeInstallGrant.decisionDigest(13, driver.value.metadata_incarnation, driver.value.head) };
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .public_key = pair.public_key.toBytes(), .root_incarnation = 7 };
    const owner_binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower), .node_id = 2, .store_id = 3, .root_incarnation = 7 };
    driver.value.native_signing = .{ .identity = .{ .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key }, .reporter_incarnation = 4, .binding_digest = try owner_binding.digest() };
    const command: local.storage_db_relational_index_validation.InstallCommand = .{ .validation = driver.value.native_install.?.validation, .metadata_decision = driver.value.native_install.?.decision_digest, .schema_digest = @splat(3), .activation_digest = @splat(4) };
    const position: local.storage_db_receipt_position.Native = .{ .namespace = binding.fence.namespace, .sequence = 9 };
    driver.installation_response = .{ .installed = .{ .physical = validated.physical, .logical = .{ .command = command, .position = position, .duplicate = false, .receipt = try command.receipt(position) } } };
    const signing_view = driver.value.native_signing.?;
    for ([_]progress.Action{ .fenced, .reserved, .validated }, [_]u64{ 1, 2, 8 }) |action, sequence| {
        const receipt: progress.Receipt = if (action == .validated) try validated.logical.command.receipt(validated.logical.position) else .{ .owner = driver.value.owner.owner, .action = action, .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = sequence } }, .candidate_digest = prepared.head.candidate_digest, .reservation_digest = prepared.head.reservation_digest, .evidence_digest = @splat(9) };
        const proof = try (publication.RootProof{ .identity = signing_view.identity, .reporter_incarnation = signing_view.reporter_incarnation, .authority = .{ .native = signing_view.binding_digest }, .root_generation = driver.root_generation, .signature = undefined }).sign(root, try receipt.digest());
        driver.value.owner.attestations[@backingInt(action)] = try proof.commitment(try receipt.digest());
    }
    try testSchedulerInstallationFaults(alloc, driver.value, binding, root, driver.installation_response.?);
    const request: Request = .{ .table_id = plan.before.table_id, .group_id = plan.owners[0].range.group_id, .id = plan.id, .operation = .install };
    var result = try step(alloc, &driver, &driver, plan.before.name, request, .{});
    defer result.deinit();
    try std.testing.expect(result == .installing and result.needsSigning());
    try std.testing.expectEqual(@as(usize, 1), driver.installation_calls);
    try std.testing.expectEqual(@as(usize, 0), driver.inspections);
    try std.testing.expectEqual(@as(usize, 0), driver.admission_reads);
    driver.root_generation += 1;
    try std.testing.expectEqual(@as(u64, 9), result.rootGeneration().?);
    try std.testing.expectError(error.IndexOwnerReceiptChanged, result.signNative(root, 2, 3, 5));
    try result.signNative(root, 2, 3, 4);
    const response = try result.response();
    const ack = (try response.installationAck(request)).?;
    try testReleaseReceiverAndScheduler(alloc, driver.value, ack, root, driver.installation_response.?);
    try std.testing.expect(ack.validation == null and ack.installation != null);
    var bare = ack;
    bare.installation = null;
    try std.testing.expectError(error.InvalidIndexOwnerReceipt, bare.validate());
    const encoded = try publication.StoredProof.encode(ack.signed, ack.validation, ack.installation, ack.cancellation);
    const decoded = try publication.StoredProof.decode(alloc, encoded.slice());
    try std.testing.expectEqualDeep(ack.installation.?, decoded.installation.?);
    const fixed = try ack.signed.encode();
    try std.testing.expectError(error.InvalidCatalogRecord, publication.StoredProof.decode(alloc, &fixed));
    var substituted = ack;
    substituted.installation.?.command.metadata_decision[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexOwnerReceipt, substituted.validate());
    var incomplete = response;
    incomplete.installing.signed_audit = null;
    try std.testing.expectError(error.InvalidPrivateIndexNativeReceipt, incomplete.validate(request));
    driver.installation_response.?.installed.logical.replication_pending = true;
    driver.installation_response.?.installed.logical.error_name = "ReplicationOutboxPending";
    driver.installation_response.?.installed.logical.receipt = null;
    var pending = try step(alloc, &driver, &driver, plan.before.name, request, .{});
    defer pending.deinit();
    try std.testing.expect(!pending.needsSigning());
    try std.testing.expectError(error.NativeIndexSigningUnavailable, pending.signNative(root, 2, 3, 4));
    try std.testing.expect(try (try pending.response()).installationAck(request) == null);
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
    const binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower), .node_id = 2, .store_id = 3, .root_incarnation = 7 };
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
        .metadata_incarnation = std.fmt.bytesToHex(plan.table_guard.incarnation, .lower),
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
