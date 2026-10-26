// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Test-only installed-model boundary for the stored all-36 diagnostic.
//!
//! The caller must own a live packed Session produced by the installed model
//! and pass the model's real CampaignRunner. This module adds no serving route,
//! profile enrollment, candidate construction, or release authority.
const std = @import("std");
const builtin = @import("builtin");
const coordinator = @import("compaction_coordinator.zig");
const packed_mod = @import("packed_context_session.zig");
const payload = @import("../runtime/kv/payload.zig");
const storage = @import("packed_context_storage.zig");
const profiles = @import("../runtime/kv/compaction_profile.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");
const teacher_forced = @import("context_teacher_forced.zig");

pub const schema = "antfly.context.stored-installed-invocation.v1";

pub const FrozenIdentity = struct {
    load_generation: u64,
    model_identity: profiles.Identity,
    algorithm_sha256: [32]u8,
    build_sha256: [32]u8,
    artifact_manifest_sha256: [32]u8,
    profile_sha256: [32]u8,
    source_descriptor_sha256: [32]u8,
    source_payload_sha256: [32]u8,
    expected_baseline_context_bytes: u64 = 0,
    expected_global_owner_count: u32 = 0,
    expected_total_owner_count: u32 = 0,
};

pub const BaselinePreflightReceipt = struct {
    schema_name: []const u8 = "antfly.context.stored-installed-baseline-preflight.v1",
    completed: bool = true,
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    load_generation: u64,
    model_identity: profiles.Identity,
    build_sha256: [32]u8,
    prompt_tokens: u32,
    prompt_tokens_sha256: [32]u8,
    session_identity: u64,
    source_descriptor_sha256: [32]u8,
    source_payload_sha256: [32]u8,
    native_peak_budget: usize,
    tracker_owner_identity: u64,
    tracker_live_bytes: u64,
    tracker_peak_bytes: u64,
    baseline_context_physical_bytes: u64,
    replay_control_bytes: u64,
    global_owner_count: u32,
    total_owner_count: u32,
};

fn validateExpectedBaseline(frozen: FrozenIdentity, measured: packed_mod.CommittedContextStorageDiagnostic) !void {
    if (frozen.expected_baseline_context_bytes == 0 or frozen.expected_global_owner_count == 0 or
        frozen.expected_total_owner_count < frozen.expected_global_owner_count or
        frozen.expected_baseline_context_bytes != measured.physical_bytes or
        frozen.expected_global_owner_count != measured.global_owner_count or
        frozen.expected_total_owner_count != measured.total_owner_count or measured.replay_control_bytes != 0)
        return error.InvalidPayloadProvenance;
}

pub const Receipt = struct {
    schema_name: []const u8 = schema,
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    completed: bool = false,
    load_generation: u64 = 0,
    model_identity: ?profiles.Identity = null,
    prompt_tokens: u32 = 0,
    prompt_tokens_sha256: [32]u8 = @splat(0),
    session_identity: u64 = 0,
    publication_epoch_before: u64 = 0,
    publication_epoch_after: u64 = 0,
    source_descriptor_sha256: [32]u8 = @splat(0),
    source_payload_sha256: [32]u8 = @splat(0),
    diagnostic_kv_readback_bytes_before: u64 = 0,
    diagnostic_kv_readback_bytes_after: u64 = 0,
    terminal: coordinator.StoredDiagnosticReceipt = .{},
};

pub const Request = struct {
    frozen: FrozenIdentity,
    prompt_tokens_sha256: [32]u8,
    sources: coordinator.PreparedQuerySources,
    authority: coordinator.CampaignAuthority,
    actual: profiles.Identity,
    provider: coordinator.StoredDiagnosticProvider,
    eligible: *const payload.Eligible,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    native_peak_budget: usize,
    receipt: Receipt = .{},
    consumed: bool = false,

    const ProviderBinding = struct {
        request: *Request,

        fn reserve(raw: *anyopaque) !coordinator.StoredDiagnosticReservation {
            const self: *ProviderBinding = @ptrCast(@alignCast(raw));
            const reservation = try self.request.provider.reserve_fn(self.request.provider.ctx);
            validateFrozenReservation(self.request.frozen, reservation) catch |err| {
                if (reservation.reservation_identity == 0) return error.StoredDiagnosticLedgerFailure;
                const failure = self.request.provider.abort_reservation_fn(
                    self.request.provider.ctx,
                    reservation.reservation_identity,
                ) catch {
                    self.request.receipt.terminal.state = .ledger_failure;
                    return error.StoredDiagnosticLedgerFailure;
                };
                if (failure.reservation_identity != reservation.reservation_identity or !failure.durable or
                    !failure.nonreuse_committed or std.mem.allEqual(u8, &failure.evidence_sha256, 0))
                {
                    self.request.receipt.terminal.state = .ledger_failure;
                    return error.StoredDiagnosticLedgerFailure;
                }
                self.request.receipt.terminal = .{
                    .state = .failed_before_commit,
                    .reservation_identity = reservation.reservation_identity,
                    .algorithm_sha256 = reservation.algorithm_sha256,
                    .build_sha256 = reservation.build_sha256,
                    .artifact_manifest_sha256 = reservation.artifact_manifest_sha256,
                    .profile_sha256 = reservation.profile_sha256,
                    .failure = failure,
                };
                return err;
            };
            return reservation;
        }

        fn materialize(raw: *anyopaque, identity: packed_mod.StoredCampaignIdentity, tracker: *owner_alloc.HostAllocationTracker) !coordinator.StoredDiagnosticMaterialization {
            const self: *ProviderBinding = @ptrCast(@alignCast(raw));
            return self.request.provider.materialize_fn(self.request.provider.ctx, identity, tracker);
        }

        fn precommit(raw: *anyopaque, identity: packed_mod.StoredCampaignIdentity, manifest: [32]u8) !void {
            const self: *ProviderBinding = @ptrCast(@alignCast(raw));
            return self.request.provider.precommit_fn(self.request.provider.ctx, identity, manifest);
        }

        fn commit(raw: *anyopaque, reservation_identity: u64, publication: packed_mod.StoredPublicationReceipt) !void {
            const self: *ProviderBinding = @ptrCast(@alignCast(raw));
            return self.request.provider.commit_reservation_fn(self.request.provider.ctx, reservation_identity, publication);
        }

        fn abort(raw: *anyopaque, reservation_identity: u64) !coordinator.StoredReservationFailureReceipt {
            const self: *ProviderBinding = @ptrCast(@alignCast(raw));
            return self.request.provider.abort_reservation_fn(self.request.provider.ctx, reservation_identity);
        }

        fn provider(self: *ProviderBinding) coordinator.StoredDiagnosticProvider {
            return .{
                .ctx = self,
                .reserve_fn = reserve,
                .materialize_fn = materialize,
                .precommit_fn = precommit,
                .commit_reservation_fn = commit,
                .abort_reservation_fn = abort,
            };
        }
    };

    pub fn run(self: *Request, live: *packed_mod.Session, prompt: []const i64, installed_load_generation: u64, runner: coordinator.CampaignRunner) !void {
        if (!builtin.is_test) return error.UnqualifiedCompactionArtifact;
        if (self.consumed or installed_load_generation == 0 or installed_load_generation != self.frozen.load_generation or
            prompt.len == 0 or prompt.len != @as(usize, live.logical_tokens) or self.native_peak_budget == 0 or
            std.mem.allEqual(u8, &self.prompt_tokens_sha256, 0))
            return error.InvalidContextTransaction;
        self.consumed = true;
        self.receipt = .{};

        const prompt_digest = try teacher_forced.promptTokenDigest(prompt);
        if (!std.meta.eql(prompt_digest, self.prompt_tokens_sha256) or
            !std.meta.eql(prompt_digest, self.sources.question.prompt_tokens_sha256))
            return error.InvalidPayloadProvenance;
        const capture = switch (self.authority) {
            .capture => |value| value,
            .production => return error.UnqualifiedCompactionArtifact,
        };
        if (capture.load_generation != installed_load_generation or !std.meta.eql(capture.identity, self.actual) or
            !std.meta.eql(self.actual, self.frozen.model_identity))
            return error.InvalidPayloadProvenance;

        const before = try live.qualificationFingerprint();
        if (@as(usize, before.logical_tokens) != prompt.len or
            !std.meta.eql(before.descriptor_sha256, self.frozen.source_descriptor_sha256) or
            !std.meta.eql(before.payload_sha256, self.frozen.source_payload_sha256) or
            before.diagnostic_kv_readback_bytes == 0)
            return error.InvalidPayloadProvenance;
        const measured_baseline = try live.committedContextStorageDiagnostic();
        // This validation is deliberately before ProviderBinding and therefore
        // before the external provider can create a tuple claim or ledger.
        try validateExpectedBaseline(self.frozen, measured_baseline);
        self.receipt = .{
            .load_generation = installed_load_generation,
            .model_identity = self.actual,
            .prompt_tokens = @intCast(prompt.len),
            .prompt_tokens_sha256 = prompt_digest,
            .session_identity = before.context_instance_id,
            .publication_epoch_before = before.publication_epoch,
            .source_descriptor_sha256 = before.descriptor_sha256,
            .source_payload_sha256 = before.payload_sha256,
            .diagnostic_kv_readback_bytes_before = before.diagnostic_kv_readback_bytes,
        };

        var binding = ProviderBinding{ .request = self };
        coordinator.runStoredCampaignDiagnostic(
            live,
            prompt,
            self.sources,
            self.authority,
            self.actual,
            runner,
            binding.provider(),
            self.eligible,
            self.parts,
            self.retained_limit,
            self.native_peak_budget,
            &self.receipt.terminal,
        ) catch |err| return err;

        errdefer {
            if (self.receipt.terminal.state == .committed)
                self.receipt.terminal.state = .committed_but_receipt_failed;
        }
        const after = try live.qualificationFingerprint();
        const publication = self.receipt.terminal.publication orelse return error.InvalidContextTransaction;
        const expected_epoch = std.math.add(u64, before.publication_epoch, 1) catch
            return error.InvalidContextTransaction;
        if (self.receipt.terminal.state != .committed or !self.receipt.terminal.diagnostic_only or
            self.receipt.terminal.serving_authority or self.receipt.terminal.release_authority or
            !std.meta.eql(self.receipt.terminal.algorithm_sha256, self.frozen.algorithm_sha256) or
            !std.meta.eql(self.receipt.terminal.build_sha256, self.frozen.build_sha256) or
            !std.meta.eql(self.receipt.terminal.artifact_manifest_sha256, self.frozen.artifact_manifest_sha256) or
            !std.meta.eql(self.receipt.terminal.profile_sha256, self.frozen.profile_sha256) or
            publication.load_generation != installed_load_generation or
            publication.session_identity != before.context_instance_id or
            publication.publication_epoch_before != before.publication_epoch or
            publication.publication_epoch_after != after.publication_epoch or
            after.context_instance_id != before.context_instance_id or
            after.publication_epoch != expected_epoch or after.logical_tokens != before.logical_tokens or
            !std.meta.eql(publication.source_descriptor_sha256, before.descriptor_sha256) or
            !std.meta.eql(publication.source_payload_sha256, before.payload_sha256) or
            after.diagnostic_kv_readback_bytes == 0)
            return error.InvalidContextTransaction;
        self.receipt.publication_epoch_after = after.publication_epoch;
        self.receipt.diagnostic_kv_readback_bytes_after = after.diagnostic_kv_readback_bytes;
        self.receipt.completed = true;
    }

    pub fn measureBaseline(self: *Request, live: *packed_mod.Session, prompt: []const i64, installed_load_generation: u64) !BaselinePreflightReceipt {
        if (!builtin.is_test or self.consumed or installed_load_generation == 0 or
            installed_load_generation != self.frozen.load_generation or prompt.len == 0 or
            prompt.len != @as(usize, live.logical_tokens)) return error.InvalidContextTransaction;
        const prompt_digest = try teacher_forced.promptTokenDigest(prompt);
        if (!std.meta.eql(prompt_digest, self.prompt_tokens_sha256) or
            !std.meta.eql(self.actual, self.frozen.model_identity)) return error.InvalidPayloadProvenance;
        const fingerprint = try live.qualificationFingerprint();
        if (!std.meta.eql(fingerprint.descriptor_sha256, self.frozen.source_descriptor_sha256) or
            !std.meta.eql(fingerprint.payload_sha256, self.frozen.source_payload_sha256) or
            fingerprint.diagnostic_kv_readback_bytes == 0) return error.InvalidPayloadProvenance;
        const measured = try live.committedContextStorageDiagnostic();
        return .{
            .load_generation = installed_load_generation,
            .model_identity = self.actual,
            .build_sha256 = self.frozen.build_sha256,
            .prompt_tokens = @intCast(prompt.len),
            .prompt_tokens_sha256 = prompt_digest,
            .session_identity = fingerprint.context_instance_id,
            .source_descriptor_sha256 = fingerprint.descriptor_sha256,
            .source_payload_sha256 = fingerprint.payload_sha256,
            .native_peak_budget = self.native_peak_budget,
            .tracker_owner_identity = measured.tracker_snapshot.owner_identity,
            .tracker_live_bytes = measured.tracker_snapshot.tracked_combined_live_bytes,
            .tracker_peak_bytes = measured.tracker_snapshot.tracked_combined_peak_bytes,
            .baseline_context_physical_bytes = measured.physical_bytes,
            .replay_control_bytes = measured.replay_control_bytes,
            .global_owner_count = measured.global_owner_count,
            .total_owner_count = measured.total_owner_count,
        };
    }
};

fn validateFrozenReservation(frozen: FrozenIdentity, reservation: coordinator.StoredDiagnosticReservation) !void {
    if (reservation.artifact_count != 36 or reservation.reservation_identity == 0 or
        !std.meta.eql(reservation.algorithm_sha256, frozen.algorithm_sha256) or
        !std.meta.eql(reservation.build_sha256, frozen.build_sha256) or
        !std.meta.eql(reservation.artifact_manifest_sha256, frozen.artifact_manifest_sha256) or
        !std.meta.eql(reservation.profile_sha256, frozen.profile_sha256) or
        !std.meta.eql(reservation.source_descriptor_sha256, frozen.source_descriptor_sha256) or
        !std.meta.eql(reservation.source_payload_sha256, frozen.source_payload_sha256))
        return error.InvalidPayloadProvenance;
}

test "installed stored invocation rejects substituted frozen manifest before materialization" {
    const frozen = FrozenIdentity{
        .load_generation = 7,
        .model_identity = undefined,
        .algorithm_sha256 = @splat(1),
        .build_sha256 = @splat(2),
        .artifact_manifest_sha256 = @splat(3),
        .profile_sha256 = @splat(4),
        .source_descriptor_sha256 = @splat(5),
        .source_payload_sha256 = @splat(6),
    };
    var reservation = coordinator.StoredDiagnosticReservation{
        .reservation_identity = 9,
        .algorithm_sha256 = frozen.algorithm_sha256,
        .build_sha256 = frozen.build_sha256,
        .artifact_manifest_sha256 = frozen.artifact_manifest_sha256,
        .profile_sha256 = frozen.profile_sha256,
        .coordinates = &.{},
        .source_descriptor_sha256 = frozen.source_descriptor_sha256,
        .source_payload_sha256 = frozen.source_payload_sha256,
        .loader_budget_bytes = 1,
        .materialization_host_bytes = 1,
        .artifact_count = 36,
        .expected_baseline_context_bytes = 1,
        .expected_owner_count = 3,
        .expected_total_owner_count = 36,
    };
    try validateFrozenReservation(frozen, reservation);
    reservation.artifact_manifest_sha256[0] ^= 1;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateFrozenReservation(frozen, reservation));
}

test "installed stored invocation receipt defaults deny serving and release authority" {
    const receipt = Receipt{};
    try std.testing.expect(receipt.diagnostic_only);
    try std.testing.expect(!receipt.serving_authority);
    try std.testing.expect(!receipt.release_authority);
    try std.testing.expect(!receipt.completed);
}

test "installed stored invocation closes mismatched reservation before materialization" {
    const Probe = struct {
        aborts: u32 = 0,
        materializations: u32 = 0,

        fn reserve(_: *anyopaque) !coordinator.StoredDiagnosticReservation {
            return .{
                .reservation_identity = 19,
                .algorithm_sha256 = @splat(1),
                .build_sha256 = @splat(2),
                .artifact_manifest_sha256 = @splat(99),
                .profile_sha256 = @splat(4),
                .coordinates = &.{},
                .source_descriptor_sha256 = @splat(5),
                .source_payload_sha256 = @splat(6),
                .loader_budget_bytes = 1,
                .materialization_host_bytes = 1,
                .artifact_count = 36,
                .expected_baseline_context_bytes = 1,
                .expected_owner_count = 3,
                .expected_total_owner_count = 36,
            };
        }
        fn materialize(raw: *anyopaque, _: packed_mod.StoredCampaignIdentity, _: *owner_alloc.HostAllocationTracker) !coordinator.StoredDiagnosticMaterialization {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.materializations += 1;
            return error.UnexpectedMaterialization;
        }
        fn precommit(_: *anyopaque, _: packed_mod.StoredCampaignIdentity, _: [32]u8) !void {}
        fn commit(_: *anyopaque, _: u64, _: packed_mod.StoredPublicationReceipt) !void {}
        fn abort(raw: *anyopaque, reservation_identity: u64) !coordinator.StoredReservationFailureReceipt {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.aborts += 1;
            return .{ .reservation_identity = reservation_identity, .durable = true, .nonreuse_committed = true, .evidence_sha256 = @splat(12) };
        }
    };
    var probe = Probe{};
    var request = Request{
        .frozen = .{
            .load_generation = 7,
            .model_identity = undefined,
            .algorithm_sha256 = @splat(1),
            .build_sha256 = @splat(2),
            .artifact_manifest_sha256 = @splat(3),
            .profile_sha256 = @splat(4),
            .source_descriptor_sha256 = @splat(5),
            .source_payload_sha256 = @splat(6),
        },
        .prompt_tokens_sha256 = @splat(7),
        .sources = undefined,
        .authority = undefined,
        .actual = undefined,
        .provider = .{ .ctx = &probe, .reserve_fn = Probe.reserve, .materialize_fn = Probe.materialize, .precommit_fn = Probe.precommit, .commit_reservation_fn = Probe.commit, .abort_reservation_fn = Probe.abort },
        .eligible = undefined,
        .parts = &.{},
        .retained_limit = 1,
        .native_peak_budget = 1,
    };
    var binding = Request.ProviderBinding{ .request = &request };
    const bound_provider = binding.provider();
    try std.testing.expectError(error.InvalidPayloadProvenance, bound_provider.reserve_fn(bound_provider.ctx));
    try std.testing.expectEqual(@as(u32, 1), probe.aborts);
    try std.testing.expectEqual(@as(u32, 0), probe.materializations);
    try std.testing.expectEqual(.failed_before_commit, request.receipt.terminal.state);
    try std.testing.expectEqual(@as(u64, 19), request.receipt.terminal.reservation_identity);
    try std.testing.expect(request.receipt.terminal.failure != null);
    try std.testing.expect(request.receipt.terminal.failure.?.nonreuse_committed);
}

test "installed expected baseline distinguishes global and total owners" {
    const frozen = FrozenIdentity{ .load_generation = 1, .model_identity = undefined, .algorithm_sha256 = @splat(1), .build_sha256 = @splat(2), .artifact_manifest_sha256 = @splat(3), .profile_sha256 = @splat(4), .source_descriptor_sha256 = @splat(5), .source_payload_sha256 = @splat(6), .expected_baseline_context_bytes = 100, .expected_global_owner_count = 3, .expected_total_owner_count = 36 };
    const good = packed_mod.CommittedContextStorageDiagnostic{ .owner_count = 36, .global_owner_count = 3, .total_owner_count = 36, .physical_bytes = 100, .replay_control_bytes = 0, .tracker_snapshot = .{} };
    try validateExpectedBaseline(frozen, good);
    var bad = good;
    bad.global_owner_count = 36;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateExpectedBaseline(frozen, bad));
    bad = good;
    bad.total_owner_count = 3;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateExpectedBaseline(frozen, bad));
    bad = good;
    bad.physical_bytes = 99;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateExpectedBaseline(frozen, bad));
    bad = good;
    bad.replay_control_bytes = 1;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateExpectedBaseline(frozen, bad));
}
