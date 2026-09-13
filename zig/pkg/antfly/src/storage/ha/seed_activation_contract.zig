// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Data-only request contracts shared by distributed control and the compiled
//! storage owner. Physical seed activation and DB materialization must not be
//! imported from this module.

const seed_artifact = @import("seed_artifact.zig");

pub const ActivationBinding = seed_artifact.LifecycleBinding;

pub const MaterializationTarget = struct {
    target_local_node_id: u64,
    target_replica_id: u64 = 1,
};

pub const ActivateRequest = struct {
    staging_root: []const u8,
    target_root: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    binding: ?ActivationBinding = null,
    materialization: ?MaterializationTarget = null,
    pod_uid: ?[]const u8 = null,
    limits: seed_artifact.Limits = .{},
};

pub const StartupExpectation = struct {
    target_root: []const u8,
    expected: seed_artifact.ExpectedArtifact,
    binding: ActivationBinding,
    manifest_sha256: ?[]const u8 = null,
    aggregate_sha256: ?[]const u8 = null,
    seed_receipt_sha256: ?[]const u8 = null,
    capture_receipt_sha256: ?[]const u8 = null,
    materialized_receipt_sha256: ?[]const u8 = null,
    materialized_aggregate_sha256: ?[]const u8 = null,
    target_local_node_id: ?u64 = null,
    target_replica_id: ?u64 = null,
    limits: seed_artifact.Limits = .{},
};

pub const ActivatedGenerationGCRequest = struct {
    target_root: []const u8,
    /// Durable controller-owned copy of HASeededSlotActivateResponse.
    slot_activation_receipt_path: []const u8,
    protected_generations: []const []const u8 = &.{},
    retain_generations: usize = 2,
    limits: seed_artifact.Limits = .{},
    max_local_generations: usize = 10_000,
};
