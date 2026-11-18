// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Exact source-artifact gate for the accurate CUDA Tensor Core projections.
const std = @import("std");
const boundary = @import("gliner_boundary_qualification.zig");
const bundle = @import("gliner_boundary_bundle.zig");
const decide = @import("gliner_decide_qualification.zig");

pub fn identityEligible(base: ?bundle.Identity, decision: ?decide.Identity) bool {
    if (decision) |identity| if (decide.qualifiedVariant(identity) == .decide_deberta) return true;
    const identity = base orelse return false;
    return identity.backbone == .base and identity.precision == .fp32 and
        std.mem.eql(u8, &identity.weight.sha256, &boundary.fastino_gliner25_base_v1.weight.sha256) and
        boundary.hasQualifiedIdentity(identity);
}

test "GLiNER fast FP32 identity includes all pinned sidecars and rejects quantization" {
    const base = boundary.fastino_gliner25_base_v1;
    try std.testing.expect(identityEligible(base, null));
    try std.testing.expect(identityEligible(boundary.fastino_gliner25_base_v1_transformers, null));
    try std.testing.expect(!identityEligible(null, null));
    var changed = base;
    changed.weight.sha256[0] ^= 1;
    try std.testing.expect(!identityEligible(changed, null));
    for (0..base.sidecars.len) |index| {
        changed = base;
        changed.sidecars[index].sha256[0] ^= 1;
        try std.testing.expect(!identityEligible(changed, null));
    }
    changed = base;
    changed.precision = .fp16_encoder;
    try std.testing.expect(!identityEligible(changed, null));
    try std.testing.expect(identityEligible(null, decide.pinned_legacy_fp32_identity));
    var decision = decide.pinned_legacy_fp32_identity;
    decision.sidecars.config.sha256[0] ^= 1;
    try std.testing.expect(!identityEligible(null, decision));
    decision = decide.pinned_legacy_fp32_identity;
    decision.inventory.all_f32 = false;
    try std.testing.expect(!identityEligible(null, decision));
}
