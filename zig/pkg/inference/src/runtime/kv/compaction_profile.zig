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

//! Frozen retention decisions and release admission. An identity must describe
//! the snapshots actually parsed/loaded, never a second pathname read. Registry
//! membership is separate from a numerical profile's structural validity.
const std = @import("std");
const policy = @import("../context_policy.zig");
const compaction_identity = @import("compaction_identity");

/// Runtime selection hashes only semantic inputs. Source-tree, executable and
/// release-report hashes remain external evidence and must not make a release
/// registry self-referential.
pub const semantic_identity_version: u32 = 1;
const layout_domain = "antfly.context-compaction.layout.v1\x00";
const implementation_domain = "antfly.context-compaction.implementation.v4\x00";
const profile_domain = "antfly.context-compaction.profile.v3\x00";
pub const FitEngine = enum(u8) { block_v1, native_whole_head_v2 };

pub const Identity = struct {
    /// Digests of the bytes parsed by the loaded model/tokenizer/template.
    artifact: [32]u8,
    tokenizer: [32]u8,
    template: [32]u8,
    /// Canonical semantic layout digest, produced by `layoutDigest`.
    layout: [32]u8,
    /// Canonical algorithm/KV ABI/kernel-route digest, produced by
    /// `implementationDigest`. This is deliberately not a binary hash.
    runtime: [32]u8,

    pub fn fromLoaded(loaded: LoadedBytes, layout: policy.Layout, implementation: Implementation) !Identity {
        return .{
            .artifact = loaded.artifact,
            .tokenizer = loaded.tokenizer,
            .template = loaded.template,
            .layout = try layoutDigest(layout),
            .runtime = implementationDigest(implementation),
        };
    }
};
pub const LoadedBytes = struct {
    artifact: [32]u8,
    tokenizer: [32]u8,
    template: [32]u8,
};
pub const Owner = struct { layer: u32, retained: []const u32 };
pub const QueryGenerationMode = enum(u8) { greedy_fixed_length_ignore_eos = 1 };
pub const QuerySampling = enum(u8) { source_uniform_partition_v3 = 1 };
pub const QueryAccounting = enum(u8) { serial_physical_branch_upper_bound_v2 = 1 };
pub const QuerySourceKind = enum(u8) { request_question_replay = 0, fixed_study_prefill = 1, fixed_study_generated_response = 2 };
pub const QueryPartition = enum(u8) { training = 0, validation = 1 };
pub const QuerySourceAuthority = struct {
    kind: QuerySourceKind,
    partition: QueryPartition,
    physical_branch: u8,
};
pub const QueryStratum = struct { consumer_layer: u32, query_head: u32, kv_head: u32 };
pub const QueryStudyAuthority = struct {
    template_sha256: [32]u8,
    generation_contract_sha256: [32]u8,
    response_tokens: u32,
    generation_mode: QueryGenerationMode,
};
pub const QueryProtocolAuthority = struct {
    schema_version: u32,
    layer: u32,
    /// Binary-domain v2 digest recomputed by context_query_protocol from this
    /// definition and the actual loaded layout. The digest excludes itself.
    sha256: [32]u8,
    query_sampling: QuerySampling,
    accounting: QueryAccounting,
    max_training_rows: u32,
    max_validation_rows: u32,
    sources: [5]QuerySourceAuthority,
    owner_kv_heads: u32,
    owner_query_heads: u32,
    owner_head_dim: u32,
    strata: []const QueryStratum,
    fit: QueryStudyAuthority,
    gate: QueryStudyAuthority,
};
pub const Profile = struct {
    identity: Identity,
    engine: FitEngine = .block_v1,
    /// SHA-256 of the exact frozen profile artifact reviewed by release.
    /// It is external provenance and deliberately excluded from semanticDigest.
    artifact_sha256: [32]u8 = @splat(0),
    owners: []const Owner,
    query_protocols: []const QueryProtocolAuthority = &.{},
    max_prompt_tokens: u32,
    max_output_tokens: u32,
    /// Required reduction in page-rounded generation bytes, not logical rows.
    minimum_saved_bytes: usize,

    /// Stable profile identity for release evidence and bounded telemetry. It
    /// excludes loaded artifact identity, which is matched independently.
    pub fn semanticDigest(self: Profile) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(profile_domain);
        hashInt(&hash, u32, semantic_identity_version);
        hashInt(&hash, u8, @backingInt(self.engine));
        hashInt(&hash, u32, self.max_prompt_tokens);
        hashInt(&hash, u32, self.max_output_tokens);
        hashInt(&hash, u64, @intCast(self.minimum_saved_bytes));
        hashInt(&hash, u32, @intCast(self.owners.len));
        for (self.owners) |owner| {
            hashInt(&hash, u32, owner.layer);
            hashInt(&hash, u32, @intCast(owner.retained.len));
            for (owner.retained) |rows| hashInt(&hash, u32, rows);
        }
        hashInt(&hash, u32, @intCast(self.query_protocols.len));
        for (self.query_protocols) |protocol| {
            hashInt(&hash, u32, protocol.schema_version);
            hashInt(&hash, u32, protocol.layer);
            hash.update(&protocol.sha256);
            hashInt(&hash, u8, @backingInt(protocol.query_sampling));
            hashInt(&hash, u8, @backingInt(protocol.accounting));
            hashInt(&hash, u32, protocol.max_training_rows);
            hashInt(&hash, u32, protocol.max_validation_rows);
            hashInt(&hash, u8, protocol.sources.len);
            for (protocol.sources, 0..) |source, source_index| {
                hashInt(&hash, u8, @intCast(source_index));
                hashInt(&hash, u8, @backingInt(source.kind));
                hashInt(&hash, u8, @backingInt(source.partition));
                hashInt(&hash, u8, source.physical_branch);
            }
            hashInt(&hash, u32, protocol.owner_kv_heads);
            hashInt(&hash, u32, protocol.owner_query_heads);
            hashInt(&hash, u32, protocol.owner_head_dim);
            hashInt(&hash, u32, @intCast(protocol.strata.len));
            for (protocol.strata) |stratum| {
                hashInt(&hash, u32, stratum.consumer_layer);
                hashInt(&hash, u32, stratum.query_head);
                hashInt(&hash, u32, stratum.kv_head);
            }
            inline for (.{ protocol.fit, protocol.gate }) |study| {
                hash.update(&study.template_sha256);
                hash.update(&study.generation_contract_sha256);
                hashInt(&hash, u32, study.response_tokens);
                hashInt(&hash, u8, @backingInt(study.generation_mode));
            }
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }

    pub fn validate(self: Profile, actual: Identity, layout: policy.Layout, prompt: usize, output: usize) !void {
        if (!std.meta.eql(self.identity, actual)) return error.CompactionIdentityMismatch;
        if (self.max_prompt_tokens == 0 or self.max_output_tokens == 0 or self.minimum_saved_bytes == 0 or
            prompt == 0 or prompt > self.max_prompt_tokens or output == 0 or output > self.max_output_tokens or layout.len == 0 or layout.len > 256)
            return error.UnsupportedCompactionProfile;
        const diagnostic_without_runtime_protocol = self.query_protocols.len == 0 and
            std.mem.allEqual(u8, &self.artifact_sha256, 0);
        if (!diagnostic_without_runtime_protocol and self.query_protocols.len != self.owners.len)
            return error.UnsupportedCompactionProfile;
        var index: usize = 0;
        var lossy = false;
        for (layout.layers[0..layout.len], 0..) |layer, i| {
            if (layer.owner != i or layer.sliding_window != 0) continue;
            if (index >= self.owners.len or self.owners[index].layer != i or self.owners[index].retained.len != layer.kv_heads)
                return error.UnsupportedCompactionProfile;
            if (!diagnostic_without_runtime_protocol) {
                const protocol = self.query_protocols[index];
                if (protocol.schema_version != 2 or protocol.layer != i or std.mem.allEqual(u8, &protocol.sha256, 0) or
                    protocol.strata.len == 0 or protocol.strata.len > 128)
                    return error.UnsupportedCompactionProfile;
                inline for (.{ protocol.fit, protocol.gate }) |study| {
                    if (study.response_tokens == 0 or
                        std.mem.allEqual(u8, &study.template_sha256, 0) or
                        std.mem.allEqual(u8, &study.generation_contract_sha256, 0))
                        return error.UnsupportedCompactionProfile;
                }
            }
            for (self.owners[index].retained) |rows| {
                if (rows == 0 or rows > 512) return error.UnsupportedCompactionProfile;
                lossy = lossy or rows < 512;
            }
            index += 1;
        }
        if (index != self.owners.len or !lossy) return error.UnsupportedCompactionProfile;
    }
};

/// Exact maximum retained rows for one KV head after applying this profile to
/// every validated eligible block. Prefixes, gaps and the final tail remain
/// exact; continuation rows are reserved exact as well.
pub fn retainedRows(profile: Profile, prompt_tokens: usize, continuation_tokens: usize, block_count: usize) !usize {
    if (profile.owners.len == 0 or block_count == 0) return error.UnsupportedCompactionProfile;
    const compacted_logical = try std.math.mul(usize, block_count, policy.block_tokens);
    if (compacted_logical > prompt_tokens) return error.InvalidPayloadProvenance;
    const logical = try std.math.add(usize, prompt_tokens, continuation_tokens);
    var maximum: usize = 0;
    for (profile.owners) |owner| {
        if (owner.retained.len == 0) return error.UnsupportedCompactionProfile;
        for (owner.retained) |rows| {
            const retained: usize = rows;
            if (retained == 0 or retained > policy.block_tokens) return error.UnsupportedCompactionProfile;
            const removed = try std.math.mul(usize, block_count, policy.block_tokens - retained);
            maximum = @max(maximum, try std.math.sub(usize, logical, removed));
        }
    }
    return maximum;
}

pub fn requireRetainedRows(profile: Profile, prompt_tokens: usize, continuation_tokens: usize, block_count: usize, retained_limit: usize) !usize {
    const rows = try retainedRows(profile, prompt_tokens, continuation_tokens, block_count);
    if (rows > retained_limit) return error.ContextKvBudgetExceeded;
    return rows;
}

fn hashInt(hash: *std.crypto.hash.sha2.Sha256, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hash.update(&bytes);
}

/// Canonicalize every field consumed by packed context storage and attention.
pub fn layoutDigest(layout: policy.Layout) ![32]u8 {
    if (layout.len == 0 or layout.len > layout.layers.len) return error.InvalidContextLayout;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(layout_domain);
    hashInt(&hash, u32, semantic_identity_version);
    hashInt(&hash, u32, @intCast(layout.len));
    for (layout.layers[0..layout.len]) |layer| {
        hashInt(&hash, u16, layer.owner);
        hashInt(&hash, u32, layer.sliding_window);
        hashInt(&hash, u32, layer.kv_heads);
        hashInt(&hash, u32, layer.query_heads);
        hashInt(&hash, u32, layer.head_dim);
        hashInt(&hash, u32, layer.rope_active_dim);
        hashInt(&hash, u32, layer.rope_frequency_dim);
        hashInt(&hash, u32, @bitCast(layer.rope_theta));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

pub const Implementation = struct {
    /// Build-generated digest of the canonical implementation sources.
    /// Registry membership is intentionally not part of this manifest.
    source_manifest: [32]u8 = current_source_manifest,
    optimize_mode: std.builtin.OptimizeMode = @import("builtin").mode,
    /// Actual compiler-derived module profile; never inferred from a receipt label.
    strip_debug_info: bool = @import("builtin").strip_debug_info,
    have_error_return_tracing: bool = @import("builtin").have_error_return_tracing,
    metal_enabled: bool = @import("build_options").enable_metal,
    metal_math_mode: enum(u8) { safe } = .safe,
    metal_runtime_abi_version: u32 = 1,
    algorithm_version: u32,
    packed_abi_version: u32,
    kv_dtype: enum(u8) { f16 },
    kernel_route: enum(u8) { metal_context_correctness, metal_context_optimized },
    /// Concrete kernel features are identity-bearing. The broad route name
    /// alone cannot distinguish split decode, flash prefill, or their pair.
    split_decode: bool = false,
    flash_prefill: bool = false,
    block_tokens: u32,
    solver: enum(u8) { bounded_coordinate_descent, rms_nnls_qr, omp_nnls_qr, residual_omp_active_nnls_fp64_qr },
    acceptance: enum(u8) { block_v1, rounded_whole_head_v2 } = .block_v1,
};

pub fn servingImplementation(engine: FitEngine, split_decode: bool, flash_prefill: bool) Implementation {
    const native = engine == .native_whole_head_v2;
    return .{
        .algorithm_version = if (native) 2 else 1,
        .packed_abi_version = 2,
        .kv_dtype = .f16,
        .kernel_route = if (split_decode or flash_prefill) .metal_context_optimized else .metal_context_correctness,
        .split_decode = split_decode,
        .flash_prefill = flash_prefill,
        .block_tokens = 512,
        .solver = if (native) .residual_omp_active_nnls_fp64_qr else .bounded_coordinate_descent,
        .acceptance = if (native) .rounded_whole_head_v2 else .block_v1,
    };
}

test "context native acceptance and solver are bound to implementation and profile identities" {
    const original = servingImplementation(.block_v1, true, true);
    const native = servingImplementation(.native_whole_head_v2, true, true);
    try std.testing.expect(!std.meta.eql(implementationDigest(original), implementationDigest(native)));
    var wrong_acceptance = native;
    wrong_acceptance.acceptance = .block_v1;
    try std.testing.expect(!std.meta.eql(implementationDigest(native), implementationDigest(wrong_acceptance)));
    const identity = Identity{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = implementationDigest(native) };
    const profile = Profile{ .identity = identity, .engine = .native_whole_head_v2, .owners = &.{}, .max_prompt_tokens = 8192, .max_output_tokens = 32, .minimum_saved_bytes = 1 };
    const profile_hex = std.fmt.bytesToHex(profile.semanticDigest(), .lower);
    try std.testing.expectEqualStrings("f513982df6299aa54157a75f483e82afff075684dfd39c4795f5cbc525612b85", &profile_hex);
    var wrong_engine = profile;
    wrong_engine.engine = .block_v1;
    const legacy_hex = std.fmt.bytesToHex(wrong_engine.semanticDigest(), .lower);
    try std.testing.expectEqualStrings("2c46e69233a79a1f400df9fdeeaff474b1a77903240d22df6830841067314540", &legacy_hex);
    try std.testing.expect(!std.meta.eql(profile.semanticDigest(), wrong_engine.semanticDigest()));
}

/// Cached host generation prevents target compilers from embedding and
/// repeatedly scanning the multi-megabyte backend source bundle.
pub const current_source_manifest = compaction_identity.digest;

pub fn implementationDigest(value: Implementation) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(implementation_domain);
    hashInt(&hash, u32, semantic_identity_version);
    hash.update(&value.source_manifest);
    hashInt(&hash, u8, @backingInt(value.optimize_mode));
    hashInt(&hash, u8, @intFromBool(value.strip_debug_info));
    hashInt(&hash, u8, @intFromBool(value.have_error_return_tracing));
    hashInt(&hash, u8, @intFromBool(value.metal_enabled));
    hashInt(&hash, u8, @backingInt(value.metal_math_mode));
    hashInt(&hash, u32, value.metal_runtime_abi_version);
    hashInt(&hash, u32, value.algorithm_version);
    hashInt(&hash, u32, value.packed_abi_version);
    hashInt(&hash, u8, @backingInt(value.kv_dtype));
    hashInt(&hash, u8, @backingInt(value.kernel_route));
    hashInt(&hash, u8, @intFromBool(value.split_decode));
    hashInt(&hash, u8, @intFromBool(value.flash_prefill));
    hashInt(&hash, u32, value.block_tokens);
    hashInt(&hash, u8, @backingInt(value.acceptance));
    hashInt(&hash, u8, @backingInt(value.solver));
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

// BEGIN RELEASE REGISTRY CONTENTS
// Intentionally empty. Adding an entry requires the artifact-bound release
// report, serving/quality/failure gates, and destination soak/performance proof.
// No environment variable, request field, or on-disk profile grants authority.
const release_profiles: []const Profile = &.{};
// END RELEASE REGISTRY CONTENTS
pub fn profileArtifactDigest(profile: Profile) ![32]u8 {
    if (std.mem.allEqual(u8, &profile.artifact_sha256, 0)) return error.UnqualifiedCompactionArtifact;
    return profile.artifact_sha256;
}

pub fn select(identity: Identity) !*const Profile {
    for (release_profiles) |*profile| if (std.meta.eql(profile.identity, identity)) {
        _ = try profileArtifactDigest(profile.*);
        return profile;
    };
    return error.UnqualifiedCompactionArtifact;
}

fn testQueryProtocol(layer: u32) QueryProtocolAuthority {
    return .{
        .schema_version = 2,
        .layer = layer,
        .sha256 = @splat(9),
        .query_sampling = .source_uniform_partition_v3,
        .accounting = .serial_physical_branch_upper_bound_v2,
        .max_training_rows = 3072,
        .max_validation_rows = 1024,
        .sources = .{
            .{ .kind = .request_question_replay, .partition = .training, .physical_branch = 0 },
            .{ .kind = .fixed_study_prefill, .partition = .training, .physical_branch = 1 },
            .{ .kind = .fixed_study_generated_response, .partition = .training, .physical_branch = 1 },
            .{ .kind = .fixed_study_prefill, .partition = .validation, .physical_branch = 2 },
            .{ .kind = .fixed_study_generated_response, .partition = .validation, .physical_branch = 2 },
        },
        .owner_kv_heads = 2,
        .owner_query_heads = 4,
        .owner_head_dim = 512,
        .strata = &.{
            .{ .consumer_layer = 0, .query_head = 0, .kv_head = 0 },
            .{ .consumer_layer = 0, .query_head = 1, .kv_head = 0 },
            .{ .consumer_layer = 0, .query_head = 2, .kv_head = 1 },
            .{ .consumer_layer = 0, .query_head = 3, .kv_head = 1 },
        },
        .fit = .{ .template_sha256 = @splat(1), .generation_contract_sha256 = @splat(2), .response_tokens = 128, .generation_mode = .greedy_fixed_length_ignore_eos },
        .gate = .{ .template_sha256 = @splat(3), .generation_contract_sha256 = @splat(4), .response_tokens = 64, .generation_mode = .greedy_fixed_length_ignore_eos },
    };
}

test "context compaction profiles reject identity drift and unqualified artifacts" {
    const a: Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    var layout = policy.Layout{ .len = 1 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 512, .rope_active_dim = 256, .rope_frequency_dim = 256, .rope_theta = 10000 };
    const p = Profile{ .identity = a, .owners = &.{.{ .layer = 0, .retained = &.{ 256, 512 } }}, .query_protocols = &.{testQueryProtocol(0)}, .max_prompt_tokens = 8192, .max_output_tokens = 512, .minimum_saved_bytes = 16384 };
    try p.validate(a, layout, 4096, 512);
    inline for (.{ "artifact", "tokenizer", "template", "layout", "runtime" }) |field| {
        var changed = a;
        @field(changed, field)[0] ^= 1;
        try std.testing.expectError(error.CompactionIdentityMismatch, p.validate(changed, layout, 4096, 512));
    }
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, select(a));
    try std.testing.expectError(error.UnsupportedCompactionProfile, p.validate(a, layout, 8193, 512));
    layout.layers[0].kv_heads = 1;
    try std.testing.expectError(error.UnsupportedCompactionProfile, p.validate(a, layout, 4096, 512));
}

test "context release profile binds exact artifact digest separately from semantics" {
    const identity: Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    const base = Profile{ .identity = identity, .owners = &.{.{ .layer = 0, .retained = &.{256} }}, .query_protocols = &.{testQueryProtocol(0)}, .max_prompt_tokens = 1024, .max_output_tokens = 8, .minimum_saved_bytes = 4096 };
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, profileArtifactDigest(base));
    var bound = base;
    bound.artifact_sha256 = @splat(9);
    try std.testing.expectEqual(@as([32]u8, @splat(9)), try profileArtifactDigest(bound));
    try std.testing.expectEqual(base.semanticDigest(), bound.semanticDigest());
}

test "profile-aware retained rows reject optimistic generic admission" {
    const identity: Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4), .runtime = @splat(5) };
    const profile = Profile{
        .identity = identity,
        .owners = &.{
            .{ .layer = 4, .retained = &.{ 288, 352 } },
            .{ .layer = 9, .retained = &.{288} },
            .{ .layer = 14, .retained = &.{288} },
        },
        .max_prompt_tokens = 8160,
        .max_output_tokens = 32,
        .minimum_saved_bytes = 1,
    };
    try std.testing.expectEqual(@as(usize, 6240), try retainedRows(profile, 8160, 0, 12));
    try std.testing.expectEqual(@as(usize, 6272), try retainedRows(profile, 8160, 32, 12));
    try std.testing.expectError(error.ContextKvBudgetExceeded, requireRetainedRows(profile, 8160, 32, 12, 6144));
    try std.testing.expectEqual(@as(usize, 6144), try requireRetainedRows(profile, 8032, 32, 12, 6144));
    try std.testing.expectError(error.ContextKvBudgetExceeded, requireRetainedRows(profile, 8033, 32, 12, 6144));
    try std.testing.expectEqual(@as(usize, 6112), try requireRetainedRows(profile, 8160, 32, 13, 6144));
    try std.testing.expectError(error.InvalidPayloadProvenance, retainedRows(profile, 511, 1, 1));
}

test "context compaction compiler profile observation" {
    const builtin = @import("builtin");
    std.debug.print("\ncontext_compiler_profile: {{\"schema_version\":1,\"strip_debug_info\":{},\"have_error_return_tracing\":{}}}\n", .{
        builtin.strip_debug_info,
        builtin.have_error_return_tracing,
    });
}

test "context compaction semantic digests are canonical and domain separated" {
    var layout = policy.Layout{ .len = 1 };
    layout.layers[0] = .{ .owner = 0, .sliding_window = 0, .kv_heads = 2, .query_heads = 4, .head_dim = 512, .rope_active_dim = 256, .rope_frequency_dim = 256, .rope_theta = 10000 };
    const first = try layoutDigest(layout);
    try std.testing.expectEqual(first, try layoutDigest(layout));
    layout.layers[0].rope_theta = 10001;
    const changed_layout = try layoutDigest(layout);
    try std.testing.expect(!std.mem.eql(u8, &first, &changed_layout));

    const implementation_fields = Implementation{ .algorithm_version = 1, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_correctness, .block_tokens = 512, .solver = .bounded_coordinate_descent };
    const implementation = implementationDigest(implementation_fields);
    const optimized = implementationDigest(.{ .algorithm_version = 1, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_optimized, .block_tokens = 512, .solver = .bounded_coordinate_descent });
    try std.testing.expect(!std.mem.eql(u8, &implementation, &optimized));
    const split = implementationDigest(.{ .algorithm_version = 1, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_optimized, .split_decode = true, .block_tokens = 512, .solver = .bounded_coordinate_descent });
    const flash = implementationDigest(.{ .algorithm_version = 1, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_optimized, .flash_prefill = true, .block_tokens = 512, .solver = .bounded_coordinate_descent });
    const combined = implementationDigest(.{ .algorithm_version = 1, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_optimized, .split_decode = true, .flash_prefill = true, .block_tokens = 512, .solver = .bounded_coordinate_descent });
    try std.testing.expect(!std.mem.eql(u8, &optimized, &split));
    try std.testing.expect(!std.mem.eql(u8, &split, &flash));
    try std.testing.expect(!std.mem.eql(u8, &split, &combined));
    var changed_source = implementation_fields;
    changed_source.source_manifest[0] ^= 1;
    const source_changed = implementationDigest(changed_source);
    try std.testing.expect(!std.mem.eql(u8, &implementation, &source_changed));
    try std.testing.expectEqual(current_source_manifest, implementation_fields.source_manifest);
    var changed_build = implementation_fields;
    changed_build.optimize_mode = if (implementation_fields.optimize_mode == .debug) .fast else .debug;
    const build_changed = implementationDigest(changed_build);
    try std.testing.expect(!std.mem.eql(u8, &implementation, &build_changed));
    try std.testing.expectEqual(@import("builtin").strip_debug_info, implementation_fields.strip_debug_info);
    try std.testing.expectEqual(@import("builtin").have_error_return_tracing, implementation_fields.have_error_return_tracing);
    var changed_strip = implementation_fields;
    changed_strip.strip_debug_info = !implementation_fields.strip_debug_info;
    try std.testing.expect(!std.mem.eql(u8, &implementation, &implementationDigest(changed_strip)));
    var changed_tracing = implementation_fields;
    changed_tracing.have_error_return_tracing = !implementation_fields.have_error_return_tracing;
    try std.testing.expect(!std.mem.eql(u8, &implementation, &implementationDigest(changed_tracing)));

    const identity: Identity = .{ .artifact = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = first, .runtime = implementation };
    const profile = Profile{ .identity = identity, .artifact_sha256 = @splat(9), .owners = &.{.{ .layer = 0, .retained = &.{ 256, 512 } }}, .query_protocols = &.{testQueryProtocol(0)}, .max_prompt_tokens = 8192, .max_output_tokens = 512, .minimum_saved_bytes = 16384 };
    const profile_digest = profile.semanticDigest();
    var changed = profile;
    changed.minimum_saved_bytes += 1;
    const changed_profile_digest = changed.semanticDigest();
    try std.testing.expect(!std.mem.eql(u8, &profile_digest, &changed_profile_digest));
    var protocol = testQueryProtocol(0);
    protocol.fit.response_tokens += 1;
    changed = profile;
    changed.query_protocols = &.{protocol};
    try std.testing.expect(!std.mem.eql(u8, &profile_digest, &changed.semanticDigest()));
    changed = profile;
    changed.query_protocols = &.{};
    try std.testing.expectError(error.UnsupportedCompactionProfile, changed.validate(identity, layout, 4096, 512));
}
