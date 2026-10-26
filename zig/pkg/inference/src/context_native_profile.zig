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

//! Strict, test-binary-only ingress for a frozen native-v2 qualification
//! candidate.  This artifact is deliberately distinct from the legacy frozen
//! numerical profile and from the production release registry.
const std = @import("std");
const profiles = @import("runtime/kv/compaction_profile.zig");
const policy = @import("runtime/context_policy.zig");
const query_protocol = @import("backends/context_query_protocol.zig");

pub const schema = "antfly.context.native.qualification-profile.v1";
pub const stage = "native_v2_candidate_qualification";

pub const RuntimeStudyAuthority = struct {
    template_sha256: []const u8,
    generation_contract_sha256: []const u8,
    response_tokens: u32,
    generation_mode: enum { greedy_fixed_length_ignore_eos },
};

pub const RuntimeSourceAuthority = struct {
    kind: enum { request_question_replay, fixed_study_prefill, fixed_study_generated_response },
    partition: enum { training, validation },
    physical_branch: u8,
};

pub const RuntimeStratum = struct { consumer_layer: u32, query_head: u32, kv_head: u32 };

pub const RuntimeQueryProtocol = struct {
    schema_version: u32,
    layer: u32,
    sha256: []const u8,
    query_sampling: enum { source_uniform_partition_v3 },
    accounting: enum { serial_physical_branch_upper_bound_v2 },
    max_training_rows: u32,
    max_validation_rows: u32,
    sources: []const RuntimeSourceAuthority,
    owner: struct { kv_heads: u32, query_heads: u32, head_dim: u32 },
    strata: []const RuntimeStratum,
    fit: RuntimeStudyAuthority,
    gate: RuntimeStudyAuthority,
};

pub const Implementation = struct {
    engine: enum { native_whole_head_v2 },
    algorithm_version: u32,
    packed_abi_version: u32,
    kv_dtype: enum { f16 },
    kernel_route: enum { metal_context_optimized },
    split_decode: bool,
    flash_prefill: bool,
    block_tokens: u32,
    solver: enum { residual_omp_active_nnls_fp64_qr },
    acceptance: enum { rounded_whole_head_v2 },
};

pub const Artifact = struct {
    schema: []const u8,
    stage: []const u8,
    diagnostic_only: bool,
    candidate_qualification: bool,
    serving_qualified: bool,
    release_authority: bool,
    training_role: []const u8,
    evaluation_opened: bool,
    frozen_before_unseen_evaluation: bool,
    identity: struct {
        artifact_sha256: []const u8,
        tokenizer_sha256: []const u8,
        template_sha256: []const u8,
        layout_sha256: []const u8,
        implementation_sha256: []const u8,
    },
    implementation: Implementation,
    allocation_page_bytes: usize,
    max_prompt_tokens: u32,
    max_output_tokens: u32,
    minimum_saved_bytes: usize,
    profile_semantic_sha256: []const u8,
    owners: []const struct { layer: u32, retained: []const u32 },
    query_protocols: []const RuntimeQueryProtocol,
    evidence: struct {
        campaign_spec_sha256: []const u8,
        repeat_results_sha256: []const u8,
        training_authority_sha256: []const u8,
        target_bootstrap_sha256: []const u8,
        target_bootstrap_resource_sha256: []const u8,
        source_build_sha256: []const u8,
        binary_sha256: []const u8,
        freezer_sha256: []const u8,
    },
};

fn digest32(text: []const u8) ![32]u8 {
    if (text.len != 64) return error.InvalidNativeQualificationProfile;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, text) catch return error.InvalidNativeQualificationProfile;
    // Canonical artifacts use lowercase hexadecimal only.
    const canonical = std.fmt.bytesToHex(result, .lower);
    if (!std.mem.eql(u8, text, &canonical)) return error.InvalidNativeQualificationProfile;
    return result;
}

fn study(source: RuntimeStudyAuthority) !profiles.QueryStudyAuthority {
    return .{
        .template_sha256 = try digest32(source.template_sha256),
        .generation_contract_sha256 = try digest32(source.generation_contract_sha256),
        .response_tokens = source.response_tokens,
        .generation_mode = .greedy_fixed_length_ignore_eos,
    };
}

pub const Loaded = struct {
    allocator: std.mem.Allocator,
    parsed: std.json.Parsed(Artifact),
    owners: []profiles.Owner,
    query_protocols: []profiles.QueryProtocolAuthority,
    profile: profiles.Profile,

    pub fn deinit(self: *Loaded) void {
        for (self.query_protocols) |protocol| self.allocator.free(protocol.strata);
        self.allocator.free(self.query_protocols);
        self.allocator.free(self.owners);
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub fn parseBytes(allocator: std.mem.Allocator, bytes: []const u8) !Loaded {
    var artifact_sha256: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &artifact_sha256, .{});
    var parsed = try std.json.parseFromSlice(Artifact, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    });
    errdefer parsed.deinit();
    const value = parsed.value;
    if (!std.mem.eql(u8, value.schema, schema) or
        !std.mem.eql(u8, value.stage, stage) or
        !value.diagnostic_only or !value.candidate_qualification or
        value.serving_qualified or value.release_authority or
        !std.mem.eql(u8, value.training_role, "internal_development_calibration") or
        value.evaluation_opened or !value.frozen_before_unseen_evaluation or
        value.allocation_page_bytes != std.heap.pageSize() or
        value.minimum_saved_bytes == 0 or value.minimum_saved_bytes % value.allocation_page_bytes != 0 or
        value.max_prompt_tokens == 0 or value.max_output_tokens == 0 or
        (std.math.add(u32, value.max_prompt_tokens, value.max_output_tokens) catch return error.InvalidNativeQualificationProfile) > 131072 or
        value.owners.len == 0 or
        value.query_protocols.len != value.owners.len)
        return error.InvalidNativeQualificationProfile;
    const implementation = value.implementation;
    if (implementation.algorithm_version != 2 or implementation.packed_abi_version != 2 or
        !implementation.split_decode or !implementation.flash_prefill or implementation.block_tokens != 512)
        return error.InvalidNativeQualificationProfile;
    const native_implementation = profiles.servingImplementation(.native_whole_head_v2, true, true);
    const implementation_sha256 = profiles.implementationDigest(native_implementation);
    const expected_implementation_sha256 = try digest32(value.identity.implementation_sha256);
    if (!std.mem.eql(u8, &implementation_sha256, &expected_implementation_sha256))
        return error.InvalidNativeQualificationProfile;
    inline for (.{
        value.evidence.campaign_spec_sha256,
        value.evidence.repeat_results_sha256,
        value.evidence.training_authority_sha256,
        value.evidence.target_bootstrap_sha256,
        value.evidence.target_bootstrap_resource_sha256,
        value.evidence.source_build_sha256,
        value.evidence.binary_sha256,
        value.evidence.freezer_sha256,
    }) |digest| {
        const decoded = try digest32(digest);
        if (std.mem.allEqual(u8, &decoded, 0)) return error.InvalidNativeQualificationProfile;
    }

    const owners = try allocator.alloc(profiles.Owner, value.owners.len);
    errdefer allocator.free(owners);
    const query_protocols = try allocator.alloc(profiles.QueryProtocolAuthority, value.query_protocols.len);
    var initialized: usize = 0;
    errdefer {
        for (query_protocols[0..initialized]) |protocol| allocator.free(protocol.strata);
        allocator.free(query_protocols);
    }
    var previous_layer: ?u32 = null;
    for (value.owners, value.query_protocols, owners, query_protocols) |source, protocol, *destination, *bound| {
        if (source.retained.len == 0 or (previous_layer != null and source.layer <= previous_layer.?) or
            protocol.layer != source.layer or protocol.schema_version != 2 or protocol.sources.len != 5 or
            protocol.max_training_rows != query_protocol.max_training_rows or
            protocol.max_validation_rows != query_protocol.max_validation_rows or
            protocol.strata.len == 0 or protocol.strata.len > 128 or
            protocol.owner.kv_heads != source.retained.len)
            return error.InvalidNativeQualificationProfile;
        for (source.retained) |rows| if (rows == 0 or rows > 512)
            return error.InvalidNativeQualificationProfile;
        var sources: [5]profiles.QuerySourceAuthority = undefined;
        const expected_sources = [5]RuntimeSourceAuthority{
            .{ .kind = .request_question_replay, .partition = .training, .physical_branch = 0 },
            .{ .kind = .fixed_study_prefill, .partition = .training, .physical_branch = 1 },
            .{ .kind = .fixed_study_generated_response, .partition = .training, .physical_branch = 1 },
            .{ .kind = .fixed_study_prefill, .partition = .validation, .physical_branch = 2 },
            .{ .kind = .fixed_study_generated_response, .partition = .validation, .physical_branch = 2 },
        };
        for (protocol.sources, expected_sources, &sources) |runtime_source, expected_source, *profile_source| {
            if (!std.meta.eql(runtime_source, expected_source)) return error.InvalidNativeQualificationProfile;
            profile_source.* = .{
                .kind = switch (runtime_source.kind) {
                    .request_question_replay => .request_question_replay,
                    .fixed_study_prefill => .fixed_study_prefill,
                    .fixed_study_generated_response => .fixed_study_generated_response,
                },
                .partition = switch (runtime_source.partition) {
                    .training => .training,
                    .validation => .validation,
                },
                .physical_branch = runtime_source.physical_branch,
            };
        }
        const strata = try allocator.alloc(profiles.QueryStratum, protocol.strata.len);
        errdefer allocator.free(strata);
        for (protocol.strata, strata) |runtime_stratum, *profile_stratum| profile_stratum.* = .{
            .consumer_layer = runtime_stratum.consumer_layer,
            .query_head = runtime_stratum.query_head,
            .kv_head = runtime_stratum.kv_head,
        };
        bound.* = .{
            .schema_version = protocol.schema_version,
            .layer = protocol.layer,
            .sha256 = try digest32(protocol.sha256),
            .query_sampling = .source_uniform_partition_v3,
            .accounting = .serial_physical_branch_upper_bound_v2,
            .max_training_rows = protocol.max_training_rows,
            .max_validation_rows = protocol.max_validation_rows,
            .sources = sources,
            .owner_kv_heads = protocol.owner.kv_heads,
            .owner_query_heads = protocol.owner.query_heads,
            .owner_head_dim = protocol.owner.head_dim,
            .strata = strata,
            .fit = try study(protocol.fit),
            .gate = try study(protocol.gate),
        };
        const protocol_digest = try query_protocol.runtimeIdentity(bound.*);
        if (!std.mem.eql(u8, &bound.sha256, &protocol_digest))
            return error.InvalidNativeQualificationProfile;
        initialized += 1;
        destination.* = .{ .layer = source.layer, .retained = source.retained };
        previous_layer = source.layer;
    }
    const profile = profiles.Profile{
        .identity = .{
            .artifact = try digest32(value.identity.artifact_sha256),
            .tokenizer = try digest32(value.identity.tokenizer_sha256),
            .template = try digest32(value.identity.template_sha256),
            .layout = try digest32(value.identity.layout_sha256),
            .runtime = try digest32(value.identity.implementation_sha256),
        },
        .engine = .native_whole_head_v2,
        .artifact_sha256 = artifact_sha256,
        .owners = owners,
        .query_protocols = query_protocols,
        .max_prompt_tokens = value.max_prompt_tokens,
        .max_output_tokens = value.max_output_tokens,
        .minimum_saved_bytes = value.minimum_saved_bytes,
    };
    const semantic_digest = profile.semanticDigest();
    const expected_semantic_digest = try digest32(value.profile_semantic_sha256);
    if (!std.mem.eql(u8, &semantic_digest, &expected_semantic_digest))
        return error.InvalidNativeQualificationProfile;
    return .{ .allocator = allocator, .parsed = parsed, .owners = owners, .query_protocols = query_protocols, .profile = profile };
}

pub fn load(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Loaded {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(bytes);
    return parseBytes(allocator, bytes);
}

pub const bootstrap_schema = "antfly.context.native.qualification-bootstrap.v1";

/// Emit the target binary's native-v2 identity and compiled five-source query
/// authorities without executing a model forward. This descriptor grants
/// neither serving nor release authority.
pub fn writeBootstrap(allocator: std.mem.Allocator, io: std.Io, path: []const u8, load_generation: u64, identity: profiles.Identity, layout: policy.Layout, authorities: []const profiles.QueryProtocolAuthority) !void {
    if (load_generation == 0 or authorities.len != 4) return error.InvalidNativeQualificationBootstrap;
    const expected_layers = [4]u32{ 5, 11, 17, 23 };
    const native_implementation = profiles.servingImplementation(.native_whole_head_v2, true, true);
    const native_implementation_sha256 = profiles.implementationDigest(native_implementation);
    if (!std.mem.eql(u8, &identity.runtime, &native_implementation_sha256))
        return error.InvalidNativeQualificationBootstrap;
    const Owner = struct { layer: u32, kv_heads: u32, query_heads: u32, head_dim: u32 };
    var owners: [4]Owner = undefined;
    var protocols: [4]RuntimeQueryProtocol = undefined;
    var sources: [4][5]RuntimeSourceAuthority = undefined;
    var strata: [4][]RuntimeStratum = undefined;
    var strata_initialized: usize = 0;
    defer for (strata[0..strata_initialized]) |rows| allocator.free(rows);
    var protocol_sha: [4][64]u8 = undefined;
    var fit_template_sha: [4][64]u8 = undefined;
    var fit_contract_sha: [4][64]u8 = undefined;
    var gate_template_sha: [4][64]u8 = undefined;
    var gate_contract_sha: [4][64]u8 = undefined;
    for (authorities, expected_layers, 0..) |authority, expected_layer, index| {
        if (authority.layer != expected_layer or @as(usize, expected_layer) >= layout.len)
            return error.InvalidNativeQualificationBootstrap;
        const layer = layout.layers[@intCast(expected_layer)];
        if (layer.owner != expected_layer or layer.sliding_window != 0 or
            layer.kv_heads != 2 or layer.query_heads != 8 or layer.head_dim != 512 or
            authority.owner_kv_heads != 2 or authority.owner_query_heads != 8 or authority.owner_head_dim != 512)
            return error.InvalidNativeQualificationBootstrap;
        try query_protocol.validateRuntimeAuthority(authority, layout);
        owners[index] = .{ .layer = expected_layer, .kv_heads = 2, .query_heads = 8, .head_dim = 512 };
        for (authority.sources, &sources[index]) |source, *out| out.* = .{
            .kind = switch (source.kind) {
                .request_question_replay => .request_question_replay,
                .fixed_study_prefill => .fixed_study_prefill,
                .fixed_study_generated_response => .fixed_study_generated_response,
            },
            .partition = switch (source.partition) {
                .training => .training,
                .validation => .validation,
            },
            .physical_branch = source.physical_branch,
        };
        strata[index] = try allocator.alloc(RuntimeStratum, authority.strata.len);
        strata_initialized += 1;
        for (authority.strata, strata[index]) |source, *out| out.* = .{ .consumer_layer = source.consumer_layer, .query_head = source.query_head, .kv_head = source.kv_head };
        protocol_sha[index] = std.fmt.bytesToHex(authority.sha256, .lower);
        fit_template_sha[index] = std.fmt.bytesToHex(authority.fit.template_sha256, .lower);
        fit_contract_sha[index] = std.fmt.bytesToHex(authority.fit.generation_contract_sha256, .lower);
        gate_template_sha[index] = std.fmt.bytesToHex(authority.gate.template_sha256, .lower);
        gate_contract_sha[index] = std.fmt.bytesToHex(authority.gate.generation_contract_sha256, .lower);
        protocols[index] = .{
            .schema_version = authority.schema_version,
            .layer = authority.layer,
            .sha256 = &protocol_sha[index],
            .query_sampling = .source_uniform_partition_v3,
            .accounting = .serial_physical_branch_upper_bound_v2,
            .max_training_rows = authority.max_training_rows,
            .max_validation_rows = authority.max_validation_rows,
            .sources = &sources[index],
            .owner = .{ .kv_heads = 2, .query_heads = 8, .head_dim = 512 },
            .strata = strata[index],
            .fit = .{ .template_sha256 = &fit_template_sha[index], .generation_contract_sha256 = &fit_contract_sha[index], .response_tokens = authority.fit.response_tokens, .generation_mode = .greedy_fixed_length_ignore_eos },
            .gate = .{ .template_sha256 = &gate_template_sha[index], .generation_contract_sha256 = &gate_contract_sha[index], .response_tokens = authority.gate.response_tokens, .generation_mode = .greedy_fixed_length_ignore_eos },
        };
    }
    const artifact = std.fmt.bytesToHex(identity.artifact, .lower);
    const tokenizer = std.fmt.bytesToHex(identity.tokenizer, .lower);
    const template = std.fmt.bytesToHex(identity.template, .lower);
    const layout_sha = std.fmt.bytesToHex(identity.layout, .lower);
    const implementation_sha = std.fmt.bytesToHex(identity.runtime, .lower);
    const encoded = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = bootstrap_schema,
        .purpose = "native_v2_candidate_qualification",
        .diagnostic_only = true,
        .candidate_qualification = true,
        .serving_qualified = false,
        .release_authority = false,
        .model_forward_steps = @as(u32, 0),
        .loaded_identity = .{ .load_generation = load_generation, .artifact_sha256 = artifact[0..], .tokenizer_sha256 = tokenizer[0..], .template_sha256 = template[0..], .layout_sha256 = layout_sha[0..], .implementation_sha256 = implementation_sha[0..] },
        .implementation = Implementation{ .engine = .native_whole_head_v2, .algorithm_version = 2, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_optimized, .split_decode = true, .flash_prefill = true, .block_tokens = 512, .solver = .residual_omp_active_nnls_fp64_qr, .acceptance = .rounded_whole_head_v2 },
        .allocation_page_bytes = std.heap.pageSize(),
        .owners = &owners,
        .query_protocols = &protocols,
    }, .{});
    defer allocator.free(encoded);
    const file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true });
    defer file.close(io);
    try file.writeStreamingAll(io, encoded);
    try file.writeStreamingAll(io, "\n");
    try file.sync(io);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(encoded);
    hash.update("\n");
    const artifact_sha256 = hash.finalResult();
    const artifact_hex = std.fmt.bytesToHex(artifact_sha256, .lower);
    std.debug.print("context_native_qualification_bootstrap_artifact: sha256={s}\n", .{artifact_hex[0..]});
}

fn testHashInt(hash: *std.crypto.hash.sha2.Sha256, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hash.update(&bytes);
}

/// Test-only raw identity writer. Unlike runtimeIdentity it deliberately does
/// not validate canonical fields, allowing a mutation test to prove that a
/// correctly rehashed but noncanonical protocol is still rejected at ingress.
fn testRawProtocolIdentity(authority: profiles.QueryProtocolAuthority) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.context-query-runtime-protocol.v2\x00");
    testHashInt(&hash, u32, authority.schema_version);
    hash.update("source_uniform_partition_v3\x00");
    hash.update("serial_physical_branch_upper_bound_v2\x00");
    testHashInt(&hash, u32, authority.max_training_rows);
    testHashInt(&hash, u32, authority.max_validation_rows);
    testHashInt(&hash, u8, authority.sources.len);
    for (authority.sources, 0..) |source, index| {
        testHashInt(&hash, u8, @intCast(index));
        testHashInt(&hash, u8, @backingInt(source.kind));
        testHashInt(&hash, u8, @backingInt(source.partition));
        testHashInt(&hash, u8, source.physical_branch);
    }
    testHashInt(&hash, u32, authority.layer);
    testHashInt(&hash, u32, authority.owner_kv_heads);
    testHashInt(&hash, u32, authority.owner_query_heads);
    testHashInt(&hash, u32, authority.owner_head_dim);
    for (authority.strata) |stratum| {
        testHashInt(&hash, u32, stratum.consumer_layer);
        testHashInt(&hash, u32, stratum.query_head);
        testHashInt(&hash, u32, stratum.kv_head);
    }
    testHashInt(&hash, u32, @intCast(authority.strata.len));
    inline for (.{ authority.fit, authority.gate }) |study_authority| {
        hash.update(&study_authority.template_sha256);
        hash.update(&study_authority.generation_contract_sha256);
        testHashInt(&hash, u32, study_authority.response_tokens);
        testHashInt(&hash, u8, @backingInt(study_authority.generation_mode));
    }
    return hash.finalResult();
}

const TestArtifactOptions = struct {
    max_prompt_tokens: u32 = 8192,
    max_output_tokens: u32 = 32,
    max_training_rows: u32 = 3072,
    max_validation_rows: u32 = 1024,
    source_mutation: enum { none, order, partition, physical_branch } = .none,
    zero_evidence: bool = false,
};

fn testArtifact(allocator: std.mem.Allocator, options: TestArtifactOptions) ![]u8 {
    const implementation = profiles.servingImplementation(.native_whole_head_v2, true, true);
    const runtime = profiles.implementationDigest(implementation);
    var sources = [5]profiles.QuerySourceAuthority{
        .{ .kind = .request_question_replay, .partition = .training, .physical_branch = 0 },
        .{ .kind = .fixed_study_prefill, .partition = .training, .physical_branch = 1 },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .physical_branch = 1 },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .physical_branch = 2 },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .physical_branch = 2 },
    };
    switch (options.source_mutation) {
        .none => {},
        .order => std.mem.swap(profiles.QuerySourceAuthority, &sources[0], &sources[1]),
        .partition => sources[0].partition = .validation,
        .physical_branch => sources[0].physical_branch = 2,
    }
    const strata = [_]profiles.QueryStratum{.{ .consumer_layer = 0, .query_head = 0, .kv_head = 0 }};
    const study_authority = profiles.QueryStudyAuthority{ .template_sha256 = @splat(1), .generation_contract_sha256 = @splat(2), .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos };
    var authority = profiles.QueryProtocolAuthority{
        .schema_version = 2,
        .layer = 0,
        .sha256 = @splat(0),
        .query_sampling = .source_uniform_partition_v3,
        .accounting = .serial_physical_branch_upper_bound_v2,
        .max_training_rows = options.max_training_rows,
        .max_validation_rows = options.max_validation_rows,
        .sources = sources,
        .owner_kv_heads = 1,
        .owner_query_heads = 1,
        .owner_head_dim = 512,
        .strata = &strata,
        .fit = study_authority,
        .gate = study_authority,
    };
    authority.sha256 = testRawProtocolIdentity(authority);
    const identity = profiles.Identity{ .artifact = @splat(3), .tokenizer = @splat(4), .template = @splat(5), .layout = @splat(6), .runtime = runtime };
    const retained = [_]u32{256};
    const owner_values = [_]profiles.Owner{.{ .layer = 0, .retained = &retained }};
    const protocol_values = [_]profiles.QueryProtocolAuthority{authority};
    const profile = profiles.Profile{ .identity = identity, .engine = .native_whole_head_v2, .artifact_sha256 = @splat(7), .owners = &owner_values, .query_protocols = &protocol_values, .max_prompt_tokens = options.max_prompt_tokens, .max_output_tokens = options.max_output_tokens, .minimum_saved_bytes = std.heap.pageSize() };
    const artifact_hex = std.fmt.bytesToHex(identity.artifact, .lower);
    const tokenizer_hex = std.fmt.bytesToHex(identity.tokenizer, .lower);
    const template_hex = std.fmt.bytesToHex(identity.template, .lower);
    const layout_hex = std.fmt.bytesToHex(identity.layout, .lower);
    const runtime_hex = std.fmt.bytesToHex(identity.runtime, .lower);
    const semantic_hex = std.fmt.bytesToHex(profile.semanticDigest(), .lower);
    const protocol_hex = std.fmt.bytesToHex(authority.sha256, .lower);
    const template_study_hex = std.fmt.bytesToHex(study_authority.template_sha256, .lower);
    const contract_study_hex = std.fmt.bytesToHex(study_authority.generation_contract_sha256, .lower);
    var runtime_sources: [5]RuntimeSourceAuthority = undefined;
    for (sources, &runtime_sources) |source, *out| out.* = .{
        .kind = switch (source.kind) {
            .request_question_replay => .request_question_replay,
            .fixed_study_prefill => .fixed_study_prefill,
            .fixed_study_generated_response => .fixed_study_generated_response,
        },
        .partition = switch (source.partition) {
            .training => .training,
            .validation => .validation,
        },
        .physical_branch = source.physical_branch,
    };
    const runtime_strata = [_]RuntimeStratum{.{ .consumer_layer = 0, .query_head = 0, .kv_head = 0 }};
    const runtime_protocols = [_]RuntimeQueryProtocol{.{
        .schema_version = 2,
        .layer = 0,
        .sha256 = &protocol_hex,
        .query_sampling = .source_uniform_partition_v3,
        .accounting = .serial_physical_branch_upper_bound_v2,
        .max_training_rows = options.max_training_rows,
        .max_validation_rows = options.max_validation_rows,
        .sources = &runtime_sources,
        .owner = .{ .kv_heads = 1, .query_heads = 1, .head_dim = 512 },
        .strata = &runtime_strata,
        .fit = .{ .template_sha256 = &template_study_hex, .generation_contract_sha256 = &contract_study_hex, .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos },
        .gate = .{ .template_sha256 = &template_study_hex, .generation_contract_sha256 = &contract_study_hex, .response_tokens = 256, .generation_mode = .greedy_fixed_length_ignore_eos },
    }};
    const evidence_digest = if (options.zero_evidence)
        "0000000000000000000000000000000000000000000000000000000000000000"
    else
        "0707070707070707070707070707070707070707070707070707070707070707";
    return std.json.Stringify.valueAlloc(allocator, .{
        .schema = schema,
        .stage = stage,
        .diagnostic_only = true,
        .candidate_qualification = true,
        .serving_qualified = false,
        .release_authority = false,
        .training_role = "internal_development_calibration",
        .evaluation_opened = false,
        .frozen_before_unseen_evaluation = true,
        .identity = .{ .artifact_sha256 = &artifact_hex, .tokenizer_sha256 = &tokenizer_hex, .template_sha256 = &template_hex, .layout_sha256 = &layout_hex, .implementation_sha256 = &runtime_hex },
        .implementation = Implementation{ .engine = .native_whole_head_v2, .algorithm_version = 2, .packed_abi_version = 2, .kv_dtype = .f16, .kernel_route = .metal_context_optimized, .split_decode = true, .flash_prefill = true, .block_tokens = 512, .solver = .residual_omp_active_nnls_fp64_qr, .acceptance = .rounded_whole_head_v2 },
        .allocation_page_bytes = std.heap.pageSize(),
        .max_prompt_tokens = options.max_prompt_tokens,
        .max_output_tokens = options.max_output_tokens,
        .minimum_saved_bytes = std.heap.pageSize(),
        .profile_semantic_sha256 = &semantic_hex,
        .owners = &[_]struct { layer: u32, retained: []const u32 }{.{ .layer = 0, .retained = &retained }},
        .query_protocols = &runtime_protocols,
        .evidence = .{ .campaign_spec_sha256 = evidence_digest, .repeat_results_sha256 = evidence_digest, .training_authority_sha256 = evidence_digest, .target_bootstrap_sha256 = evidence_digest, .target_bootstrap_resource_sha256 = evidence_digest, .source_build_sha256 = evidence_digest, .binary_sha256 = evidence_digest, .freezer_sha256 = evidence_digest },
    }, .{});
}

test "context native v2 qualification profile is separate strict candidate authority" {
    const allocator = std.testing.allocator;
    const bytes = try testArtifact(allocator, .{});
    defer allocator.free(bytes);
    var loaded = try parseBytes(allocator, bytes);
    defer loaded.deinit();
    try std.testing.expectEqual(profiles.FitEngine.native_whole_head_v2, loaded.profile.engine);
    try std.testing.expect(!loaded.parsed.value.serving_qualified and !loaded.parsed.value.release_authority);
    const replacements = [_][2][]const u8{
        .{ "residual_omp_active_nnls_fp64_qr", "bounded_coordinate_descent" },
        .{ "rounded_whole_head_v2", "block_v1" },
        .{ "native_whole_head_v2", "block_v1" },
    };
    for (replacements) |replacement| {
        const changed = try std.mem.replaceOwned(u8, allocator, bytes, replacement[0], replacement[1]);
        defer allocator.free(changed);
        if (parseBytes(allocator, changed)) |unexpected_value| {
            var unexpected = unexpected_value;
            unexpected.deinit();
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    const digest_changed = try std.mem.replaceOwned(u8, allocator, bytes, "\"implementation_sha256\":\"", "\"implementation_sha256\":\"f");
    defer allocator.free(digest_changed);
    if (parseBytes(allocator, digest_changed)) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
    const semantic_changed = try std.mem.replaceOwned(u8, allocator, bytes, "\"profile_semantic_sha256\":\"", "\"profile_semantic_sha256\":\"f");
    defer allocator.free(semantic_changed);
    if (parseBytes(allocator, semantic_changed)) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
}

fn expectTestArtifactRejected(allocator: std.mem.Allocator, options: TestArtifactOptions) !void {
    const bytes = try testArtifact(allocator, options);
    defer allocator.free(bytes);
    if (parseBytes(allocator, bytes)) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
}

test "context native v2 qualification ingress rejects noncanonical rehashed protocols and envelopes" {
    const allocator = std.testing.allocator;
    // testArtifact recomputes both the protocol digest and profile semantic
    // digest after these mutations. Rejection therefore comes from the strict
    // canonical contract, not a conveniently stale checksum.
    try expectTestArtifactRejected(allocator, .{ .max_training_rows = 3071 });
    try expectTestArtifactRejected(allocator, .{ .max_validation_rows = 1023 });
    try expectTestArtifactRejected(allocator, .{ .source_mutation = .order });
    try expectTestArtifactRejected(allocator, .{ .source_mutation = .partition });
    try expectTestArtifactRejected(allocator, .{ .source_mutation = .physical_branch });
    try expectTestArtifactRejected(allocator, .{ .max_prompt_tokens = 0 });
    try expectTestArtifactRejected(allocator, .{ .max_output_tokens = 0 });
    try expectTestArtifactRejected(allocator, .{ .max_prompt_tokens = 131072, .max_output_tokens = 1 });
    try expectTestArtifactRejected(allocator, .{ .max_prompt_tokens = std.math.maxInt(u32), .max_output_tokens = 1 });
    try expectTestArtifactRejected(allocator, .{ .zero_evidence = true });
}
