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

//! Opt-in installed-model entrypoint for the persistent qualification driver.
//! This file is intended to be imported only by the inference test root: its
//! manager-scoped candidate injection does not exist in a release build.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const server = @import("server/server.zig");
const profiles = @import("runtime/kv/compaction_profile.zig");
const Driver = @import("context_serving_driver.zig").Driver;
const protocol_mod = @import("context_serving_protocol.zig");
const Protocol = protocol_mod.Session;
const CoverageTrust = @import("context_serving_coverage_trust.zig").Trust;
const CliRunner = @import("context_serving_cli_process.zig").Runner;
const native_profile = @import("context_native_profile.zig");

const RuntimeStudyAuthority = struct {
    template_sha256: []const u8,
    generation_contract_sha256: []const u8,
    response_tokens: u32,
    generation_mode: enum { greedy_fixed_length_ignore_eos },
};
const RuntimeSourceAuthority = struct {
    kind: enum { request_question_replay, fixed_study_prefill, fixed_study_generated_response },
    partition: enum { training, validation },
    physical_branch: u8,
};
const RuntimeStratum = struct { consumer_layer: u32, query_head: u32, kv_head: u32 };
const RuntimeQueryProtocol = struct {
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

const RuntimeCandidate = struct {
    schema_version: u32,
    identity: struct {
        artifact_sha256: []const u8,
        tokenizer_sha256: []const u8,
        template_sha256: []const u8,
        layout_sha256: []const u8,
        implementation_sha256: []const u8,
    },
    query_protocols: []const RuntimeQueryProtocol,
    implementation: struct {
        algorithm_version: u32,
        packed_abi_version: u32,
        kv_dtype: enum { f16 },
        kernel_route: enum { metal_context_correctness, metal_context_optimized },
        split_decode: bool,
        flash_prefill: bool,
        block_tokens: u32,
        solver: enum { bounded_coordinate_descent, rms_nnls_qr, omp_nnls_qr },
        recipe: enum { relative_cd_ridge_rms },
        solver_identity: struct {
            selection: []const u8,
            mass_solver: []const u8,
            value_solver: []const u8,
            precision: []const u8,
        },
    },
    max_prompt_tokens: u32,
    max_output_tokens: u32,
    allocation_page_bytes: usize,
    minimum_saved_bytes: usize,
    profile_semantic_sha256: []const u8,
    owners: []const struct { layer: u32, retained: []const u32 },
};

const FrozenArtifact = struct {
    schema_version: u32,
    stage: []const u8,
    serving_qualified: bool,
    retention_policy: []const u8,
    script_sha256: []const u8,
    calibration_receipt_sha256: []const u8,
    calibration_document_hashes: []const []const u8,
    calibration_document_ids: []const []const u8,
    calibration_query_hashes: []const []const u8,
    profiles: std.json.Value,
    runtime_candidate: ?RuntimeCandidate = null,
};

fn env(name: [:0]const u8) ?[]const u8 {
    return if (std.c.getenv(name)) |value| std.mem.span(value) else null;
}

fn requiredEnv(name: [:0]const u8) ![]const u8 {
    return env(name) orelse error.MissingContextServingDriverConfig;
}

fn number(name: [:0]const u8, default: u32) !u32 {
    const raw = env(name) orelse return default;
    const value = try std.fmt.parseInt(u32, raw, 10);
    if (value == 0) return error.InvalidContextServingDriverConfig;
    return value;
}

fn digest32(text: []const u8) ![32]u8 {
    if (text.len != 64) return error.InvalidServingDriverIdentity;
    var result: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&result, text) catch return error.InvalidServingDriverIdentity;
    return result;
}

fn executableDigest(allocator: std.mem.Allocator, io: std.Io) ![32]u8 {
    const path = try std.process.executablePathAlloc(io, allocator);
    defer allocator.free(path);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{
        .mode = .read_only,
        .allow_directory = false,
        .follow_symlinks = false,
    });
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.InvalidContextServingDriverConfig;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < stat.size) {
        const remaining: usize = @intCast(@min(stat.size - offset, buffer.len));
        const read = try file.readPositionalAll(io, buffer[0..remaining], offset);
        if (read == 0) return error.InvalidContextServingDriverConfig;
        hash.update(buffer[0..read]);
        offset += read;
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

const LegacyCandidate = struct {
    parsed: std.json.Parsed(FrozenArtifact),
    owners: []profiles.Owner,
    query_protocols: []profiles.QueryProtocolAuthority,
    profile: profiles.Profile,
};

fn parseCandidate(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) !LegacyCandidate {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(bytes);
    var artifact_sha256: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &artifact_sha256, .{});
    var parsed = try std.json.parseFromSlice(FrozenArtifact, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = false,
    });
    errdefer parsed.deinit();
    if (parsed.value.schema_version != 1 or
        !std.mem.eql(u8, parsed.value.stage, "offline_numerical") or
        parsed.value.serving_qualified or
        !std.mem.eql(u8, parsed.value.retention_policy, "fixed"))
        return error.InvalidServingDriverProfile;
    const candidate = parsed.value.runtime_candidate orelse return error.InvalidServingDriverProfile;
    if (candidate.schema_version != 2 or candidate.allocation_page_bytes != std.heap.pageSize() or
        candidate.minimum_saved_bytes == 0 or
        candidate.minimum_saved_bytes % candidate.allocation_page_bytes != 0 or
        candidate.implementation.algorithm_version != 1 or
        candidate.implementation.packed_abi_version != 2 or
        candidate.implementation.kv_dtype != .f16 or
        candidate.implementation.kernel_route != .metal_context_optimized or
        (!candidate.implementation.split_decode and !candidate.implementation.flash_prefill) or
        candidate.implementation.block_tokens != 512 or
        candidate.implementation.solver != .bounded_coordinate_descent or
        candidate.implementation.recipe != .relative_cd_ridge_rms or
        candidate.max_prompt_tokens == 0 or candidate.max_output_tokens == 0)
        return error.InvalidServingDriverProfile;
    const solver_identity = candidate.implementation.solver_identity;
    if (!std.mem.eql(u8, solver_identity.selection, "rms_probability") or
        !std.mem.eql(u8, solver_identity.mass_solver, "positive_coordinate_descent_128") or
        !std.mem.eql(u8, solver_identity.value_solver, "ridge_normal_equations") or
        !std.mem.eql(u8, solver_identity.precision, "numpy_float64"))
        return error.InvalidServingDriverProfile;
    if (candidate.query_protocols.len != candidate.owners.len)
        return error.InvalidServingDriverProfile;
    const owners = try allocator.alloc(profiles.Owner, candidate.owners.len);
    errdefer allocator.free(owners);
    const query_protocols = try allocator.alloc(profiles.QueryProtocolAuthority, candidate.query_protocols.len);
    var protocols_initialized: usize = 0;
    errdefer {
        for (query_protocols[0..protocols_initialized]) |protocol| allocator.free(protocol.strata);
        allocator.free(query_protocols);
    }
    var previous_layer: ?u32 = null;
    for (candidate.owners, candidate.query_protocols, owners, query_protocols) |source, protocol, *destination, *bound| {
        if (source.retained.len == 0 or (previous_layer != null and source.layer <= previous_layer.?))
            return error.InvalidServingDriverProfile;
        if (protocol.layer != source.layer) return error.InvalidServingDriverProfile;
        if (protocol.schema_version != 2 or protocol.sources.len != 5 or protocol.strata.len == 0 or
            protocol.strata.len > 128 or protocol.owner.kv_heads != source.retained.len)
            return error.InvalidServingDriverProfile;
        var sources: [5]profiles.QuerySourceAuthority = undefined;
        for (protocol.sources, &sources) |runtime_source, *profile_source| profile_source.* = .{
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
        const strata = try allocator.alloc(profiles.QueryStratum, protocol.strata.len);
        errdefer allocator.free(strata);
        for (protocol.strata, strata) |runtime_stratum, *profile_stratum| profile_stratum.* = .{
            .consumer_layer = runtime_stratum.consumer_layer,
            .query_head = runtime_stratum.query_head,
            .kv_head = runtime_stratum.kv_head,
        };
        const fit = profiles.QueryStudyAuthority{
            .template_sha256 = try digest32(protocol.fit.template_sha256),
            .generation_contract_sha256 = try digest32(protocol.fit.generation_contract_sha256),
            .response_tokens = protocol.fit.response_tokens,
            .generation_mode = .greedy_fixed_length_ignore_eos,
        };
        const gate = profiles.QueryStudyAuthority{
            .template_sha256 = try digest32(protocol.gate.template_sha256),
            .generation_contract_sha256 = try digest32(protocol.gate.generation_contract_sha256),
            .response_tokens = protocol.gate.response_tokens,
            .generation_mode = .greedy_fixed_length_ignore_eos,
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
            .fit = fit,
            .gate = gate,
        };
        protocols_initialized += 1;
        for (source.retained) |rows| if (rows == 0 or rows > candidate.implementation.block_tokens)
            return error.InvalidServingDriverProfile;
        destination.* = .{ .layer = source.layer, .retained = source.retained };
        previous_layer = source.layer;
    }
    const implementation = profiles.Implementation{
        .algorithm_version = candidate.implementation.algorithm_version,
        .packed_abi_version = candidate.implementation.packed_abi_version,
        .kv_dtype = switch (candidate.implementation.kv_dtype) {
            .f16 => .f16,
        },
        .kernel_route = switch (candidate.implementation.kernel_route) {
            .metal_context_correctness => .metal_context_correctness,
            .metal_context_optimized => .metal_context_optimized,
        },
        .split_decode = candidate.implementation.split_decode,
        .flash_prefill = candidate.implementation.flash_prefill,
        .block_tokens = candidate.implementation.block_tokens,
        .solver = switch (candidate.implementation.solver) {
            .bounded_coordinate_descent => .bounded_coordinate_descent,
            .rms_nnls_qr => .rms_nnls_qr,
            .omp_nnls_qr => .omp_nnls_qr,
        },
    };
    const implementation_digest = profiles.implementationDigest(implementation);
    const expected_implementation = try digest32(candidate.identity.implementation_sha256);
    if (!std.mem.eql(u8, &implementation_digest, &expected_implementation))
        return error.InvalidServingDriverProfile;
    const profile = profiles.Profile{
        .identity = .{
            .artifact = try digest32(candidate.identity.artifact_sha256),
            .tokenizer = try digest32(candidate.identity.tokenizer_sha256),
            .template = try digest32(candidate.identity.template_sha256),
            .layout = try digest32(candidate.identity.layout_sha256),
            .runtime = try digest32(candidate.identity.implementation_sha256),
        },
        .artifact_sha256 = artifact_sha256,
        .owners = owners,
        .query_protocols = query_protocols,
        .max_prompt_tokens = candidate.max_prompt_tokens,
        .max_output_tokens = candidate.max_output_tokens,
        .minimum_saved_bytes = candidate.minimum_saved_bytes,
    };
    const semantic_digest = profile.semanticDigest();
    const expected_semantic = try digest32(candidate.profile_semantic_sha256);
    if (!std.mem.eql(u8, &semantic_digest, &expected_semantic))
        return error.InvalidServingDriverProfile;
    return .{ .parsed = parsed, .owners = owners, .query_protocols = query_protocols, .profile = profile };
}

const Candidate = union(enum) {
    legacy: LegacyCandidate,
    native: native_profile.Loaded,

    fn deinit(self: *Candidate, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .legacy => |*value| {
                value.parsed.deinit();
                allocator.free(value.owners);
                for (value.query_protocols) |protocol| allocator.free(protocol.strata);
                allocator.free(value.query_protocols);
            },
            .native => |*value| value.deinit(),
        }
        self.* = undefined;
    }

    fn profile(self: *const Candidate) *const profiles.Profile {
        return switch (self.*) {
            .legacy => |*value| &value.profile,
            .native => |*value| &value.profile,
        };
    }

    fn splitDecode(self: *const Candidate) bool {
        return switch (self.*) {
            .legacy => |value| value.parsed.value.runtime_candidate.?.implementation.split_decode,
            .native => |value| value.parsed.value.implementation.split_decode,
        };
    }

    fn flashPrefill(self: *const Candidate) bool {
        return switch (self.*) {
            .legacy => |value| value.parsed.value.runtime_candidate.?.implementation.flash_prefill,
            .native => |value| value.parsed.value.implementation.flash_prefill,
        };
    }
};

const CandidateFormat = enum { legacy, native_v2 };

fn candidateFormat(allocator: std.mem.Allocator, bytes: []const u8) !CandidateFormat {
    const Tag = struct { schema: ?[]const u8 = null };
    var tag = try std.json.parseFromSlice(Tag, allocator, bytes, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer tag.deinit();
    if (tag.value.schema) |artifact_schema| if (std.mem.eql(u8, artifact_schema, native_profile.schema))
        return .native_v2;
    return .legacy;
}

fn loadCandidate(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !Candidate {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(bytes);
    if (try candidateFormat(allocator, bytes) == .native_v2)
        return .{ .native = try native_profile.parseBytes(allocator, bytes) };
    // Absence of the native tag preserves the legacy parser byte-for-byte;
    // malformed native artifacts never acquire a second interpretation.
    return .{ .legacy = try parseCandidate(allocator, io, path) };
}

test "context qualification profile format keeps legacy and native v2 disjoint" {
    try std.testing.expectEqual(CandidateFormat.legacy, try candidateFormat(std.testing.allocator, "{\"schema_version\":1,\"stage\":\"offline_numerical\"}"));
    try std.testing.expectEqual(CandidateFormat.native_v2, try candidateFormat(std.testing.allocator, "{\"schema\":\"antfly.context.native.qualification-profile.v1\"}"));
    try std.testing.expectEqual(CandidateFormat.legacy, try candidateFormat(std.testing.allocator, "{\"schema\":\"antfly.context.native.qualification-profile.v0\"}"));
}

test "serving qualification installed persistent Metal driver" {
    if (comptime !builtin.is_test or !build_options.enable_metal) return error.SkipZigTest;
    if (!std.mem.eql(u8, env("ANTFLY_CONTEXT_SERVING_DRIVER") orelse return error.SkipZigTest, "1"))
        return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const models_dir = try requiredEnv("ANTFLY_CONTEXT_MODELS_DIR");
    const model = try requiredEnv("ANTFLY_CONTEXT_MODEL");
    const candidate_path = env("ANTFLY_CONTEXT_PROFILE");
    const candidate_sha = env("ANTFLY_CONTEXT_PROFILE_SHA256");
    if ((candidate_path != null) != (candidate_sha != null))
        return error.InvalidContextServingDriverConfig;
    const campaign_path = try requiredEnv("ANTFLY_CONTEXT_CAMPAIGN_DIR");
    const session_text = try requiredEnv("ANTFLY_CONTEXT_SESSION_ID");
    if (session_text.len != 64) return error.InvalidContextServingDriverConfig;
    _ = try digest32(session_text);
    var session_id: [64]u8 = undefined;
    @memcpy(&session_id, session_text);

    var candidate = if (candidate_path) |path| try loadCandidate(allocator, io, path) else null;
    defer if (candidate) |*value| value.deinit(allocator);
    if (candidate) |value| {
        const expected_profile_sha256 = try digest32(candidate_sha.?);
        if (!std.mem.eql(u8, &value.profile().artifact_sha256, &expected_profile_sha256))
            return error.InvalidServingDriverProfile;
    }

    var node = try server.Node.init(allocator, .{
        .models_dir = models_dir,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .http_max_connections = 1,
        .http_max_request_tasks = 1,
        .process_termination_available = true,
    });
    defer node.deinit();
    try node.attachIo(io);
    node.session_manager.preferred_backends = &.{backends.BackendType.metal};
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &.{backends.BackendType.metal};
    node.model_manager.session_manager.required_backend = .metal;
    if (candidate) |value|
        try node.model_manager.configureCompactionCandidateForTesting(value.profile().*);

    const persistent = try Driver.init(allocator, &node);
    defer persistent.deinit();
    var campaign = try std.Io.Dir.cwd().openDir(io, campaign_path, .{ .follow_symlinks = false });
    defer campaign.close(io);
    const coverage_manifest = env("ANTFLY_CONTEXT_COVERAGE_MANIFEST");
    const coverage_manifest_sha256 = env("ANTFLY_CONTEXT_COVERAGE_MANIFEST_SHA256");
    const coverage_source_receipt = env("ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT");
    if ((coverage_manifest != null) != (coverage_manifest_sha256 != null) or
        (coverage_manifest != null) != (coverage_source_receipt != null))
        return error.InvalidContextServingDriverConfig;
    const coverage_only = candidate == null;
    if (coverage_only and coverage_manifest == null)
        return error.MissingContextServingDriverConfig;
    var coverage_trust: ?CoverageTrust = if (coverage_manifest) |manifest|
        try CoverageTrust.init(
            allocator,
            io,
            campaign,
            manifest,
            coverage_source_receipt.?,
            &session_id,
            try digest32(coverage_manifest_sha256.?),
        )
    else
        null;
    defer if (coverage_trust) |*trust| trust.deinit();
    const cli_binary = env("ANTFLY_CONTEXT_CLI_BINARY");
    const cli_binary_sha256 = env("ANTFLY_CONTEXT_CLI_BINARY_SHA256");
    if ((cli_binary != null) != (cli_binary_sha256 != null))
        return error.InvalidContextServingDriverConfig;
    var cli_runner: ?CliRunner = if (cli_binary) |binary| .{
        .allocator = allocator,
        .io = io,
        .campaign_dir = campaign,
        .binary = binary,
        .expected_binary_sha256 = try digest32(cli_binary_sha256.?),
    } else null;
    const optimize_decode = if (candidate) |value|
        value.splitDecode()
    else
        server.context_coverage_reference_route.split_decode;
    const optimize_prefill = if (candidate) |value|
        value.flashPrefill()
    else
        server.context_coverage_reference_route.flash_prefill;
    const fit_engine: profiles.FitEngine = if (candidate) |value| value.profile().engine else .block_v1;
    const implementation_sha256 = profiles.implementationDigest(
        server.contextServingCompactionImplementationForEngine(fit_engine, optimize_decode, optimize_prefill),
    );
    // Binary is measured from the executable actually running this session.
    // The other three fields are parent-owned campaign authority, injected in
    // the process environment once and never accepted from per-request JSON.
    const provenance: protocol_mod.Provenance = if (coverage_only) .{
        .binary_sha256 = @splat(0),
        .source_build_sha256 = @splat(0),
        .device_sha256 = @splat(0),
        .os_sha256 = @splat(0),
    } else .{
        .binary_sha256 = try executableDigest(allocator, io),
        .source_build_sha256 = try digest32(try requiredEnv("ANTFLY_CONTEXT_SOURCE_BUILD_SHA256")),
        .device_sha256 = try digest32(try requiredEnv("ANTFLY_CONTEXT_DEVICE_SHA256")),
        .os_sha256 = try digest32(try requiredEnv("ANTFLY_CONTEXT_OS_SHA256")),
    };
    var protocol = Protocol{
        .allocator = allocator,
        .io = io,
        .driver = persistent,
        .campaign_dir = campaign,
        .model = model,
        .expected_session_id = session_id,
        .max_context_tokens = try number("ANTFLY_CONTEXT_MAX_TOKENS", 32768),
        .max_kv_tokens = try number("ANTFLY_CONTEXT_MAX_KV_TOKENS", 16384),
        .keep_prefix_tokens = try number("ANTFLY_CONTEXT_KEEP_PREFIX_TOKENS", 512),
        .keep_recent_tokens = try number("ANTFLY_CONTEXT_KEEP_RECENT_TOKENS", 1024),
        .fit_engine = fit_engine,
        .optimize_decode = optimize_decode,
        .optimize_prefill = optimize_prefill,
        .admitted_prefill_chunk_rows = @import("context_capture_coverage.zig").required_prefill_chunk_rows,
        .implementation_sha256 = implementation_sha256,
        .profile_artifact_sha256 = if (candidate) |value| value.profile().artifact_sha256 else @splat(0),
        .profile_semantic_sha256 = if (candidate) |value| value.profile().semanticDigest() else @splat(0),
        .provenance = provenance,
        .coverage = if (coverage_trust) |*trust| trust else null,
        .cli_runner = if (cli_runner) |*runner| runner else null,
        .coverage_only = coverage_only,
    };
    try protocol.run();
}
