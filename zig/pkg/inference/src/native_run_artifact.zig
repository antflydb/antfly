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

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ml = @import("ml");
const compiled_artifact = @import("compiled_artifact.zig");
const gpt_arch = @import("architectures/gpt.zig");
const session_factory = @import("architectures/session_factory.zig");
const manifest_mod = @import("models/manifest.zig");
const model_manager_mod = @import("server/model_manager.zig");
const generation = @import("pipelines/generation.zig");
const graph_mod = @import("graph/root.zig");
const ModelAttentionMode = graph_mod.cache.AttentionMode;
const model_runtime = graph_mod.model_runtime;
const interpreter = @import("graph/interpreter.zig");
const runtime = @import("runtime/root.zig");
const ops = @import("ops/ops.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const tokenizer_mod = @import("inference_tokenizer");
const backends = @import("backends/backends.zig");
const activations = @import("backends/activations.zig");
const PjrtInputBinding = if (build_options.enable_pjrt)
    graph_mod.pjrt_compiler.InputBinding
else
    union(enum) {
        graph_node: ml.graph.NodeId,
        embedding_ids: ml.graph.NodeId,
        semantic_past_graph_node: ml.graph.NodeId,
    };
const PjrtBuffer = if (build_options.enable_pjrt) @import("pjrt").pjrt.Buffer else struct {};

const print = std.debug.print;

fn pjrtExecDebugEnabled() bool {
    return platform.env.getenvBool("ANTFLY_INFERENCE_PJRT_EXEC_DEBUG");
}

const Options = struct {
    artifact_or_manifest_path: []const u8,
    prompt: ?[]const u8 = null,
    validate: bool = false,
    compare_host: bool = false,
    no_chat_template: bool = false,
    raw_prompt: bool = false,
};

pub const ValidationResult = struct {
    backend: []u8,
    kind: []u8,
    model_dir: []u8,
    artifact_path: []u8,
    inputs: usize,
    outputs: usize,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []u8,
    source_artifact: ?[]u8 = null,
    runtime_state_ownership: ?[]u8 = null,
    supports_decode: bool = false,
    is_package: bool = false,
    package_artifact_count: usize = 0,
    package_prefill_count: usize = 0,
    package_decode_count: usize = 0,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.backend);
        allocator.free(self.kind);
        allocator.free(self.model_dir);
        allocator.free(self.artifact_path);
        allocator.free(self.attention_mode);
        if (self.source_artifact) |path| allocator.free(path);
        if (self.runtime_state_ownership) |ownership| allocator.free(ownership);
        self.* = undefined;
    }
};

const PjrtValidationSummary = struct {
    runtime_state_ownership: ?[]const u8 = null,
    supports_decode: bool = false,
};

fn pjrtStateOwnershipName(ownership: model_runtime.RuntimeStateOwnership) []const u8 {
    return switch (ownership) {
        .host_assisted_inputs => "host_assisted_inputs",
        .runtime_owned_host_cache => "runtime_owned_host_cache",
        .backend_owned => "backend_owned",
    };
}

fn pjrtManifestRequiresHostMaterialization(bindings: []const compiled_artifact.PjrtInputBindingMeta) bool {
    for (bindings) |binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_graph_node)) return true;
    }
    return false;
}

fn pjrtManifestHasSemanticPastInputs(bindings: []const compiled_artifact.PjrtInputBindingMeta) bool {
    for (bindings) |binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_key) or
            std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_value))
        {
            return true;
        }
    }
    return false;
}

fn pjrtManifestHasSemanticPresentOutputs(bindings: []const compiled_artifact.PjrtOutputBindingMeta) bool {
    for (bindings) |binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_present_key) or
            std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_present_value))
        {
            return true;
        }
    }
    return false;
}

fn summarizePjrtManifestValidation(manifest: compiled_artifact.Manifest) PjrtValidationSummary {
    const requires_host = pjrtManifestRequiresHostMaterialization(manifest.pjrt_input_bindings);
    const has_semantic_inputs = pjrtManifestHasSemanticPastInputs(manifest.pjrt_input_bindings);
    const has_semantic_outputs = pjrtManifestHasSemanticPresentOutputs(manifest.pjrt_output_bindings);
    const ownership: model_runtime.RuntimeStateOwnership = if (!requires_host and (has_semantic_inputs or has_semantic_outputs))
        .backend_owned
    else
        .host_assisted_inputs;
    return .{
        .runtime_state_ownership = pjrtStateOwnershipName(ownership),
        .supports_decode = has_semantic_inputs or has_semantic_outputs or std.mem.eql(u8, manifest.attention_mode, "paged_decode"),
    };
}

fn summarizePjrtPackageValidation(
    allocator: std.mem.Allocator,
    io: std.Io,
    package: compiled_artifact.PackageManifest,
) !PjrtValidationSummary {
    var prefill_requires_host: ?bool = null;
    var prefill_has_present: bool = false;
    var prefill_summary: ?PjrtValidationSummary = null;
    var decode_requires_host: ?bool = null;
    var decode_has_past: bool = false;

    for (package.artifacts) |entry| {
        if (prefill_requires_host == null and std.mem.eql(u8, entry.artifact_role, compiled_artifact.artifact_role_prefill)) {
            var parsed = try compiled_artifact.readManifest(allocator, io, entry.manifest_path);
            defer parsed.deinit();
            prefill_requires_host = pjrtManifestRequiresHostMaterialization(parsed.value.pjrt_input_bindings);
            prefill_has_present = pjrtManifestHasSemanticPresentOutputs(parsed.value.pjrt_output_bindings);
            prefill_summary = summarizePjrtManifestValidation(parsed.value);
            continue;
        }
        if (decode_requires_host == null and std.mem.eql(u8, entry.artifact_role, compiled_artifact.artifact_role_decode)) {
            var parsed = try compiled_artifact.readManifest(allocator, io, entry.manifest_path);
            defer parsed.deinit();
            decode_requires_host = pjrtManifestRequiresHostMaterialization(parsed.value.pjrt_input_bindings);
            decode_has_past = pjrtManifestHasSemanticPastInputs(parsed.value.pjrt_input_bindings);
        }
    }

    if (prefill_requires_host) |prefill_host| {
        if (decode_requires_host) |decode_host| {
            const ownership: model_runtime.RuntimeStateOwnership = if (!prefill_host and !decode_host and prefill_has_present and decode_has_past)
                .backend_owned
            else
                .host_assisted_inputs;
            return .{
                .runtime_state_ownership = pjrtStateOwnershipName(ownership),
                .supports_decode = true,
            };
        }
        return prefill_summary orelse .{};
    }

    return .{};
}

pub const RunResult = struct {
    backend: []u8,
    kind: []u8,
    manifest_path: []u8,
    artifact_path: []u8,
    has_token: bool = false,
    token_id: usize = 0,
    token_text: []u8,
    output_count: usize = 0,
    output_shapes_summary: []u8,
    compare_summary: []u8,
    seq_len: usize,
    query_seq_len: usize,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.backend);
        allocator.free(self.kind);
        allocator.free(self.manifest_path);
        allocator.free(self.artifact_path);
        if (self.token_text.len > 0) allocator.free(self.token_text);
        if (self.output_shapes_summary.len > 0) allocator.free(self.output_shapes_summary);
        if (self.compare_summary.len > 0) allocator.free(self.compare_summary);
        self.* = undefined;
    }
};

const ArtifactContext = struct {
    allocator: std.mem.Allocator,
    manager: *model_manager_mod.ModelManager,
    model_dir: []const u8,
    manifest: manifest_mod.ModelManifest,
    assets: model_manager_mod.ModelManager.TokenizerAssets,
    policy: ?model_manager_mod.ModelLoadPolicy,

    fn init(allocator: std.mem.Allocator, io: std.Io, model_dir: []const u8, policy: ?model_manager_mod.ModelLoadPolicy) !@This() {
        // Keep metadata and tokenizer preparation in the same publication.
        // Later session construction revalidates this retained manifest.
        var publication_lease = try @import("registry/publication_lease.zig").Lease.shared(allocator, io, model_dir);
        defer publication_lease.deinit(io);
        var manifest = try manifest_mod.loadFromDir(allocator, model_dir);
        errdefer manifest.deinit();
        if (policy) |configured| if (configured.preflight) |preflight| {
            _ = try preflight.check(preflight.context, model_dir, &manifest, .native, null);
        };
        const manager = try allocator.create(model_manager_mod.ModelManager);
        errdefer allocator.destroy(manager);
        manager.* = model_manager_mod.ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, io));
        errdefer manager.deinit();
        try (policy orelse model_manager_mod.ModelLoadPolicy{}).configure(manager);
        const assets = try manager.loadGenerationTokenizerAssets(model_dir, &manifest);
        return .{ .allocator = allocator, .manager = manager, .model_dir = model_dir, .manifest = manifest, .assets = assets, .policy = policy };
    }

    fn admissionContext(self: *const @This()) pjrt_admission.Context {
        return .{ .allocator = self.allocator, .manager = self.manager, .policy = self.policy };
    }

    fn deinit(self: *@This()) void {
        self.assets.deinit();
        self.manifest.deinit();
        self.manager.deinit();
        self.allocator.destroy(self.manager);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    if (opts.validate) {
        var validation = try validateArtifact(allocator, io, opts.artifact_or_manifest_path);
        defer validation.deinit(allocator);
        if (validation.is_package) {
            print(
                "loaded artifact package backend={s} kind={s} model_dir={s} package={s} artifacts={d} prefill={d} decode={d}\n",
                .{
                    validation.backend,
                    validation.kind,
                    validation.model_dir,
                    validation.artifact_path,
                    validation.package_artifact_count,
                    validation.package_prefill_count,
                    validation.package_decode_count,
                },
            );
        } else {
            print(
                "loaded artifact backend={s} kind={s} model_dir={s} artifact={s} inputs={d} outputs={d} seq_len={d} query_seq_len={d} attention_mode={s}\n",
                .{
                    validation.backend,
                    validation.kind,
                    validation.model_dir,
                    validation.artifact_path,
                    validation.inputs,
                    validation.outputs,
                    validation.seq_len,
                    validation.query_seq_len,
                    validation.attention_mode,
                },
            );
        }
        if (validation.source_artifact) |path| {
            print("source_artifact={s}\n", .{path});
        }
        if (validation.runtime_state_ownership) |ownership| {
            print("runtime_state_ownership={s} supports_decode={}\n", .{ ownership, validation.supports_decode });
        }
        return;
    }
    const result = try runArtifactPrompt(
        allocator,
        io,
        opts.artifact_or_manifest_path,
        opts.prompt.?,
        opts.compare_host,
        opts.no_chat_template,
        opts.raw_prompt,
        null,
    );
    if (result.has_token) {
        const token_id = result.token_id;
        const token_text = result.token_text;
        const token_id_value: usize = token_id;
        var stdout_buffer: [4096]u8 = undefined;
        var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
        const stdout = &stdout_writer.interface;
        try stdout.print(
            "artifact backend={s} kind={s} seq_len={d} query_seq_len={d} token_id={d}\n",
            .{ result.backend, result.kind, result.seq_len, result.query_seq_len, token_id_value },
        );
        try stdout.print("text=", .{});
        try stdout.writeAll(token_text);
        try stdout.print("\n", .{});
        if (result.compare_summary.len > 0) {
            try stdout.print("compare_host: {s}\n", .{result.compare_summary});
        }
        try stdout.flush();
        return;
    } else {
        print(
            "artifact backend={s} kind={s} seq_len={d} query_seq_len={d} outputs={d}\n",
            .{ result.backend, result.kind, result.seq_len, result.query_seq_len, result.output_count },
        );
        if (result.output_shapes_summary.len > 0) {
            print("output_shapes: {s}\n", .{result.output_shapes_summary});
        }
        if (result.compare_summary.len > 0) {
            print("compare_host: {s}\n", .{result.compare_summary});
        }
        return;
    }
}

pub fn validateArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_or_manifest_path: []const u8,
) !ValidationResult {
    if (compiled_artifact.isPackageManifestPath(artifact_or_manifest_path)) {
        var parsed = try compiled_artifact.readPackageManifest(allocator, io, artifact_or_manifest_path);
        defer parsed.deinit();
        var prefill_count: usize = 0;
        var decode_count: usize = 0;
        for (parsed.value.artifacts) |entry| {
            if (std.mem.eql(u8, entry.artifact_role, compiled_artifact.artifact_role_prefill)) {
                prefill_count += 1;
            } else if (std.mem.eql(u8, entry.artifact_role, compiled_artifact.artifact_role_decode)) {
                decode_count += 1;
            }
        }
        const pjrt_summary = if (std.mem.eql(u8, parsed.value.backend, "xla"))
            try summarizePjrtPackageValidation(allocator, io, parsed.value)
        else
            PjrtValidationSummary{};
        return .{
            .backend = try allocator.dupe(u8, parsed.value.backend),
            .kind = try allocator.dupe(u8, parsed.value.kind),
            .model_dir = try allocator.dupe(u8, parsed.value.model_dir),
            .artifact_path = try allocator.dupe(u8, artifact_or_manifest_path),
            .inputs = 0,
            .outputs = 0,
            .seq_len = 0,
            .query_seq_len = 0,
            .attention_mode = try allocator.dupe(u8, "package"),
            .is_package = true,
            .package_artifact_count = parsed.value.artifacts.len,
            .package_prefill_count = prefill_count,
            .package_decode_count = decode_count,
            .runtime_state_ownership = if (pjrt_summary.runtime_state_ownership) |ownership| try allocator.dupe(u8, ownership) else null,
            .supports_decode = pjrt_summary.supports_decode,
        };
    }

    const manifest_path = try compiled_artifact.resolveManifestPath(allocator, artifact_or_manifest_path);
    defer allocator.free(manifest_path);

    var parsed = try compiled_artifact.readManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    const manifest = parsed.value;

    if (std.mem.eql(u8, manifest.backend, "xla")) {
        if (!build_options.enable_pjrt) return error.BackendUnavailable;
        const hlo_bytes = try std.Io.Dir.cwd().readFileAlloc(io, manifest.artifact_path, allocator, .limited(std.math.maxInt(usize)));
        defer allocator.free(hlo_bytes);
        if (hlo_bytes.len == 0) return error.InvalidArtifact;
        const summary = summarizePjrtManifestValidation(manifest);
        return .{
            .backend = try allocator.dupe(u8, manifest.backend),
            .kind = try allocator.dupe(u8, manifest.kind),
            .model_dir = try allocator.dupe(u8, manifest.model_dir),
            .artifact_path = try allocator.dupe(u8, manifest.artifact_path),
            .inputs = manifest.pjrt_input_bindings.len,
            .outputs = manifest.pjrt_output_node_ids.len,
            .seq_len = manifest.seq_len,
            .query_seq_len = manifest.query_seq_len,
            .attention_mode = try allocator.dupe(u8, manifest.attention_mode),
            .runtime_state_ownership = if (summary.runtime_state_ownership) |ownership| try allocator.dupe(u8, ownership) else null,
            .supports_decode = summary.supports_decode,
        };
    }

    return error.UnsupportedCompileBackend;
}

pub fn runArtifactPrompt(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_or_manifest_path: []const u8,
    prompt: []const u8,
    compare_host: bool,
    no_chat_template: bool,
    raw_prompt: bool,
    load_policy: ?model_manager_mod.ModelLoadPolicy,
) !RunResult {
    return runArtifactPromptWithContext(allocator, io, artifact_or_manifest_path, prompt, compare_host, no_chat_template, raw_prompt, load_policy, null);
}

fn runArtifactPromptWithContext(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_or_manifest_path: []const u8,
    prompt: []const u8,
    compare_host: bool,
    no_chat_template: bool,
    raw_prompt: bool,
    load_policy: ?model_manager_mod.ModelLoadPolicy,
    prepared: ?*ArtifactContext,
) !RunResult {
    var effective = load_policy orelse model_manager_mod.ModelLoadPolicy{};
    var resources: ?model_manager_mod.ModelManager.SharedResources = null;
    if (prepared == null and effective.resources == null) {
        resources = try model_manager_mod.ModelManager.SharedResources.init(allocator, effective.limits);
        effective.resources = &resources.?;
    }
    defer if (resources) |*owned| owned.deinit();
    var bootstrap = model_manager_mod.ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, io));
    defer bootstrap.deinit();
    if (prepared == null) try effective.configure(&bootstrap);
    const admission = if (prepared) |context| context.admissionContext() else pjrt_admission.Context{ .allocator = allocator, .manager = &bootstrap, .policy = effective };
    const budget = try @import("graph/pjrt_metadata.zig").Budget.create(allocator, &admission);
    defer budget.destroy();
    return runArtifactPromptWithMetadata(allocator, io, artifact_or_manifest_path, prompt, compare_host, no_chat_template, raw_prompt, effective, prepared, budget.allocator()) catch |err| return budget.translate(err);
}

fn runArtifactPromptWithMetadata(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact_or_manifest_path: []const u8,
    prompt: []const u8,
    compare_host: bool,
    no_chat_template: bool,
    raw_prompt: bool,
    load_policy: ?model_manager_mod.ModelLoadPolicy,
    prepared: ?*ArtifactContext,
    metadata: std.mem.Allocator,
) !RunResult {
    if (compiled_artifact.isPackageManifestPath(artifact_or_manifest_path)) {
        var package = try compiled_artifact.readPackageManifest(metadata, io, artifact_or_manifest_path);
        defer package.deinit();
        try compiled_artifact.validatePackageManifest(package.value, package.value.backend, package.value.model_dir, package.value.kind, if (std.mem.eql(u8, package.value.backend, "xla")) package.value.pjrt_parameter_mode else null);
        var owned_context = if (prepared == null) try ArtifactContext.init(allocator, io, package.value.model_dir, load_policy) else null;
        defer if (owned_context) |*owned| owned.deinit();
        const context = prepared orelse &owned_context.?;
        if (!std.mem.eql(u8, context.model_dir, package.value.model_dir)) return error.ArtifactModelMismatch;
        const manifest_path = try resolvePackagePrefillManifestPath(
            metadata,
            io,
            artifact_or_manifest_path,
            prompt,
            no_chat_template,
            raw_prompt,
            context,
        );
        defer metadata.free(manifest_path);

        var parsed = try compiled_artifact.readManifest(metadata, io, manifest_path);
        defer parsed.deinit();
        @import("graph/pjrt_metadata.zig").Budget.releaseUnusedCapacity(metadata);
        return runParsedArtifactPrompt(
            allocator,
            io,
            parsed.value,
            manifest_path,
            prompt,
            compare_host,
            no_chat_template,
            raw_prompt,
            load_policy,
            context,
        );
    }

    const manifest_path = try compiled_artifact.resolveManifestPath(metadata, artifact_or_manifest_path);
    defer metadata.free(manifest_path);

    var parsed = try compiled_artifact.readManifest(metadata, io, manifest_path);
    defer parsed.deinit();
    @import("graph/pjrt_metadata.zig").Budget.releaseUnusedCapacity(metadata);
    return runParsedArtifactPrompt(
        allocator,
        io,
        parsed.value,
        manifest_path,
        prompt,
        compare_host,
        no_chat_template,
        raw_prompt,
        load_policy,
        prepared,
    );
}

pub fn tryRunMatchingArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
    load_policy: ?model_manager_mod.ModelLoadPolicy,
) !?RunResult {
    var context = try ArtifactContext.init(allocator, io, model_dir, load_policy);
    defer context.deinit();
    const budget = try @import("graph/pjrt_metadata.zig").Budget.create(allocator, &context.admissionContext());
    defer budget.destroy();
    return tryRunMatchingArtifactWithMetadata(allocator, io, search_dir, backend, model_dir, prompt, no_chat_template, raw_prompt, load_policy, &context, budget.allocator()) catch |err| return budget.translate(err);
}

fn tryRunMatchingArtifactWithMetadata(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
    load_policy: ?model_manager_mod.ModelLoadPolicy,
    context: *ArtifactContext,
    metadata: std.mem.Allocator,
) !?RunResult {
    const model_manifest = &context.manifest;
    const assets = &context.assets;

    const apply_chat_template = !raw_prompt and !no_chat_template and assets.chat_tmpl != null;
    const rendered_prompt = if (raw_prompt)
        try allocator.dupe(u8, prompt)
    else if (apply_chat_template)
        try assets.chat_tmpl.?.apply(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }}, true)
    else
        try generation.formatMessages(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }});
    defer allocator.free(rendered_prompt);

    var encoded = try generation.encodePromptForGeneration(
        assets.tokenizer(),
        allocator,
        rendered_prompt,
        4096,
        model_manifest.add_bos_token,
        model_manifest.bos_token,
    );
    defer encoded.deinit();

    const prompt_tokens = countPromptTokens(encoded.attention_mask);
    if (prompt_tokens == 0) {
        return null;
    }

    if (try compiled_artifact.findManagedPjrtPackage(
        metadata,
        io,
        search_dir,
        backend,
        model_dir,
        prompt_tokens,
        prompt_tokens,
        "paged_prefill",
    )) |package_manifest_path| {
        defer metadata.free(package_manifest_path);
        @import("graph/pjrt_metadata.zig").Budget.releaseUnusedCapacity(metadata);
        return try runArtifactPromptWithContext(
            allocator,
            io,
            package_manifest_path,
            prompt,
            false,
            no_chat_template,
            raw_prompt,
            load_policy,
            context,
        );
    }

    var found = (try findMatchingFullModelArtifact(
        metadata,
        io,
        search_dir,
        backend,
        model_dir,
        prompt_tokens,
        prompt_tokens,
        "paged_prefill",
    )) orelse {
        std.log.info(
            "offline artifact lookup miss: backend={s} dir={s} model_dir={s} seq_len={d} query_seq_len={d} attention_mode=paged_prefill",
            .{ backend, search_dir, model_dir, prompt_tokens, prompt_tokens },
        );
        return null;
    };
    defer found.deinit(metadata);

    var parsed = try compiled_artifact.readManifest(metadata, io, found.manifest_path);
    defer parsed.deinit();

    const artifact = parsed.value;
    if (artifact.seq_len != prompt_tokens or artifact.query_seq_len != prompt_tokens) {
        return null;
    }
    @import("graph/pjrt_metadata.zig").Budget.releaseUnusedCapacity(metadata);
    return try runArtifactPromptWithContext(
        allocator,
        io,
        found.manifest_path,
        prompt,
        false,
        no_chat_template,
        raw_prompt,
        load_policy,
        context,
    );
}

fn findMatchingFullModelPackageManifest(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
) !?[]u8 {
    const PackageCandidate = struct {
        kind: []const u8,
        parameter_mode: ?[]const u8,
    };

    const candidates: []const PackageCandidate = if (std.mem.eql(u8, backend, "xla"))
        &.{
            .{ .kind = "pjrt_executable", .parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded },
            .{ .kind = "pjrt_executable", .parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs },
            .{ .kind = "pjrt_hlo", .parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded },
            .{ .kind = "pjrt_hlo", .parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs },
        }
    else
        return null;

    for (candidates) |candidate| {
        const package_path = try compiled_artifact.packageManifestPath(
            allocator,
            search_dir,
            backend,
            model_dir,
            candidate.kind,
            candidate.parameter_mode,
        );
        defer allocator.free(package_path);

        var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => continue,
            else => return err,
        };
        defer parsed.deinit();

        const package = parsed.value;
        if (!std.mem.eql(u8, package.backend, backend) or
            !std.mem.eql(u8, package.model_dir, model_dir) or
            !std.mem.eql(u8, package.kind, candidate.kind))
        {
            return error.InvalidArtifact;
        }
        if (candidate.parameter_mode) |mode| {
            if (!std.mem.eql(u8, package.pjrt_parameter_mode, mode)) return error.InvalidArtifact;
        }

        for (package.artifacts) |entry| {
            if (!std.mem.eql(u8, entry.artifact_role, compiled_artifact.artifact_role_prefill)) continue;
            if (entry.seq_len != seq_len or entry.query_seq_len != query_seq_len) continue;
            if (!std.mem.eql(u8, entry.attention_mode, attention_mode)) continue;
            const owned_package_path = try allocator.dupe(u8, package_path);
            return owned_package_path;
        }
    }
    return null;
}

fn findMatchingFullModelArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: []const u8,
) !?compiled_artifact.LocatedArtifact {
    return compiled_artifact.findManagedPjrtPrefill(allocator, io, search_dir, backend, model_dir, seq_len, query_seq_len, attention_mode);
}

fn resolvePackagePrefillManifestPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_manifest_path: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
    context: *ArtifactContext,
) ![]u8 {
    var parsed = try compiled_artifact.readPackageManifest(allocator, io, package_manifest_path);
    defer parsed.deinit();

    try compiled_artifact.validatePackageManifest(
        parsed.value,
        parsed.value.backend,
        parsed.value.model_dir,
        parsed.value.kind,
        if (std.mem.eql(u8, parsed.value.backend, "xla")) parsed.value.pjrt_parameter_mode else null,
    );

    const prompt_tokens = try promptTokenCountForModel(
        allocator,
        context,
        prompt,
        no_chat_template,
        raw_prompt,
    );

    const entry = try compiled_artifact.findUniqueMatchingPackageEntry(parsed.value, .{
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .seq_len = prompt_tokens,
        .query_seq_len = prompt_tokens,
        .attention_mode = "paged_prefill",
    });
    return if (entry) |found|
        allocator.dupe(u8, found.manifest_path)
    else
        error.ArtifactShapeMismatch;
}

fn promptTokenCountForModel(
    allocator: std.mem.Allocator,
    context: *ArtifactContext,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
) !usize {
    const model_manifest = &context.manifest;
    const assets = &context.assets;

    const apply_chat_template = !raw_prompt and !no_chat_template and assets.chat_tmpl != null;
    const rendered_prompt = if (raw_prompt)
        try allocator.dupe(u8, prompt)
    else if (apply_chat_template)
        try assets.chat_tmpl.?.apply(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }}, true)
    else
        try generation.formatMessages(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }});
    defer allocator.free(rendered_prompt);

    var encoded = try generation.encodePromptForGeneration(
        assets.tokenizer(),
        allocator,
        rendered_prompt,
        4096,
        model_manifest.add_bos_token,
        model_manifest.bos_token,
    );
    defer encoded.deinit();
    return countPromptTokens(encoded.attention_mask);
}

pub fn tryRunArtifactFromDir(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
) !?RunResult {
    if (try tryRunPackageArtifactFromDir(
        allocator,
        io,
        search_dir,
        backend,
        model_dir,
        prompt,
        no_chat_template,
        raw_prompt,
    )) |result| return result;

    var dir = if (std.fs.path.isAbsolute(search_dir))
        try std.Io.Dir.openDirAbsolute(io, search_dir, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, search_dir, .{ .iterate = true });
    defer dir.close(io);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".inference.json")) continue;

        const manifest_path = try std.fs.path.join(allocator, &.{ search_dir, entry.name });
        errdefer allocator.free(manifest_path);

        var parsed = compiled_artifact.readManifest(allocator, io, manifest_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => {
                allocator.free(manifest_path);
                continue;
            },
            else => return err,
        };
        defer parsed.deinit();

        const manifest = parsed.value;
        if (!std.mem.eql(u8, manifest.backend, backend)) {
            allocator.free(manifest_path);
            continue;
        }
        if (!std.mem.eql(u8, manifest.model_dir, model_dir)) {
            allocator.free(manifest_path);
            continue;
        }
        if (!std.mem.eql(u8, manifest.attention_mode, "paged_prefill")) {
            allocator.free(manifest_path);
            continue;
        }
        if (!(std.mem.eql(u8, manifest.kind, "pjrt_hlo") or
            std.mem.eql(u8, manifest.kind, "pjrt_executable") or
            std.mem.eql(u8, manifest.kind, "pjrt_partition_executable")))
        {
            allocator.free(manifest_path);
            continue;
        }

        const result = runParsedArtifactPrompt(
            allocator,
            io,
            manifest,
            manifest_path,
            prompt,
            false,
            no_chat_template,
            raw_prompt,
            null,
            null,
        ) catch |err| switch (err) {
            error.ArtifactShapeMismatch => {
                allocator.free(manifest_path);
                continue;
            },
            else => {
                allocator.free(manifest_path);
                return err;
            },
        };
        allocator.free(manifest_path);
        return result;
    }

    return null;
}

fn tryRunPackageArtifactFromDir(
    allocator: std.mem.Allocator,
    io: std.Io,
    search_dir: []const u8,
    backend: []const u8,
    model_dir: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
) !?RunResult {
    var dir = if (std.fs.path.isAbsolute(search_dir))
        try std.Io.Dir.openDirAbsolute(io, search_dir, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, search_dir, .{ .iterate = true });
    defer dir.close(io);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!compiled_artifact.isPackageManifestPath(entry.name)) continue;

        const package_path = try std.fs.path.join(allocator, &.{ search_dir, entry.name });
        errdefer allocator.free(package_path);

        var parsed = compiled_artifact.readPackageManifest(allocator, io, package_path) catch |err| switch (err) {
            error.FileNotFound, error.InvalidCharacter, error.SyntaxError, error.UnexpectedToken, error.UnknownField => {
                allocator.free(package_path);
                continue;
            },
            else => return err,
        };
        defer parsed.deinit();

        const package = parsed.value;
        if (!std.mem.eql(u8, package.backend, backend) or !std.mem.eql(u8, package.model_dir, model_dir)) {
            allocator.free(package_path);
            continue;
        }

        const result = runArtifactPrompt(
            allocator,
            io,
            package_path,
            prompt,
            false,
            no_chat_template,
            raw_prompt,
            null,
        ) catch |err| switch (err) {
            error.ArtifactShapeMismatch => {
                allocator.free(package_path);
                continue;
            },
            else => {
                allocator.free(package_path);
                return err;
            },
        };
        allocator.free(package_path);
        return result;
    }

    return null;
}

fn runParsedArtifactPrompt(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    manifest_path: []const u8,
    prompt: []const u8,
    compare_host: bool,
    no_chat_template: bool,
    raw_prompt: bool,
    load_policy: ?model_manager_mod.ModelLoadPolicy,
    prepared: ?*ArtifactContext,
) !RunResult {
    const session_manager = backends.SessionManager.init(allocator);
    try validateArtifactBackendPolicy(&session_manager, artifact.backend);

    var owned_context = if (prepared == null) try ArtifactContext.init(allocator, io, artifact.model_dir, load_policy) else null;
    defer if (owned_context) |*owned| owned.deinit();
    const context = prepared orelse &owned_context.?;
    if (!std.mem.eql(u8, context.model_dir, artifact.model_dir)) return error.ArtifactModelMismatch;
    const model_manifest = &context.manifest;
    const assets = &context.assets;

    const apply_chat_template = !raw_prompt and !no_chat_template and artifact.chat_template_applied and assets.chat_tmpl != null;
    const rendered_prompt = if (raw_prompt)
        try allocator.dupe(u8, prompt)
    else if (apply_chat_template)
        try assets.chat_tmpl.?.apply(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }}, true)
    else
        try generation.formatMessages(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }});
    defer {
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact rendered prompt free", .{});
        allocator.free(rendered_prompt);
    }

    var encoded = try generation.encodePromptForGeneration(
        assets.tokenizer(),
        allocator,
        rendered_prompt,
        @max(artifact.seq_len, artifact.prompt_tokens),
        model_manifest.add_bos_token,
        model_manifest.bos_token,
    );
    defer {
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact encoded prompt deinit begin", .{});
        encoded.deinit();
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact encoded prompt deinit complete", .{});
    }

    const prompt_tokens = countPromptTokens(encoded.attention_mask);
    if (prompt_tokens != artifact.seq_len) return error.ArtifactShapeMismatch;
    const input_start = artifact.seq_len - artifact.query_seq_len;
    const graph_input_ids = encoded.ids[input_start .. input_start + artifact.query_seq_len];

    return runPreparedArtifactPrompt(
        allocator,
        io,
        artifact,
        manifest_path,
        assets.tokenizer(),
        graph_input_ids,
        compare_host,
        context,
    );
}

fn validateArtifactBackendPolicy(
    session_manager: *const backends.SessionManager,
    artifact_backend_name: []const u8,
) !void {
    const artifact_backend = try native_backend_choice.compiledArtifactBackend(artifact_backend_name);
    try native_backend_choice.validateRequiredCompiledBackend(session_manager, artifact_backend);
}

test "run-artifact rejects a compiled backend outside the required policy" {
    var session_manager = backends.SessionManager.init(std.testing.allocator);
    session_manager.required_backend = .native;
    session_manager.required_backend_invalid = false;
    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        validateArtifactBackendPolicy(&session_manager, "xla"),
    );
    try std.testing.expectError(
        error.UnsupportedCompileBackend,
        validateArtifactBackendPolicy(&session_manager, "onnx"),
    );

    session_manager.required_backend = null;
    try validateArtifactBackendPolicy(&session_manager, "xla");
}

fn runPreparedArtifactPrompt(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    manifest_path: []const u8,
    tokenizer: tokenizer_mod.Tokenizer,
    graph_input_ids: []const i32,
    compare_host: bool,
    context: *ArtifactContext,
) !RunResult {
    if (std.mem.eql(u8, artifact.kind, "pjrt_hlo") or
        std.mem.eql(u8, artifact.kind, "pjrt_executable"))
    {
        const full_result = if (std.mem.eql(u8, artifact.backend, "xla"))
            try runPjrtHloArtifactFull(allocator, io, artifact, graph_input_ids, compare_host, context)
        else
            return error.UnsupportedCompileBackend;
        errdefer full_result.deinit(allocator);

        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact full result token_id={d}", .{full_result.token_id});
        }
        const token_i32 = std.math.cast(i32, full_result.token_id) orelse return error.UnsupportedShape;
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact decoding token text", .{});
        }
        const token_text = try tokenizer.decode(allocator, &.{token_i32});
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact token text decoded len={d}", .{token_text.len});
        }

        const result = RunResult{
            .backend = try allocator.dupe(u8, artifact.backend),
            .kind = try allocator.dupe(u8, artifact.kind),
            .manifest_path = try allocator.dupe(u8, manifest_path),
            .artifact_path = try allocator.dupe(u8, artifact.artifact_path),
            .has_token = true,
            .token_id = full_result.token_id,
            .token_text = token_text,
            .output_shapes_summary = &.{},
            .compare_summary = if (full_result.compare_summary.len > 0)
                try allocator.dupe(u8, full_result.compare_summary)
            else
                &.{},
            .seq_len = artifact.seq_len,
            .query_seq_len = artifact.query_seq_len,
        };
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact RunResult constructed", .{});
        }
        full_result.deinit(allocator);
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact full result deinit complete", .{});
        }
        if (std.mem.eql(u8, artifact.backend, "xla")) {
            if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact post-run yield begin", .{});
            yieldAfterShortLivedPjrtSessionClose();
            if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact post-run yield complete", .{});
        }
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact returning RunResult", .{});
        }
        return result;
    }

    if (std.mem.eql(u8, artifact.kind, "pjrt_partition_hlo") or
        std.mem.eql(u8, artifact.kind, "pjrt_partition_executable"))
    {
        var partition_result = try runPjrtPartitionArtifact(
            allocator,
            io,
            artifact,
            graph_input_ids,
            compare_host,
            context,
        );
        defer partition_result.deinit(allocator);
        return .{
            .backend = try allocator.dupe(u8, artifact.backend),
            .kind = try allocator.dupe(u8, artifact.kind),
            .manifest_path = try allocator.dupe(u8, manifest_path),
            .artifact_path = try allocator.dupe(u8, artifact.artifact_path),
            .token_text = &.{},
            .output_count = partition_result.output_count,
            .output_shapes_summary = try allocator.dupe(u8, partition_result.output_shapes_summary),
            .compare_summary = try allocator.dupe(u8, partition_result.compare_summary),
            .seq_len = artifact.seq_len,
            .query_seq_len = artifact.query_seq_len,
        };
    }

    return error.UnsupportedArtifactKind;
}

const FullModelRunResult = struct {
    token_id: usize,
    compare_summary: []u8,

    pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        if (self.compare_summary.len > 0) allocator.free(self.compare_summary);
    }
};

fn modelRuntimeAttentionModeFromArtifact(artifact: compiled_artifact.Manifest) !ModelAttentionMode {
    if (std.mem.eql(u8, artifact.attention_mode, "full_recompute")) return .full_recompute;
    if (std.mem.eql(u8, artifact.attention_mode, "paged_prefill")) return .paged_prefill;
    if (std.mem.eql(u8, artifact.attention_mode, "paged_decode")) return .paged_decode;
    return error.UnsupportedArtifactKind;
}

const ArtifactHostWork = enum { trace, execute };

fn artifactKvPoolConfig(cfg: gpt_arch.Config, backend: runtime.kv.pool.BackendKind, dtype: runtime.kv.pool.KvDType, work: ArtifactHostWork) runtime.kv.pool.KvPoolConfig {
    return .{
        .backend = backend,
        .dtype = dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(cfg.num_hidden_layers),
        .num_kv_heads = cfg.maxKvHeads(),
        .head_dim = cfg.maxHeadDim(),
        .sliding_window_size = cfg.kvPoolSlidingWindowSize(false),
        .store_cpu_bytes = work == .execute,
    };
}

fn artifactHostEstimate(session: backends.Session, artifact: compiled_artifact.Manifest, work: ArtifactHostWork) !runtime.tier.memory.Estimate {
    const cfg = session_factory.getGptConfig(session) orelse return error.InvalidModelForGeneration;
    return artifactHostEstimateForConfig(cfg, session_factory.recommendedKvDTypeForSession(session, .native), artifact, work);
}

fn artifactHostEstimateForConfig(cfg: @import("models/gpt.zig").Config, kv_dtype: runtime.kv.pool.KvDType, artifact: compiled_artifact.Manifest, work: ArtifactHostWork) !runtime.tier.memory.Estimate {
    var estimate = try runtime.tier.memory.estimateGptGeneration(.native, kv_dtype, cfg, artifact.seq_len, 0, artifact.query_seq_len);
    // Full recomputation has no persistent KV, and tracing requires only KV
    // page identities/geometry. Actual host KV is owned only by native execution.
    if (work == .trace or std.mem.eql(u8, artifact.attention_mode, "full_recompute")) estimate.kv_bytes = 0;
    return estimate;
}

fn artifactRunBudget(session: backends.Session, artifact: compiled_artifact.Manifest, policy: ?model_manager_mod.ModelLoadPolicy, work: ArtifactHostWork) !runtime.tier.memory.RunBudget {
    const memory = runtime.tier.memory;
    var limits = session_factory.widenBudgetLimitsForSession(session, memory.defaultLimitsForBackend(.cpu));
    if (policy) |configured| limits = memory.applyLimitOverrides(limits, configured.limits);
    var budget = memory.RunBudget.init(limits);
    try budget.reserveEstimate(try artifactHostEstimate(session, artifact, work));
    return budget;
}

fn admitArtifactRun(manager: *model_manager_mod.ModelManager, session: backends.Session, budget: runtime.tier.memory.RunBudget) !model_manager_mod.GenerationAdmissionLease {
    return manager.acquireGenerationResources(&.{.{ .session = session, .resources = .{
        .backend_class = .cpu,
        .limits = budget.limits,
        .amounts = .{ .host_kv_bytes = budget.host_kv_bytes, .host_scratch_bytes = budget.host_scratch_bytes },
    } }});
}

const pjrt_admission = @import("graph/pjrt_admission.zig");
const ArtifactDeviceAdmission = pjrt_admission.ArtifactDeviceAdmission;
const PreparedPjrtArtifact = pjrt_admission.PreparedPjrtArtifact;

fn runPjrtHloArtifactFull(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    compare_host: bool,
    context: *ArtifactContext,
) !FullModelRunResult {
    if (comptime !build_options.enable_pjrt) return error.BackendUnavailable;
    if (artifact.pjrt_input_bindings.len == 0 or artifact.pjrt_output_node_ids.len == 0) return error.MissingArtifactMetadata;

    _ = try ArtifactDeviceAdmission.plan(artifact);
    const model_manager = context.manager;
    var model = try model_manager.loadArtifactSession(artifact.model_dir, context.manifest);
    defer model.deinit();
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;

    const input_ids = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(input_ids);
    for (graph_input_ids, 0..) |id, i| input_ids[i] = id;

    var run_budget = try artifactRunBudget(model.session, artifact, context.policy, .trace);
    var run_admission = try admitArtifactRun(model_manager, model.session, run_budget);
    defer run_admission.release();
    var cb = try session_factory.getComputeBackendWithBudget(model.session, allocator, &run_budget);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact host backend deinit begin", .{});
        cb.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact host backend deinit complete", .{});
    }

    const plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator) orelse return error.MissingPjrtPlugin;
    defer allocator.free(plugin_path);

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact KV manager deinit begin", .{});
        kv_manager.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact KV manager deinit complete", .{});
    }
    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    var host_last_logits: ?[]f32 = null;
    defer if (host_last_logits) |logits| allocator.free(logits);
    if (compare_host) {
        var reference = try artifactHostEstimate(model.session, artifact, .execute);
        reference.scratch_bytes = 0; // already covered by host preparation
        var reference_admission = try model_manager.acquireRunResourceAmounts(.cpu, run_budget.limits, runtime.tier.memory.AdmissionAmounts.fromEstimate(reference));
        defer reference_admission.release();
        try run_budget.reserveEstimate(reference);
        defer run_budget.releaseEstimate(reference);
        std.log.info("PJRT compare_host: computing native reference logits before PJRT execution", .{});
        host_last_logits = try computeNativeLastLogitsWithBackend(
            allocator,
            artifact,
            graph_input_ids,
            gpt_config,
            &cb,
            backend_kind,
            kv_dtype,
            model.shared_moe_cache,
        );
        std.log.info("PJRT compare_host: native reference logits ready len={d}", .{host_last_logits.?.len});
    }

    const pool_id = try kv_manager.addPool(artifactKvPoolConfig(gpt_config, backend_kind, kv_dtype, .trace));

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact decode state deinit begin", .{});
        decode_state.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact decode state deinit complete", .{});
    }
    var decode_context = try buildArtifactDecodeContext(artifact, &decode_state);

    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact graph cache deinit begin", .{});
        graph_cache.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact graph cache deinit complete", .{});
    }
    const pipeline_stub = .{
        .allocator = allocator,
        .gpt_config = gpt_config,
        .cb = &cb,
    };
    const entry = try graph_mod.execution.ensureGraphEntry(
        &pipeline_stub,
        &graph_cache,
        input_ids,
        1,
        artifact.seq_len,
        &decode_context,
    );

    var prepared = try PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin_path);
    defer prepared.deinit();
    var client = try prepared.createClient(plugin_path);
    defer client.deinit();

    var model_executor_ctx = try graph_mod.pjrt_artifact_executor.createModelExecutorFromAdmittedArtifactBytes(
        allocator,
        io,
        artifact,
        prepared.bytes,
        &entry.graph,
        &cb,
        &client,
        &context.admissionContext(),
    );
    var model_executor = model_executor_ctx.modelExecutor();
    defer model_executor.deinit();
    try prepared.loaded(artifact, try model_executor_ctx.executor.executable.compiledMemoryStats());

    var runtime_value = try model_executor.createRuntime(allocator);
    defer runtime_value.deinit();

    var output = try runtime_value.prefill(allocator, .{
        .input_ids = input_ids,
        .seq_len = artifact.seq_len,
        .query_seq_len = artifact.query_seq_len,
        .attention_mode = try modelRuntimeAttentionModeFromArtifact(artifact),
    });
    defer output.deinit(allocator);

    const output_logits = try output.hostLogits(allocator);
    const token_id = activations.argmax(output_logits);
    const compare_summary = if (compare_host) blk: {
        const summary = try compareLastLogitsSummary(allocator, host_last_logits orelse return error.InvalidArtifactOutput, output_logits, token_id);
        std.log.info("PJRT compare_host: summary computed", .{});
        break :blk summary;
    } else try allocator.dupe(u8, "");

    return .{
        .token_id = token_id,
        .compare_summary = compare_summary,
    };
}

fn compareLastLogitsSummary(
    allocator: std.mem.Allocator,
    host_last_logits: []const f32,
    artifact_last_logits: []const f32,
    artifact_top1: usize,
) ![]u8 {
    const n = @min(host_last_logits.len, artifact_last_logits.len);
    if (n == 0) return error.InvalidArtifactOutput;
    var max_abs_diff: f32 = 0;
    var mean_abs_diff: f32 = 0;
    for (host_last_logits[0..n], artifact_last_logits[0..n]) |host_v, artifact_v| {
        const diff = @abs(host_v - artifact_v);
        if (diff > max_abs_diff) max_abs_diff = diff;
        mean_abs_diff += diff;
    }
    mean_abs_diff /= @floatFromInt(n);
    return std.fmt.allocPrint(
        allocator,
        "host_top1={d}, artifact_top1={d}; last_logits:max_abs_diff={d}, mean_abs_diff={d}",
        .{ activations.argmax(host_last_logits), artifact_top1, max_abs_diff, mean_abs_diff },
    );
}

const OwnedTensorInfoList = struct {
    items: []backends.TensorInfo,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        for (self.items) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
        allocator.free(self.items);
        self.* = undefined;
    }
};

fn yieldAfterShortLivedPjrtSessionClose() void {
    // The CPU PJRT plugin can still be quiescing runtime/ABSL state after a
    // short compile-load-execute-destroy cycle. Pause briefly so smoke-sized
    // artifact runs do not abort during immediate process exit.
    platform.time.sleepNs(250_000_000);
}

fn appendUniqueCaptureNodeId(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(ml.graph.NodeId),
    node_id: ml.graph.NodeId,
) !void {
    for (out.items) |existing| {
        if (existing == node_id) return;
    }
    try out.append(allocator, node_id);
}

const MaterializedPartitionInputs = struct {
    values: []ops.CT,
    node_ids: []ml.graph.NodeId,
    cb: ops.ComputeBackend,
    runtime_inputs: []interpreter.RuntimeInput,
    graph: ?ml.graph.Graph = null,
    model: model_manager_mod.ArtifactSession,
    run_budget: *runtime.tier.memory.RunBudget,
    run_admission: model_manager_mod.GenerationAdmissionLease,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        var capture = interpreter.CapturedValuesResult{ .values = self.values, .allocator = allocator };
        capture.deinit(&self.cb);
        for (self.runtime_inputs) |ri| self.cb.free(ri.value);
        allocator.free(self.runtime_inputs);
        allocator.free(self.node_ids);
        self.cb.deinit();
        if (self.graph) |*graph| graph.deinit();
        self.run_admission.release();
        self.model.deinit();
        allocator.destroy(self.run_budget);
    }
};

const PartitionRunResult = struct {
    output_count: usize,
    output_shapes_summary: []u8,
    compare_summary: []u8,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.output_shapes_summary);
        allocator.free(self.compare_summary);
        self.* = undefined;
    }
};

fn runPjrtPartitionArtifact(
    allocator: std.mem.Allocator,
    _: std.Io,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    compare_host: bool,
    context: *ArtifactContext,
) !PartitionRunResult {
    if (comptime !build_options.enable_pjrt) return error.BackendUnavailable;
    if (artifact.pjrt_input_bindings.len == 0 or artifact.pjrt_output_node_ids.len == 0) return error.MissingArtifactMetadata;

    _ = try ArtifactDeviceAdmission.plan(artifact);
    const capture_node_ids = try collectPjrtPartitionNodeIds(
        allocator,
        artifact.pjrt_input_bindings,
        artifact.pjrt_output_node_ids,
    );
    defer allocator.free(capture_node_ids);

    var materialized = try materializePartitionInputs(
        allocator,
        artifact,
        graph_input_ids,
        capture_node_ids,
        context,
    );
    defer materialized.deinit(allocator);
    const graph = if (materialized.graph) |*graph| graph else return error.MissingArtifactMetadata;

    const input_bindings = try buildPjrtInputBindings(allocator, artifact.pjrt_input_bindings);
    defer allocator.free(input_bindings);
    const output_node_ids = try clonePjrtOutputNodeIds(allocator, artifact.pjrt_output_node_ids);
    defer allocator.free(output_node_ids);

    const plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator) orelse return error.MissingPjrtPlugin;
    defer allocator.free(plugin_path);

    var prepared = try PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin_path);
    defer prepared.deinit();
    var client = try prepared.createClient(plugin_path);
    defer client.deinit();

    var exec = if (std.mem.eql(u8, artifact.kind, "pjrt_partition_executable"))
        try graph_mod.pjrt_executor.createExecutorFromSerializedExecutable(
            allocator,
            graph,
            prepared.bytes,
            input_bindings,
            output_node_ids,
            &materialized.cb,
            &client,
        )
    else
        try graph_mod.pjrt_executor.createExecutorFromHlo(
            allocator,
            graph,
            prepared.bytes,
            input_bindings,
            output_node_ids,
            &materialized.cb,
            &client,
        );
    defer exec.partitionExecutor().deinitExecutor();
    try prepared.loaded(artifact, try exec.executable.compiledMemoryStats());

    const graph_input_ids_i64 = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(graph_input_ids_i64);
    for (graph_input_ids, 0..) |id, i| graph_input_ids_i64[i] = id;

    const values = try allocator.alloc(?ops.CT, graph.nodeCount());
    defer allocator.free(values);
    @memset(values, null);
    for (materialized.node_ids, materialized.values) |node_id, value| {
        values[@intCast(node_id)] = value;
    }

    const output_buffers = try exec.executeToBuffers(values, .{ .embedding_ids = graph_input_ids_i64 }, allocator);
    defer {
        for (output_buffers) |*buf| buf.deinit();
        allocator.free(output_buffers);
    }

    const output_shapes_summary = try summarizeShapesConst(allocator, exec.output_shapes);
    errdefer allocator.free(output_shapes_summary);
    const compare_summary = if (compare_host)
        try comparePjrtPartitionOutputs(
            allocator,
            output_node_ids,
            output_buffers,
            materialized.node_ids,
            materialized.values,
            &materialized.cb,
        )
    else
        try allocator.dupe(u8, "");
    errdefer allocator.free(compare_summary);

    return .{
        .output_count = output_buffers.len,
        .output_shapes_summary = output_shapes_summary,
        .compare_summary = compare_summary,
    };
}

fn collectPjrtPartitionNodeIds(
    allocator: std.mem.Allocator,
    input_bindings: []const compiled_artifact.PjrtInputBindingMeta,
    output_node_ids: []const u32,
) ![]ml.graph.NodeId {
    var out = std.ArrayListUnmanaged(ml.graph.NodeId).empty;
    defer out.deinit(allocator);
    for (input_bindings) |binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_graph_node)) {
            try appendUniqueCaptureNodeId(allocator, &out, @intCast(binding.node_id));
        }
    }
    for (output_node_ids) |node_id| {
        try appendUniqueCaptureNodeId(allocator, &out, @intCast(node_id));
    }
    return out.toOwnedSlice(allocator);
}

fn buildPjrtInputBindings(
    allocator: std.mem.Allocator,
    bindings: []const compiled_artifact.PjrtInputBindingMeta,
) ![]PjrtInputBinding {
    const out = try allocator.alloc(PjrtInputBinding, bindings.len);
    errdefer allocator.free(out);
    for (bindings, 0..) |binding, i| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_graph_node)) {
            out[i] = .{ .graph_node = @intCast(binding.node_id) };
        } else if (compiled_artifact.pjrtBindingIsInputIds(binding)) {
            out[i] = .{ .embedding_ids = @intCast(binding.node_id) };
        } else if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_key) or
            std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_past_value))
        {
            out[i] = .{ .semantic_past_graph_node = @intCast(binding.node_id) };
        } else {
            return error.UnsupportedArtifactInputs;
        }
    }
    return out;
}

fn clonePjrtOutputNodeIds(allocator: std.mem.Allocator, output_node_ids: []const u32) ![]ml.graph.NodeId {
    const out = try allocator.alloc(ml.graph.NodeId, output_node_ids.len);
    for (output_node_ids, 0..) |node_id, i| out[i] = @intCast(node_id);
    return out;
}

fn materializePartitionInputs(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    capture_node_ids: []const ml.graph.NodeId,
    context: *ArtifactContext,
) !MaterializedPartitionInputs {
    const model_manager = context.manager;
    var model = try model_manager.loadArtifactSession(artifact.model_dir, context.manifest);
    errdefer model.deinit();
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;

    const graph_input_ids_i64 = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(graph_input_ids_i64);
    for (graph_input_ids, 0..) |id, i| graph_input_ids_i64[i] = id;

    const run_budget = try allocator.create(runtime.tier.memory.RunBudget);
    errdefer allocator.destroy(run_budget);
    run_budget.* = try artifactRunBudget(model.session, artifact, context.policy, .execute);
    var run_admission = try admitArtifactRun(model_manager, model.session, run_budget.*);
    errdefer run_admission.release();
    var cb = try session_factory.getComputeBackendWithBudget(model.session, allocator, run_budget);
    errdefer cb.deinit();

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);

    const pool_id = try kv_manager.addPool(artifactKvPoolConfig(gpt_config, backend_kind, kv_dtype, .execute));

    var decode_state = generation.NativeDecodeState.initPaged(allocator, &kv_manager, pool_id, model.shared_moe_cache);
    defer decode_state.deinit();
    var decode_context = try buildArtifactDecodeContext(artifact, &decode_state);

    var graph_cache = graph_mod.cache.GraphCache.init(allocator);
    defer graph_cache.deinit();

    const pipeline_stub = .{
        .allocator = allocator,
        .gpt_config = gpt_config,
        .cb = &cb,
    };
    const entry = try graph_mod.execution.ensureGraphEntry(
        &pipeline_stub,
        &graph_cache,
        graph_input_ids_i64,
        1,
        artifact.seq_len,
        &decode_context,
    );

    const capturable_node_ids = try filterCapturableNodeIds(allocator, &entry.graph, capture_node_ids);
    defer allocator.free(capturable_node_ids);

    const runtime_inputs = try buildGraphRuntimeInputs(allocator, &cb, &entry.graph, gpt_config);
    errdefer {
        for (runtime_inputs) |ri| cb.free(ri.value);
        allocator.free(runtime_inputs);
    }

    const cached_analysis = try interpreter.CachedAnalysis.computeForTargets(
        allocator,
        &entry.graph,
        capturable_node_ids,
    );
    defer {
        var owned = cached_analysis;
        owned.deinit(allocator);
    }

    var captured = try interpreter.captureNodeValues(
        allocator,
        &entry.graph,
        &cb,
        .{
            .attention = gpt_arch.attentionContextFromDecode(&decode_context),
            .embedding_ids = graph_input_ids_i64,
            .runtime_inputs = runtime_inputs,
            .cached_analysis = cached_analysis,
        },
        capturable_node_ids,
    );
    errdefer captured.deinit(&cb);

    try overrideCapturedPagedKvInputs(
        allocator,
        artifact,
        &entry.graph,
        &cb,
        &kv_manager,
        &decode_context,
        capturable_node_ids,
        captured.values,
    );

    var shape_graph = try cloneShapeOnlyGraph(allocator, &entry.graph);
    errdefer shape_graph.deinit();

    return .{
        .values = captured.values,
        .node_ids = try allocator.dupe(ml.graph.NodeId, capturable_node_ids),
        .cb = cb,
        .runtime_inputs = runtime_inputs,
        .graph = shape_graph,
        .model = model,
        .run_budget = run_budget,
        .run_admission = run_admission,
    };
}

fn cloneShapeOnlyGraph(allocator: std.mem.Allocator, source: *const ml.graph.Graph) !ml.graph.Graph {
    var graph = ml.graph.Graph.init(allocator);
    errdefer graph.deinit();
    try graph.nodes.appendSlice(allocator, source.nodes.items);
    return graph;
}

fn filterCapturableNodeIds(
    allocator: std.mem.Allocator,
    graph: *const ml.graph.Graph,
    capture_node_ids: []const ml.graph.NodeId,
) ![]ml.graph.NodeId {
    var out = std.ArrayListUnmanaged(ml.graph.NodeId).empty;
    defer out.deinit(allocator);
    for (capture_node_ids) |node_id| {
        if (graph.node(node_id).op == .fused_from_float32) continue;
        try out.append(allocator, node_id);
    }
    return out.toOwnedSlice(allocator);
}

fn buildArtifactDecodeContext(
    artifact: compiled_artifact.Manifest,
    decode_state: *generation.NativeDecodeState,
) !gpt_arch.DecodeContext {
    if (std.mem.eql(u8, artifact.attention_mode, "full_recompute")) {
        return .{
            .attention_mode = .full_recompute,
            .total_sequence_len = artifact.seq_len,
            .query_sequence_len = artifact.query_seq_len,
            .kv_sequence_len = artifact.seq_len,
            .kv_position_offset = 0,
            .moe_runtime = &decode_state.moe_runtime,
        };
    }

    try decode_state.notePrefill(artifact.seq_len);
    const ctx = decode_state.gptDecodeContext(artifact.seq_len, artifact.query_seq_len);
    if (std.mem.eql(u8, artifact.attention_mode, "paged_prefill")) {
        if (ctx.attention_mode != .paged_prefill) return error.ArtifactShapeMismatch;
        return ctx;
    }
    if (std.mem.eql(u8, artifact.attention_mode, "paged_decode")) {
        if (ctx.attention_mode != .paged_decode) return error.ArtifactShapeMismatch;
        return ctx;
    }
    return error.UnsupportedArtifactKind;
}

fn overrideCapturedPagedKvInputs(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph: *const ml.graph.Graph,
    cb: *const ops.ComputeBackend,
    kv_manager: *runtime.kv.manager.KvManager,
    decode_context: *const gpt_arch.DecodeContext,
    captured_node_ids: []const ml.graph.NodeId,
    captured_values: []ops.CT,
) !void {
    if (artifact.attention_bindings.len == 0) return;
    if (!std.mem.eql(u8, artifact.attention_mode, "paged_prefill")) return;
    const kv = decode_context.kv_cache orelse return;

    for (artifact.attention_bindings) |binding| {
        if (!binding.skip_kv_write) continue;
        if (binding.layer_index == std.math.maxInt(u32)) return error.MissingArtifactMetadata;
        const layer_index: usize = @intCast(binding.layer_index);
        const gathered = try kv_manager.gatherLayerKv(
            allocator,
            kv.sequence_id,
            layer_index,
            decode_context.kv_sequence_len,
        );
        defer {
            allocator.free(gathered.k);
            allocator.free(gathered.v);
        }
        try overrideCapturedNodeWithKv(
            allocator,
            graph,
            cb,
            captured_node_ids,
            captured_values,
            @intCast(binding.k_node_id),
            decode_context.kv_sequence_len,
            gathered.k,
        );
        try overrideCapturedNodeWithKv(
            allocator,
            graph,
            cb,
            captured_node_ids,
            captured_values,
            @intCast(binding.v_node_id),
            decode_context.kv_sequence_len,
            gathered.v,
        );
    }
}

fn overrideCapturedNodeWithKv(
    allocator: std.mem.Allocator,
    graph: *const ml.graph.Graph,
    cb: *const ops.ComputeBackend,
    captured_node_ids: []const ml.graph.NodeId,
    captured_values: []ops.CT,
    node_id: ml.graph.NodeId,
    token_count: usize,
    gathered: []const f32,
) !void {
    const capture_idx = findCapturedNodeIndex(captured_node_ids, node_id) orelse return;
    const shape = graph.node(node_id).output_shape;
    const expected_len = shapeElementCount(shape) orelse return error.UnsupportedShape;
    if (gathered.len != expected_len) {
        if (token_count == 0 or gathered.len % token_count != 0 or expected_len % token_count != 0) {
            return error.InvalidPagedKvState;
        }
    }
    const compacted = if (gathered.len == expected_len)
        try allocator.dupe(f32, gathered)
    else
        try compactGatheredKvRows(allocator, token_count, expected_len, gathered);
    defer allocator.free(compacted);

    var dims_buf: [8]i32 = undefined;
    const rank = shape.rank();
    if (rank > dims_buf.len) return error.UnsupportedShape;
    for (0..rank) |axis| dims_buf[axis] = @intCast(shape.dim(@intCast(axis)));
    const replacement = try cb.fromFloat32Shape(compacted, dims_buf[0..rank]);
    cb.free(captured_values[capture_idx]);
    captured_values[capture_idx] = replacement;
}

fn compactGatheredKvRows(
    allocator: std.mem.Allocator,
    token_count: usize,
    expected_len: usize,
    gathered: []const f32,
) ![]f32 {
    const expected_row_width = expected_len / token_count;
    const gathered_row_width = gathered.len / token_count;
    if (expected_row_width > gathered_row_width) return error.InvalidPagedKvState;
    const compacted = try allocator.alloc(f32, expected_len);
    errdefer allocator.free(compacted);
    for (0..token_count) |token_idx| {
        const src_start = token_idx * gathered_row_width;
        const dst_start = token_idx * expected_row_width;
        @memcpy(
            compacted[dst_start .. dst_start + expected_row_width],
            gathered[src_start .. src_start + expected_row_width],
        );
    }
    return compacted;
}

fn shapeElementCount(shape: ml.graph.Shape) ?usize {
    var total: usize = 1;
    for (0..shape.rank()) |axis| {
        const dim = shape.dim(@intCast(axis));
        if (dim <= 0) return null;
        total = std.math.mul(usize, total, @as(usize, @intCast(dim))) catch return null;
    }
    return total;
}

fn computeNativeLastLogitsWithBackend(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    gpt_config: gpt_arch.Config,
    cb: *ops.ComputeBackend,
    backend_kind: runtime.kv.pool.BackendKind,
    kv_dtype: runtime.kv.pool.KvDType,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
) ![]f32 {
    var scratch_arena = std.heap.ArenaAllocator.init(platform.allocator.reclaimingScratch(allocator));
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();

    const input_ids = try scratch.alloc(i64, graph_input_ids.len);
    for (graph_input_ids, 0..) |id, i| input_ids[i] = id;

    var kv_manager = runtime.kv.manager.KvManager.init(scratch);
    defer kv_manager.deinit();
    const pool_id = try kv_manager.addPool(artifactKvPoolConfig(gpt_config, backend_kind, kv_dtype, .execute));

    var decode_state = generation.NativeDecodeState.initPaged(scratch, &kv_manager, pool_id, shared_moe_cache);
    defer decode_state.deinit();
    var decode_context = try buildArtifactDecodeContext(artifact, &decode_state);

    std.log.info("native artifact reference forward begin seq_len={d} query_seq_len={d}", .{ artifact.seq_len, artifact.query_seq_len });
    const logits = try gpt_arch.forward(
        cb,
        scratch,
        gpt_config,
        input_ids,
        1,
        artifact.seq_len,
        &decode_context,
    );
    std.log.info("native artifact reference forward complete logits={d}", .{logits.len});

    const vocab_size = gpt_config.vocab_size;
    if (logits.len < vocab_size) return error.InvalidArtifactOutput;
    return try allocator.dupe(f32, logits[logits.len - vocab_size ..]);
}

fn buildGraphRuntimeInputs(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    graph: *const ml.graph.Graph,
    gpt_config: anytype,
) ![]interpreter.RuntimeInput {
    const params = graph.parameters.items;
    const inputs = try allocator.alloc(interpreter.RuntimeInput, params.len);
    errdefer allocator.free(inputs);
    for (params, 0..) |param_id, idx| {
        const name = graph.parameterName(graph.node(param_id));
        inputs[idx] = .{
            .node_id = param_id,
            .value = try graphWeightForArtifact(allocator, cb, gpt_config, name),
        };
    }
    return inputs;
}

fn graphWeightForArtifact(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    gpt_config: anytype,
    name: []const u8,
) !ops.CT {
    return cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => {
            if (std.mem.eql(u8, name, "lm_head.weight")) {
                return switch (gpt_config.family) {
                    .gpt2 => cb.getWeight("wte.weight"),
                    .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .phi => cb.getWeight("model.embed_tokens.weight"),
                    else => cb.getWeight("model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
                };
            }
            var fallback_buf: [128]u8 = undefined;
            if (graphOmittedVProjFallback(gpt_config, name, &fallback_buf)) |fallback_name| {
                return cb.getWeight(fallback_name);
            }
            if (graphOptionalRouterInputScale(name)) {
                const ones = try allocator.alloc(f32, gpt_config.hidden_size);
                defer allocator.free(ones);
                @memset(ones, 1.0);
                const shape = [_]i32{@intCast(gpt_config.hidden_size)};
                return cb.fromFloat32Shape(ones, &shape);
            }
            if (graphOptionalExpertOutputScale(gpt_config, name)) {
                const ones = try allocator.alloc(f32, gpt_config.num_local_experts);
                defer allocator.free(ones);
                @memset(ones, 1.0);
                const shape = [_]i32{@intCast(gpt_config.num_local_experts)};
                return cb.fromFloat32Shape(ones, &shape);
            }
            return err;
        },
        else => return err,
    };
}

fn graphOptionalRouterInputScale(name: []const u8) bool {
    const prefix = "model.layers.";
    const suffix = ".block_sparse_moe.gate.input_scale";
    return std.mem.startsWith(u8, name, prefix) and std.mem.endsWith(u8, name, suffix);
}

fn graphOptionalExpertOutputScale(gpt_config: anytype, name: []const u8) bool {
    if (gpt_config.num_local_experts == 0) return false;
    const prefix = "model.layers.";
    const suffix = ".block_sparse_moe.expert_output_scale";
    return std.mem.startsWith(u8, name, prefix) and std.mem.endsWith(u8, name, suffix);
}

fn graphOmittedVProjFallback(gpt_config: anytype, name: []const u8, buf: *[128]u8) ?[]const u8 {
    const prefix = "model.layers.";
    const suffix = ".self_attn.v_proj.weight";
    if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
    const layer_text = name[prefix.len .. name.len - suffix.len];
    const layer = std.fmt.parseInt(usize, layer_text, 10) catch return null;
    if (!gpt_config.layerOmitsVProj(layer)) return null;
    return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer}) catch null;
}

fn summarizeShapes(allocator: std.mem.Allocator, shapes: [][]i64) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    for (shapes, 0..) |shape, idx| {
        if (idx > 0) try out.appendSlice(allocator, ", ");
        const prefix = try std.fmt.allocPrint(allocator, "out{d}=[", .{idx});
        defer allocator.free(prefix);
        try out.appendSlice(allocator, prefix);
        for (shape, 0..) |dim, dim_idx| {
            if (dim_idx > 0) try out.appendSlice(allocator, "x");
            const dim_text = try std.fmt.allocPrint(allocator, "{d}", .{dim});
            defer allocator.free(dim_text);
            try out.appendSlice(allocator, dim_text);
        }
        try out.append(allocator, ']');
    }
    return out.toOwnedSlice(allocator);
}

fn summarizeShapesConst(allocator: std.mem.Allocator, shapes: []const []const i64) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    for (shapes, 0..) |shape, idx| {
        if (idx > 0) try out.appendSlice(allocator, ", ");
        const prefix = try std.fmt.allocPrint(allocator, "out{d}=[", .{idx});
        defer allocator.free(prefix);
        try out.appendSlice(allocator, prefix);
        for (shape, 0..) |dim, dim_idx| {
            if (dim_idx > 0) try out.appendSlice(allocator, "x");
            const dim_text = try std.fmt.allocPrint(allocator, "{d}", .{dim});
            defer allocator.free(dim_text);
            try out.appendSlice(allocator, dim_text);
        }
        try out.append(allocator, ']');
    }
    return out.toOwnedSlice(allocator);
}

fn comparePjrtPartitionOutputs(
    allocator: std.mem.Allocator,
    output_node_ids: []const ml.graph.NodeId,
    output_buffers: []const PjrtBuffer,
    captured_node_ids: []const ml.graph.NodeId,
    captured_values: []const ops.CT,
    cb: *const ops.ComputeBackend,
) ![]u8 {
    if (comptime !build_options.enable_pjrt) return error.BackendUnavailable;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    for (output_node_ids, output_buffers, 0..) |node_id, buffer, idx| {
        const capture_idx = findCapturedNodeIndex(captured_node_ids, node_id) orelse return error.MissingRuntimeInput;
        const staging_allocator = platform.allocator.reclaimingScratch(allocator);
        const host_data = try cb.toFloat32(captured_values[capture_idx], staging_allocator);
        defer staging_allocator.free(host_data);
        const pjrt_data = try buffer.toFloat32(staging_allocator);
        defer staging_allocator.free(pjrt_data);
        if (host_data.len != pjrt_data.len) return error.ArtifactShapeMismatch;
        var max_abs_diff: f32 = 0.0;
        for (host_data, pjrt_data) |host_v, pjrt_v| {
            const diff = @abs(host_v - pjrt_v);
            if (diff > max_abs_diff) max_abs_diff = diff;
        }
        if (idx > 0) try out.appendSlice(allocator, ", ");
        const line = try std.fmt.allocPrint(allocator, "out{d}:max_abs_diff={d}", .{ idx, max_abs_diff });
        defer allocator.free(line);
        try out.appendSlice(allocator, line);
    }
    return out.toOwnedSlice(allocator);
}

fn findFeatureIndex(names: []const []u8, target: []const u8) ?usize {
    for (names, 0..) |name, i| {
        if (std.mem.eql(u8, name, target)) return i;
    }
    return null;
}

fn findCapturedNodeIndex(node_ids: []const ml.graph.NodeId, target: ml.graph.NodeId) ?usize {
    for (node_ids, 0..) |node_id, idx| {
        if (node_id == target) return idx;
    }
    return null;
}

fn countPromptTokens(attention_mask: []const i32) usize {
    var count: usize = 0;
    while (count < attention_mask.len and attention_mask[count] != 0) : (count += 1) {}
    return count;
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 1) return error.InvalidArgs;
    var opts = Options{
        .artifact_or_manifest_path = args[0],
    };
    var i: usize = 1;
    if (i < args.len and !std.mem.startsWith(u8, args[i], "--")) {
        opts.prompt = args[i];
        i += 1;
    }
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--no-chat-template")) {
            opts.no_chat_template = true;
        } else if (std.mem.eql(u8, args[i], "--raw-prompt")) {
            opts.raw_prompt = true;
        } else if (std.mem.eql(u8, args[i], "--compare-host")) {
            opts.compare_host = true;
        } else if (std.mem.eql(u8, args[i], "--validate") or std.mem.eql(u8, args[i], "--dry-run")) {
            opts.validate = true;
        } else {
            return error.InvalidArgs;
        }
    }
    if (!opts.validate and opts.prompt == null) return error.InvalidArgs;
    return opts;
}

pub fn printUsage() void {
    print(
        \\usage: antfly inference run-artifact <artifact-or-manifest> <prompt> [--no-chat-template] [--raw-prompt] [--compare-host]
        \\       antfly inference run-artifact <artifact-or-manifest> [--validate|--dry-run]
        \\
        \\<artifact-or-manifest> may be a raw artifact path, a .inference.json sidecar,
        \\or a .antfly-inference-package.json package manifest.
        \\Runs an offline artifact for its exact traced shape and prints the top-1
        \\next token without retracing or recompiling. Package manifests resolve the
        \\matching prefill entry for the prompt shape. Partition artifacts print
        \\output shapes; --compare-host adds a traced native output comparison.
        \\Use --validate/--dry-run for load-only validation without execution; package
        \\validation reports artifact/prefill/decode counts and runtime ownership.
        \\PJRT/XLA HLO artifacts require ANTFLY_INFERENCE_XLA_PLUGIN,
        \\ANTFLY_INFERENCE_PJRT_PLUGIN, PJRT_PLUGIN_PATH, or PJRT_PLUGIN.
        \\
    , .{});
}

test "parseArgs accepts artifact path and prompt" {
    const opts = try parseArgs(&.{ "/tmp/model.onnx.inference.json", "hello", "--raw-prompt", "--compare-host" });
    try std.testing.expectEqualStrings("/tmp/model.onnx.inference.json", opts.artifact_or_manifest_path);
    try std.testing.expectEqualStrings("hello", opts.prompt.?);
    try std.testing.expect(opts.raw_prompt);
    try std.testing.expect(opts.compare_host);
}

test "parseArgs accepts validate without prompt" {
    const opts = try parseArgs(&.{ "/tmp/model.onnx.inference.json", "--validate" });
    try std.testing.expectEqualStrings("/tmp/model.onnx.inference.json", opts.artifact_or_manifest_path);
    try std.testing.expectEqual(@as(?[]const u8, null), opts.prompt);
    try std.testing.expect(opts.validate);
}

test "validateArtifact keeps XLA metadata inspection outside execution policy" {
    if (!build_options.enable_pjrt) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const artifact_path = try std.fs.path.join(allocator, &.{ base_dir, "model.hlo" });
    defer allocator.free(artifact_path);
    const manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.hlo.inference.json" });
    defer allocator.free(manifest_path);

    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = artifact_path, .data = "hlo" });
    try compiled_artifact.writeManifest(allocator, io, manifest_path, .{
        .backend = "xla",
        .kind = "stablehlo",
        .model_dir = "/tmp/model",
        .artifact_path = artifact_path,
        .prompt_tokens = 1,
        .seq_len = 1,
        .query_seq_len = 1,
        .attention_mode = "causal",
        .raw_prompt = true,
        .chat_template_applied = false,
    });

    var validation = try validateArtifact(allocator, io, manifest_path);
    defer validation.deinit(allocator);
    try std.testing.expectEqualStrings("xla", validation.backend);
}

test "validateArtifact summarizes package manifests" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.prefill.exec.inference.json" });
    defer allocator.free(prefill_manifest_path);
    const decode_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.decode.s3.exec.inference.json" });
    defer allocator.free(decode_manifest_path);
    const package_path = try std.fs.path.join(allocator, &.{ base_dir, "gpt2.xla.pjrt_executable.inputs.antfly-inference-package.json" });
    defer allocator.free(package_path);

    try compiled_artifact.writeManifest(allocator, io, prefill_manifest_path, .{
        .backend = "xla",
        .kind = "pjrt_executable",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.prefill.exec",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .prompt_tokens = 2,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .pjrt_input_bindings = @constCast(&[_]compiled_artifact.PjrtInputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 0, .name = "input_ids" },
            .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 7, .name = "wte.weight" },
        }),
        .pjrt_output_node_ids = @constCast(&[_]u32{ 10, 11 }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 10, .name = "logits" },
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 11, .name = "present.0.key", .layer_index = 0 },
        }),
    });
    try compiled_artifact.writeManifest(allocator, io, decode_manifest_path, .{
        .backend = "xla",
        .kind = "pjrt_executable",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.decode.s3.exec",
        .artifact_role = compiled_artifact.artifact_role_decode,
        .prompt_tokens = 2,
        .seq_len = 3,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .pjrt_input_bindings = @constCast(&[_]compiled_artifact.PjrtInputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 0, .name = "input_ids" },
            .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 7, .name = "wte.weight" },
            .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 11, .name = "past_key_values.0.key", .layer_index = 0 },
        }),
        .pjrt_output_node_ids = @constCast(&[_]u32{ 12, 13 }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 12, .name = "logits" },
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 13, .name = "present.0.key", .layer_index = 0 },
        }),
    });

    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = prefill_manifest_path,
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = decode_manifest_path,
                .artifact_path = "/tmp/model.decode.s3.exec",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 3,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    });

    var validation = try validateArtifact(allocator, io, package_path);
    defer validation.deinit(allocator);
    try std.testing.expect(validation.is_package);
    try std.testing.expectEqualStrings("xla", validation.backend);
    try std.testing.expectEqualStrings("pjrt_executable", validation.kind);
    try std.testing.expectEqual(@as(usize, 2), validation.package_artifact_count);
    try std.testing.expectEqual(@as(usize, 1), validation.package_prefill_count);
    try std.testing.expectEqual(@as(usize, 1), validation.package_decode_count);
    try std.testing.expectEqualStrings("host_assisted_inputs", validation.runtime_state_ownership.?);
    try std.testing.expect(validation.supports_decode);
}

test "validateArtifact summarizes backend-owned PJRT package manifests" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.prefill.exec.inference.json" });
    defer allocator.free(prefill_manifest_path);
    const decode_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.decode.s3.exec.inference.json" });
    defer allocator.free(decode_manifest_path);
    const package_path = try std.fs.path.join(allocator, &.{ base_dir, "gpt2.xla.pjrt_executable.embedded.antfly-inference-package.json" });
    defer allocator.free(package_path);

    try compiled_artifact.writeManifest(allocator, io, prefill_manifest_path, .{
        .backend = "xla",
        .kind = "pjrt_executable",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.prefill.exec",
        .artifact_role = compiled_artifact.artifact_role_prefill,
        .prompt_tokens = 2,
        .seq_len = 2,
        .query_seq_len = 2,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded,
        .pjrt_input_bindings = @constCast(&[_]compiled_artifact.PjrtInputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 0, .name = "input_ids" },
        }),
        .pjrt_output_node_ids = @constCast(&[_]u32{ 10, 11 }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 10, .name = "logits" },
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 11, .name = "present.0.key", .layer_index = 0 },
        }),
    });
    try compiled_artifact.writeManifest(allocator, io, decode_manifest_path, .{
        .backend = "xla",
        .kind = "pjrt_executable",
        .model_dir = "/tmp/model",
        .artifact_path = "/tmp/model.decode.s3.exec",
        .artifact_role = compiled_artifact.artifact_role_decode,
        .prompt_tokens = 2,
        .seq_len = 3,
        .query_seq_len = 1,
        .attention_mode = "paged_decode",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded,
        .pjrt_input_bindings = @constCast(&[_]compiled_artifact.PjrtInputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 0, .name = "input_ids" },
            .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 11, .name = "past_key_values.0.key", .layer_index = 0 },
        }),
        .pjrt_output_node_ids = @constCast(&[_]u32{ 12, 13 }),
        .pjrt_output_bindings = @constCast(&[_]compiled_artifact.PjrtOutputBindingMeta{
            .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 12, .name = "logits" },
            .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 13, .name = "present.0.key", .layer_index = 0 },
        }),
    });

    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded,
        .artifacts = &.{
            .{
                .manifest_path = prefill_manifest_path,
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
            .{
                .manifest_path = decode_manifest_path,
                .artifact_path = "/tmp/model.decode.s3.exec",
                .artifact_role = compiled_artifact.artifact_role_decode,
                .seq_len = 3,
                .query_seq_len = 1,
                .attention_mode = "paged_decode",
            },
        },
    });

    var validation = try validateArtifact(allocator, io, package_path);
    defer validation.deinit(allocator);
    try std.testing.expect(validation.is_package);
    try std.testing.expectEqualStrings("backend_owned", validation.runtime_state_ownership.?);
    try std.testing.expect(validation.supports_decode);
}

test "findMatchingFullModelPackageManifest resolves matching PJRT package" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "xla",
        "/tmp/model",
        "pjrt_executable",
        compiled_artifact.pjrt_parameter_mode_inputs,
    );
    defer allocator.free(package_path);
    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
        },
    });

    const found = try findMatchingFullModelPackageManifest(
        allocator,
        io,
        base_dir,
        "xla",
        "/tmp/model",
        2,
        2,
        "paged_prefill",
    );
    try std.testing.expect(found != null);
    defer allocator.free(found.?);
    try std.testing.expectEqualStrings(package_path, found.?);
}

test "findMatchingFullModelPackageManifest prefers embedded PJRT package" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const embedded_package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "xla",
        "/tmp/model",
        "pjrt_executable",
        compiled_artifact.pjrt_parameter_mode_embedded,
    );
    defer allocator.free(embedded_package_path);
    const inputs_package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "xla",
        "/tmp/model",
        "pjrt_executable",
        compiled_artifact.pjrt_parameter_mode_inputs,
    );
    defer allocator.free(inputs_package_path);
    try compiled_artifact.writePackageManifest(allocator, io, embedded_package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_embedded,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.embedded.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.embedded.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
        },
    });
    try compiled_artifact.writePackageManifest(allocator, io, inputs_package_path, .{
        .backend = "xla",
        .model_dir = "/tmp/model",
        .kind = "pjrt_executable",
        .pjrt_parameter_mode = compiled_artifact.pjrt_parameter_mode_inputs,
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.inputs.exec.inference.json",
                .artifact_path = "/tmp/model.prefill.inputs.exec",
                .artifact_role = compiled_artifact.artifact_role_prefill,
                .seq_len = 2,
                .query_seq_len = 2,
                .attention_mode = "paged_prefill",
            },
        },
    });

    const found = try findMatchingFullModelPackageManifest(
        allocator,
        io,
        base_dir,
        "xla",
        "/tmp/model",
        2,
        2,
        "paged_prefill",
    );
    try std.testing.expect(found != null);
    defer allocator.free(found.?);
    try std.testing.expectEqualStrings(embedded_package_path, found.?);
}

fn writeArtifactAdmissionFixture(dir: std.Io.Dir) !void {
    try dir.writeFile(std.testing.io, .{ .sub_path = "model.safetensors", .data = "x" });
    try dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"llama\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":2,\"intermediate_size\":8,\"vocab_size\":2}" });
    try dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data = "{\"version\":\"1.0\",\"model\":{\"type\":\"BPE\",\"vocab\":{\"a\":0,\"b\":1},\"merges\":[]}}" });
}

fn artifactTokenizerLifetimeScenario(a: std.mem.Allocator, root: []const u8) !void {
    const memory = runtime.tier.memory;
    const limits = memory.Limits{ .host_limit_bytes = 128 * 1024 * 1024, .combined_limit_bytes = 128 * 1024 * 1024 };
    var resources = try model_manager_mod.ModelManager.SharedResources.init(a, limits);
    defer resources.deinit();
    var observer = model_manager_mod.ModelManager.init(a, backends.SessionManager.init(a));
    defer observer.deinit();
    const policy = model_manager_mod.ModelLoadPolicy{ .limits = limits, .resources = &resources };
    try policy.configure(&observer);
    {
        var context = try ArtifactContext.init(a, std.testing.io, root, policy);
        defer context.deinit();
        try std.testing.expect(observer.currentAdmissionAmounts().hostTotalBytes() >= 16 * 1024 * 1024);
        const encoded = try context.assets.tokenizer().encode(a, "ab");
        defer a.free(encoded);
    }
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, observer.currentAdmissionAmounts());
}

test "run-artifact admitted tokenizer assets refund across every allocation failure" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeArtifactAdmissionFixture(tmp.dir);
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    try platform.allocator.checkAllAllocationFailures(a, artifactTokenizerLifetimeScenario, .{root});
}

test "run-artifact PJRT admission covers buffers workspace CPU placement and refunds" {
    if (comptime !build_options.enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    const memory = runtime.tier.memory;
    const mib: usize = 1024 * 1024;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeArtifactAdmissionFixture(tmp.dir);
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "model.exec" });
    defer a.free(path);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.exec", .data = "x" });
    const limits = memory.Limits{ .host_limit_bytes = 128 * mib, .backend_limit_bytes = mib, .combined_limit_bytes = 128 * mib, .scratch_limit_bytes = mib };
    var context = try ArtifactContext.init(a, std.testing.io, root, .{ .limits = limits });
    defer context.deinit();
    const before = context.manager.currentAdmissionAmounts();
    var input_shape = [_]i64{16};
    var output_shape = [_]i64{16};
    var input_shapes = [_][]i64{&input_shape};
    var output_shapes = [_][]i64{&output_shape};
    var output_ids = [_]u32{1};
    var bindings = [_]compiled_artifact.PjrtInputBindingMeta{.{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 0 }};
    var artifact = compiled_artifact.Manifest{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = root,
        .artifact_path = path,
        .prompt_tokens = 1,
        .seq_len = 1,
        .query_seq_len = 1,
        .attention_mode = "paged_prefill",
        .raw_prompt = true,
        .chat_template_applied = false,
        .pjrt_input_bindings = &bindings,
        .pjrt_input_shapes = &input_shapes,
        .pjrt_output_node_ids = &output_ids,
        .pjrt_output_shapes = &output_shapes,
    };
    artifact.pjrt_memory_plan = .{
        .platform = .cuda,
        .artifact_sha256 = @splat(0),
        .plugin_sha256 = @splat(0),
        .memory = .{ .code = 64 * mib },
    };
    // Even probing CUDA must follow rejection of an impossible footprint.
    try std.testing.expectError(error.ResourceLimitExceeded, ArtifactDeviceAdmission.init(&context.admissionContext(), artifact));
    try std.testing.expectEqualDeep(before, context.manager.currentAdmissionAmounts());
    artifact.pjrt_memory_plan.?.platform = .cpu;
    artifact.pjrt_memory_plan.?.memory = .{ .arguments = 64, .outputs = 64, .temporary = 128, .host_temporary = 256 };
    {
        var admitted = try ArtifactDeviceAdmission.init(&context.admissionContext(), artifact);
        defer admitted.deinit();
        const during = context.manager.currentAdmissionAmounts();
        try std.testing.expectEqual(@as(usize, 0), during.backendTotalBytes());
        try std.testing.expectEqual(before.hostTotalBytes() + 640, during.hostTotalBytes());
        const Stats = @import("pjrt").pjrt.CompiledMemoryStats;
        var stats = std.mem.zeroes(Stats);
        stats.arguments = 64;
        stats.outputs = 64;
        stats.temporary = 128;
        stats.host_temporary = 256;
        try ArtifactDeviceAdmission.verifyLoaded(artifact, stats);
        stats.temporary += 1;
        try std.testing.expectError(error.InvalidArtifact, ArtifactDeviceAdmission.verifyLoaded(artifact, stats));
        try std.testing.expectEqualDeep(during, context.manager.currentAdmissionAmounts());
    }
    try std.testing.expectEqualDeep(before, context.manager.currentAdmissionAmounts());
    artifact.pjrt_memory_plan.?.memory.temporary = 2 * mib;
    try std.testing.expectError(error.ResourceLimitExceeded, ArtifactDeviceAdmission.init(&context.admissionContext(), artifact));
    try std.testing.expectEqualDeep(before, context.manager.currentAdmissionAmounts());
    artifact.pjrt_memory_plan.?.platform = .cuda;
    artifact.pjrt_memory_plan.?.memory = .{};
    const cuda = try ArtifactDeviceAdmission.plannedAmounts(artifact);
    try std.testing.expectEqual(@as(usize, 6 * mib), cuda.backendTotalBytes());
    try std.testing.expectError(error.ResourceLimitExceeded, ArtifactDeviceAdmission.initForCapacity(&context.admissionContext(), artifact, .{ .total_bytes = mib, .unified = false }));
    artifact.pjrt_memory_plan = null;
    try std.testing.expectError(error.UnsupportedPjrtFeature, ArtifactDeviceAdmission.init(&context.admissionContext(), artifact));
    input_shape[0] = -1;
    try std.testing.expectError(error.InvalidArtifactShape, ArtifactDeviceAdmission.bufferAmounts(artifact));
    input_shape[0] = std.math.maxInt(i64);
    try std.testing.expectError(error.ResourceLimitExceeded, ArtifactDeviceAdmission.bufferAmounts(artifact));
    artifact.pjrt_input_shapes = &.{};
    try std.testing.expectError(error.MissingArtifactMetadata, ArtifactDeviceAdmission.bufferAmounts(artifact));
}

fn testPjrtManifest(path: []const u8) compiled_artifact.Manifest {
    return .{
        .kind = "pjrt_executable",
        .backend = "xla",
        .model_dir = "",
        .artifact_path = path,
        .prompt_tokens = 16,
        .seq_len = 16,
        .query_seq_len = 16,
        .attention_mode = "full_recompute",
        .raw_prompt = true,
        .chat_template_applied = false,
    };
}

test "run-artifact host KV exists only during native paged execution" {
    const cfg = @import("models/gpt.zig").Config{ .hidden_size = 64, .num_hidden_layers = 2, .num_attention_heads = 2, .vocab_size = 16, .intermediate_size = 128 };
    var artifact = testPjrtManifest("");
    const full = try artifactHostEstimateForConfig(cfg, .f32, artifact, .execute);
    try std.testing.expectEqual(@as(usize, 0), full.kv_bytes);
    for ([_][]const u8{ "paged_prefill", "paged_decode" }) |mode| {
        artifact.attention_mode = mode;
        const trace = try artifactHostEstimateForConfig(cfg, .f32, artifact, .trace);
        const execute = try artifactHostEstimateForConfig(cfg, .f32, artifact, .execute);
        try std.testing.expectEqual(@as(usize, 0), trace.kv_bytes);
        try std.testing.expect(execute.kv_bytes > 0);
        try std.testing.expectEqual(trace.scratch_bytes, execute.scratch_bytes);
        // Reference execution is the sole CPU KV owner; tracing overlaps
        // without adding another cache or another invocation KV reservation.
        var controller = runtime.tier.memory.AdmissionController{};
        controller.tightenInvocationQuotas(.{ .kv_limit_bytes = execute.kv_bytes });
        var tracing = try controller.tryAcquire(.cpu, .{}, runtime.tier.memory.AdmissionAmounts.fromEstimate(trace), false);
        defer tracing.release();
        var reference = try controller.tryAcquire(.cpu, .{}, .{ .host_kv_bytes = execute.kv_bytes }, false);
        reference.release();
        try std.testing.expectEqual(@as(usize, 0), controller.snapshot().kvTotalBytes());
    }
}

fn preparedArtifactScenario(a: std.mem.Allocator, root: []const u8, artifact: compiled_artifact.Manifest, plugin: [:0]const u8) !void {
    var context = try ArtifactContext.init(a, std.testing.io, root, .{ .limits = .{ .host_limit_bytes = 128 * 1024 * 1024 } });
    defer context.deinit();
    const before = context.manager.currentAdmissionAmounts();
    defer std.debug.assert(std.meta.eql(before, context.manager.currentAdmissionAmounts()));
    {
        var prepared = try PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin);
        defer prepared.deinit();
        const staged = context.manager.currentAdmissionAmounts();
        try std.testing.expect(staged.host_weight_bytes >= before.host_weight_bytes + 2 * prepared.encoded_bytes);
        var client = try prepared.createClient(plugin);
        defer client.deinit();
        try std.testing.expectError(error.OutputCountMismatch, client.deserializeExecutable(prepared.bytes, 2));
        var executable = try client.deserializeExecutable(prepared.bytes, 1);
        defer executable.deinit();
        const stats = try executable.compiledMemoryStats();
        try prepared.loaded(artifact, stats);
        try std.testing.expectEqual(@as(usize, 0), prepared.bytes.len);
        try std.testing.expectEqual(staged.host_weight_bytes - prepared.encoded_bytes, context.manager.currentAdmissionAmounts().host_weight_bytes);
    }
    try std.testing.expectEqualDeep(before, context.manager.currentAdmissionAmounts());
}

test "run-artifact PJRT constructors require admitted verified bytes and refund every allocation failure" {
    if (comptime !build_options.enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeArtifactAdmissionFixture(tmp.dir);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.exec", .data = "fixture executable" });
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "plugin", .data = @import("pjrt_admission_fixture").bytes });
    const plugin = try std.fmt.allocPrintSentinel(a, "{s}/plugin", .{root}, 0);
    defer a.free(plugin);
    var lib = try std.DynLib.open(plugin);
    defer lib.close();
    const reset = lib.lookup(*const fn () callconv(.c) void, "AntflyTestReset").?;
    const count = lib.lookup(*const fn (usize) callconv(.c) usize, "AntflyTestRead").?;
    reset();

    const path = try std.fs.path.join(a, &.{ root, "model.exec" });
    defer a.free(path);
    var artifact = testPjrtManifest(path);
    artifact.model_dir = root;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("fixture executable", &digest, .{});
    artifact.pjrt_memory_plan = .{
        .platform = .cpu,
        .artifact_sha256 = digest,
        .plugin_sha256 = try @import("util/c_file.zig").sha256File(a, plugin),
        .memory = .{ .code = 64, .arguments = 4096, .outputs = 8, .temporary = 128 },
    };
    {
        var context = try ArtifactContext.init(a, std.testing.io, root, .{ .limits = .{ .host_limit_bytes = 26 * 1024 * 1024 } });
        defer context.deinit();
        const before = context.manager.currentAdmissionAmounts();
        const original = artifact.pjrt_memory_plan.?;
        artifact.pjrt_memory_plan = null;
        try std.testing.expectError(error.UnsupportedPjrtFeature, PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin));
        artifact.pjrt_memory_plan = original;
        artifact.pjrt_memory_plan.?.artifact_sha256[0] ^= 1;
        try std.testing.expectError(error.InvalidArtifact, PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin));
        artifact.pjrt_memory_plan = original;
        artifact.pjrt_memory_plan.?.plugin_sha256[0] ^= 1;
        try std.testing.expectError(error.InvalidArtifact, PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin));
        artifact.pjrt_memory_plan = original;
        artifact.pjrt_memory_plan.?.memory.code = 64 * 1024 * 1024;
        try std.testing.expectError(error.ResourceLimitExceeded, PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin));
        artifact.pjrt_memory_plan = original;
        // Sparse oversized serialized input is rejected before reading it.
        const oversized = try tmp.dir.createFile(std.testing.io, "large.exec", .{});
        defer oversized.close(std.testing.io);
        try oversized.setLength(std.testing.io, 32 * 1024 * 1024);
        const large = try std.fs.path.join(a, &.{ root, "large.exec" });
        defer a.free(large);
        artifact.artifact_path = large;
        try std.testing.expectError(error.ResourceLimitExceeded, PreparedPjrtArtifact.init(&context.admissionContext(), artifact, plugin));
        artifact.artifact_path = path;
        try std.testing.expectEqualDeep(before, context.manager.currentAdmissionAmounts());
        try std.testing.expectEqual(@as(usize, 0), count(0));
        try std.testing.expectEqual(@as(usize, 0), count(1));
    }
    try platform.allocator.checkAllAllocationFailures(a, preparedArtifactScenario, .{ root, artifact, plugin });
    try std.testing.expect(count(1) > 0);
    try std.testing.expectEqual(count(1), count(2));
    try std.testing.expectEqual(count(3), count(4));
    try std.testing.expectEqual(count(5), count(6));
}

test "run-artifact trace pools keep geometry without duplicating native KV storage" {
    const a = std.testing.allocator;
    const cfg = gpt_arch.Config{ .family = .llama, .hidden_size = 128, .num_hidden_layers = 4, .num_attention_heads = 4, .num_key_value_heads = 4, .intermediate_size = 256, .vocab_size = 1024 };
    var artifact = testPjrtManifest("");
    artifact.seq_len = 256;
    artifact.query_seq_len = 256;
    for ([_][]const u8{ "full_recompute", "paged_prefill", "paged_decode" }) |mode| {
        artifact.attention_mode = mode;
        artifact.query_seq_len = if (std.mem.eql(u8, mode, "paged_decode")) 1 else artifact.seq_len;
        var manager = runtime.kv.manager.KvManager.init(a);
        defer manager.deinit();
        const traced = try manager.addPool(artifactKvPoolConfig(cfg, .native, .f32, .trace));
        var traced_state = generation.NativeDecodeState.initPaged(a, &manager, traced, null);
        defer traced_state.deinit();
        _ = try buildArtifactDecodeContext(artifact, &traced_state);
        const reference = try manager.addPool(artifactKvPoolConfig(cfg, .native, .f32, .execute));
        var reference_state = generation.NativeDecodeState.initPaged(a, &manager, reference, null);
        defer reference_state.deinit();
        _ = try buildArtifactDecodeContext(artifact, &reference_state);
        var actual: usize = 0;
        for (manager.getPoolMut(traced).?.hostPool().?.blocks.items) |block| {
            try std.testing.expectEqual(@as(usize, 0), block.k_data.len + block.v_data.len);
        }
        for (manager.getPoolMut(reference).?.hostPool().?.blocks.items) |block| actual += block.k_data.len + block.v_data.len;
        const estimated = try artifactHostEstimateForConfig(cfg, .f32, artifact, .execute);
        try std.testing.expect(actual <= estimated.kv_bytes);
        if (std.mem.eql(u8, mode, "full_recompute")) {
            try std.testing.expectEqual(@as(usize, 0), actual);
        } else try std.testing.expect(actual > 0);
    }
}

fn admittedPjrtPackageScenario(a: std.mem.Allocator, root: []const u8, prefill: []const u8, decode: []const u8, plugin: [:0]const u8) !void {
    var context = try ArtifactContext.init(a, std.testing.io, root, .{ .limits = .{ .host_limit_bytes = 128 * 1024 * 1024 } });
    defer context.deinit();
    const before = context.manager.currentAdmissionAmounts();
    defer std.debug.assert(std.meta.eql(before, context.manager.currentAdmissionAmounts()));
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const parameter = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{1}));
    try graph.markOutput(parameter);
    var backend: ops.ComputeBackend = undefined; // the load-only fixture does not execute

    const executor = try graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{decode}, &graph, &backend, &context.admissionContext(), plugin);
    var owned = executor.modelExecutor();
    var owns_executor = true;
    defer if (owns_executor) owned.deinit();
    {
        var single_token = context.admissionContext();
        single_token.decode_steps_limit = 0;
        const prefill_only = try graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{"unused-missing-decode.json"}, &graph, &backend, &single_token, plugin);
        var prefill_only_owner = prefill_only.modelExecutor();
        defer prefill_only_owner.deinit();
        try std.testing.expectEqual(@as(usize, 0), prefill_only.decode_variants.len);
        var prefill_runtime = try prefill_only_owner.createRuntime(a);
        defer prefill_runtime.deinit();
        try std.testing.expect(!prefill_runtime.capabilities().supports_decode);
        try std.testing.expectError(error.UnsupportedDecode, prefill_runtime.decode(a, .{ .token_id = 0, .position = 16 }));
    }
    const before_runtime = context.manager.currentAdmissionAmounts().host_weight_bytes;
    var retained_runtime = try owned.createRuntime(a);
    defer retained_runtime.deinit();
    const runtime_metadata = context.manager.currentAdmissionAmounts().host_weight_bytes - before_runtime;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, owned.createRuntime(a));
    owned.deinit();
    owns_executor = false;
    try std.testing.expect(executor.graph != &graph);
    try std.testing.expectEqual(@as(usize, 4096), (try executor.executor.executable.compiledMemoryStats()).arguments);
    try std.testing.expectEqual(@as(usize, 1), executor.decode_variants.len);
    // Code is retained twice, argument buffers are shared across phases, and
    // both provider-owned serialized representations remain charged.
    try std.testing.expectEqual(before.host_weight_bytes + 2 * 64 + 4096 + 2 * "fixture executable".len + executor.artifact_owner.?.metadata_bytes + executor.metadata_budget.?.live_bytes + runtime_metadata, context.manager.currentAdmissionAmounts().host_weight_bytes);
}

test "run-artifact admitted PJRT packages share phase memory and refund every failed allocation" {
    if (comptime !build_options.enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeArtifactAdmissionFixture(tmp.dir);
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "plugin", .data = @import("pjrt_admission_fixture").bytes });
    const plugin = try std.fmt.allocPrintSentinel(a, "{s}/plugin", .{root}, 0);
    defer a.free(plugin);
    var lib = try std.DynLib.open(plugin);
    defer lib.close();
    const reset = lib.lookup(*const fn () callconv(.c) void, "AntflyTestReset").?;
    const count = lib.lookup(*const fn (usize) callconv(.c) usize, "AntflyTestRead").?;
    reset();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.exec", .data = "fixture executable" });
    const path = try std.fs.path.join(a, &.{ root, "model.exec" });
    defer a.free(path);
    var artifact = testPjrtManifest(path);
    artifact.model_dir = root;
    var shape = [_]i64{1};
    var shapes = [_][]i64{&shape};
    var bindings = [_]compiled_artifact.PjrtInputBindingMeta{.{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 0 }};
    var outputs = [_]u32{0};
    artifact.pjrt_input_shapes = &shapes;
    artifact.pjrt_input_bindings = &bindings;
    artifact.pjrt_output_shapes = &shapes;
    artifact.pjrt_output_node_ids = &outputs;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("fixture executable", &digest, .{});
    artifact.pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = digest, .plugin_sha256 = try @import("util/c_file.zig").sha256File(a, plugin), .memory = .{ .code = 64, .arguments = 4096, .outputs = 8, .temporary = 128 } };
    const prefill = try std.fs.path.join(a, &.{ root, "prefill.inference.json" });
    defer a.free(prefill);
    const decode = try std.fs.path.join(a, &.{ root, "decode.inference.json" });
    defer a.free(decode);
    try compiled_artifact.writeManifest(a, std.testing.io, prefill, artifact);
    artifact.seq_len += 1;
    artifact.query_seq_len = 1;
    artifact.attention_mode = "paged_decode";
    artifact.artifact_role = compiled_artifact.artifact_role_decode;
    const profile = artifact.pjrt_memory_plan;
    artifact.pjrt_memory_plan = null;
    try compiled_artifact.writeManifest(a, std.testing.io, decode, artifact);
    try std.testing.expectError(error.UnsupportedPjrtFeature, admittedPjrtPackageScenario(a, root, prefill, decode, plugin));
    try std.testing.expectEqual(@as(usize, 0), count(0));
    artifact.pjrt_memory_plan = profile;
    artifact.pjrt_memory_plan.?.artifact_sha256[0] ^= 1;
    try compiled_artifact.writeManifest(a, std.testing.io, decode, artifact);
    try std.testing.expectError(error.InvalidArtifact, admittedPjrtPackageScenario(a, root, prefill, decode, plugin));
    try std.testing.expectEqual(count(1), count(2));
    try std.testing.expectEqual(count(3), count(4));
    artifact.pjrt_memory_plan = profile;
    try compiled_artifact.writeManifest(a, std.testing.io, decode, artifact);
    try platform.allocator.checkAllAllocationFailures(a, admittedPjrtPackageScenario, .{ root, prefill, decode, plugin });
    try std.testing.expectEqual(count(1), count(2));
    try std.testing.expectEqual(count(3), count(4));
    try std.testing.expectEqual(count(5), count(6));
}

test "run-artifact reserves host upload and comparison staging outside compiler statistics" {
    const mib: usize = 1024 * 1024;
    var artifact = testPjrtManifest("");
    var shape = [_]i64{8 * mib};
    var shapes = [_][]i64{&shape};
    var bindings = [_]compiled_artifact.PjrtInputBindingMeta{.{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 0 }};
    artifact.pjrt_input_shapes = &shapes;
    artifact.pjrt_input_bindings = &bindings;
    artifact.pjrt_memory_plan = .{ .platform = .cuda, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{ .arguments = 32 * mib } };
    const input = try ArtifactDeviceAdmission.plannedAmounts(artifact);
    try std.testing.expectEqual(32 * mib, input.host_scratch_bytes);
    const a = std.testing.allocator;
    var manager = model_manager_mod.ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    const limits = runtime.tier.memory.Limits{ .host_limit_bytes = 40 * mib, .backend_limit_bytes = 128 * mib, .combined_limit_bytes = 168 * mib, .scratch_limit_bytes = 64 * mib };
    const policy = model_manager_mod.ModelLoadPolicy{ .limits = limits };
    try policy.configure(&manager);
    var resident = try manager.admitHostPreflight(32 * mib);
    defer resident.release();
    const before = manager.currentAdmissionAmounts();
    const context = pjrt_admission.Context{ .allocator = a, .manager = &manager, .policy = policy };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ArtifactDeviceAdmission.initForCapacity(&context, artifact, .{ .total_bytes = 128 * mib, .unified = false }));
    try std.testing.expectEqualDeep(before, manager.currentAdmissionAmounts());
    artifact.pjrt_input_bindings = &.{};
    artifact.pjrt_input_shapes = &.{};
    var outputs = [_]u32{0};
    artifact.pjrt_output_shapes = &shapes;
    artifact.pjrt_output_node_ids = &outputs;
    const output = try ArtifactDeviceAdmission.plannedAmounts(artifact);
    try std.testing.expectEqual(64 * mib, output.host_scratch_bytes);
}

test "run-artifact selection skips HLO packages and unprofiled standalone executables" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    var artifact = testPjrtManifest("model.exec");
    artifact.model_dir = root;
    artifact.attention_mode = "paged_prefill";
    const legacy = try std.fs.path.join(a, &.{ root, "old.inference.json" });
    defer a.free(legacy);
    try compiled_artifact.writeManifest(a, std.testing.io, legacy, artifact);
    artifact.pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{} };
    const fresh = try std.fs.path.join(a, &.{ root, "new.inference.json" });
    defer a.free(fresh);
    try compiled_artifact.writeManifest(a, std.testing.io, fresh, artifact);
    const hlo = try compiled_artifact.packageManifestPath(a, root, "xla", root, "pjrt_hlo", compiled_artifact.pjrt_parameter_mode_embedded);
    defer a.free(hlo);
    try compiled_artifact.writePackageManifest(a, std.testing.io, hlo, .{ .backend = "xla", .model_dir = root, .kind = "pjrt_hlo", .artifacts = &.{.{ .manifest_path = legacy, .artifact_path = "old.hlo", .artifact_role = compiled_artifact.artifact_role_prefill, .seq_len = 16, .query_seq_len = 16, .attention_mode = "paged_prefill" }} });
    try std.testing.expectEqual(null, try compiled_artifact.findManagedPjrtPackage(a, std.testing.io, root, "xla", root, 16, 16, "paged_prefill"));
    var found = (try findMatchingFullModelArtifact(a, std.testing.io, root, "xla", root, 16, 16, "paged_prefill")).?;
    defer found.deinit(a);
    try std.testing.expectEqualStrings(fresh, found.manifest_path);
}

test "run-artifact managed decode runtime survives executor graph and manager teardown" {
    if (comptime !build_options.enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "plugin", .data = @import("pjrt_admission_fixture").bytes });
    const plugin = try std.fmt.allocPrintSentinel(a, "{s}/plugin", .{root}, 0);
    defer a.free(plugin);
    var lib = try std.DynLib.open(plugin);
    defer lib.close();
    const reset = lib.lookup(*const fn () callconv(.c) void, "AntflyTestReset").?;
    const count = lib.lookup(*const fn (usize) callconv(.c) usize, "AntflyTestRead").?;
    reset();
    const prefill = try std.fs.path.join(a, &.{ root, "prefill.inference.json" });
    defer a.free(prefill);
    const decode = try std.fs.path.join(a, &.{ root, "decode.inference.json" });
    defer a.free(decode);
    var artifact = testPjrtManifest("");
    artifact.model_dir = root;
    artifact.attention_mode = "paged_prefill";
    var ids_shape = [_]i64{16};
    var kv_shape = [_]i64{1};
    var logits_shape = [_]i64{2};
    var prefill_shapes = [_][]i64{&ids_shape};
    var decode_shapes = [_][]i64{ &kv_shape, &kv_shape, &kv_shape };
    var output_shapes = [_][]i64{ &logits_shape, &kv_shape, &kv_shape };
    var inputs = [_]compiled_artifact.PjrtInputBindingMeta{.{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 0 }};
    var decode_inputs = [_]compiled_artifact.PjrtInputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_input_ids, .node_id = 0 },
        .{ .kind = compiled_artifact.pjrt_binding_past_key, .node_id = 1, .layer_index = 0, .name = "past.0.key" },
        .{ .kind = compiled_artifact.pjrt_binding_past_value, .node_id = 2, .layer_index = 0, .name = "past.0.value" },
    };
    var outputs = [_]u32{ 0, 1, 2 };
    var output_bindings = [_]compiled_artifact.PjrtOutputBindingMeta{
        .{ .kind = compiled_artifact.pjrt_binding_graph_node, .node_id = 0 },
        .{ .kind = compiled_artifact.pjrt_binding_present_key, .node_id = 1, .layer_index = 0, .name = "present.0.key" },
        .{ .kind = compiled_artifact.pjrt_binding_present_value, .node_id = 2, .layer_index = 0, .name = "present.0.value" },
    };
    artifact.pjrt_input_bindings = &inputs;
    artifact.pjrt_input_shapes = &prefill_shapes;
    artifact.pjrt_output_shapes = &output_shapes;
    artifact.pjrt_output_node_ids = &outputs;
    artifact.pjrt_output_bindings = &output_bindings;
    const plugin_hash = try @import("util/c_file.zig").sha256File(a, plugin);
    for ([_][]const u8{ "fixture paged prefill", "fixture paged decode" }, 0..) |bytes, i| {
        const name = if (i == 0) "prefill.exec" else "decode.exec";
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = bytes });
        const path = try std.fs.path.join(a, &.{ root, name });
        defer a.free(path);
        artifact.artifact_path = path;
        if (i == 1) {
            artifact.seq_len = 17;
            artifact.query_seq_len = 1;
            artifact.artifact_role = compiled_artifact.artifact_role_decode;
            artifact.attention_mode = "paged_decode";
            artifact.pjrt_input_bindings = &decode_inputs;
            artifact.pjrt_input_shapes = &decode_shapes;
            output_bindings[1].name = "decode.present.0.key";
            output_bindings[2].name = "decode.present.0.value";
        }
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        artifact.pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = digest, .plugin_sha256 = plugin_hash, .memory = .{ .code = 64, .arguments = 4096, .outputs = 16, .temporary = 128 } };
        try compiled_artifact.writeManifest(a, std.testing.io, if (i == 0) prefill else decode, artifact);
    }
    var manager = model_manager_mod.ModelManager.init(a, backends.SessionManager.init(a));
    var manager_live = true;
    defer if (manager_live) manager.deinit();
    const policy = model_manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 128 * 1024 * 1024, .kv_limit_bytes = 48 } };
    try policy.configure(&manager);
    var context = pjrt_admission.Context{ .allocator = a, .manager = &manager, .policy = policy, .runtime_capacity = 4, .decode_steps_limit = 1 };
    var graph = ml.graph.Graph.init(a);
    var graph_live = true;
    defer if (graph_live) graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const parameter = try builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{16}));
    try graph.markOutput(parameter);
    var backend: ops.ComputeBackend = undefined; // semantic ABI needs no host weights

    // Capacity must be backed before client creation, not added after buffers
    // already exist. Four runtimes require 64 KV bytes under this 48-byte cap.
    try std.testing.expectError(error.ResourceLimitExceeded, graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{decode}, &graph, &backend, &context, plugin));
    try std.testing.expectEqual(@as(usize, 0), count(0));
    context.runtime_capacity = 3;
    try std.testing.expectError(error.PjrtLoadOnlyArtifactsRequired, graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{}, &graph, &backend, &context, plugin));
    context.decode_steps_limit = 2;
    try std.testing.expectError(error.PjrtLoadOnlyArtifactsRequired, graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{decode}, &graph, &backend, &context, plugin));
    context.decode_steps_limit = 1;
    var gap_manifest = try compiled_artifact.readManifest(a, std.testing.io, decode);
    defer gap_manifest.deinit();
    gap_manifest.value.seq_len = 18;
    try compiled_artifact.writeManifest(a, std.testing.io, decode, gap_manifest.value);
    try std.testing.expectError(error.ArtifactShapeMismatch, graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{decode}, &graph, &backend, &context, plugin));
    gap_manifest.value.seq_len = 17;
    try compiled_artifact.writeManifest(a, std.testing.io, decode, gap_manifest.value);
    try std.testing.expectEqual(@as(usize, 0), count(0));

    const package_path = try std.fs.path.join(a, &.{ root, "model.antfly-inference-package.json" });
    defer a.free(package_path);
    var prefill_manifest = try compiled_artifact.readManifest(a, std.testing.io, prefill);
    defer prefill_manifest.deinit();
    const package_entries = [_]compiled_artifact.PackageArtifactEntry{
        .{ .manifest_path = prefill, .artifact_path = prefill_manifest.value.artifact_path, .artifact_role = "prefill", .seq_len = 16, .query_seq_len = 16, .attention_mode = "paged_prefill" },
        .{ .manifest_path = decode, .artifact_path = gap_manifest.value.artifact_path, .artifact_role = "decode", .seq_len = 17, .query_seq_len = 1, .attention_mode = "paged_decode" },
    };
    var package = compiled_artifact.PackageManifest{ .backend = "xla", .model_dir = root, .kind = "pjrt_executable", .artifacts = package_entries[0..1] };
    try compiled_artifact.writePackageManifest(a, std.testing.io, package_path, package);
    try std.testing.expectError(error.PjrtLoadOnlyArtifactsRequired, graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromPackageManifestPath(a, std.testing.io, package_path, 16, 16, "paged_prefill", &graph, &backend, &context, plugin));
    try std.testing.expectEqual(@as(usize, 0), count(0));
    package.artifacts = &package_entries;
    try compiled_artifact.writePackageManifest(a, std.testing.io, package_path, package);
    try managedPrefillReuseScenario(a, prefill, plugin, count);
    const closed_clients_before = count(2);
    const executions_before = count(10);
    const executor = try graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromPackageManifestPath(a, std.testing.io, package_path, 16, 16, "paged_prefill", &graph, &backend, &context, plugin);
    var owned = executor.modelExecutor();
    var executor_live = true;
    defer if (executor_live) owned.deinit();
    var failing_runtime_allocator = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var allocation_free_runtime = try owned.createRuntime(failing_runtime_allocator.allocator());
    allocation_free_runtime.deinit();
    try std.testing.expect(!failing_runtime_allocator.has_induced_failure);
    const before_runtime = manager.currentAdmissionAmounts().host_weight_bytes;
    var run = try owned.createRuntime(a);
    const runtime_metadata = manager.currentAdmissionAmounts().host_weight_bytes - before_runtime;
    var runtime_live = true;
    defer if (runtime_live) run.deinit();
    const ids: [16]i64 = @splat(1);
    var first = try run.prefill(a, .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
    defer first.deinit(a);
    try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, try first.hostLogits(a));
    {
        var admitted_before = manager.currentAdmissionAmounts();
        try std.testing.expectEqual(@as(usize, 48), admitted_before.host_kv_bytes);
        var other = try owned.createRuntime(a);
        var other_live = true;
        defer if (other_live) other.deinit();
        var third = try owned.createRuntime(a);
        defer third.deinit();
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, owned.createRuntime(a));
        var other_output = try other.prefill(a, .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
        defer other_output.deinit(a);
        var third_output = try third.prefill(a, .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
        defer third_output.deinit(a);
        try std.testing.expectEqual(@as(usize, 6), count(8) - count(9));
        try std.testing.expect((count(8) - count(9)) * 4 <= admitted_before.host_kv_bytes);
        admitted_before.host_weight_bytes += 2 * runtime_metadata;
        try std.testing.expectEqualDeep(admitted_before, manager.currentAdmissionAmounts());
        other.deinit();
        other_live = false;
        var reused = try owned.createRuntime(a);
        reused.deinit();
    }

    owned.deinit();
    executor_live = false;
    graph.deinit();
    graph_live = false;
    manager.deinit();
    manager_live = false;
    try std.testing.expectEqual(closed_clients_before, count(2));
    var second = try run.decode(a, .{ .token_id = 1, .position = 16 });
    defer second.deinit(a);
    try std.testing.expectEqualSlices(f32, &.{ 1, 0 }, try second.hostLogits(a));
    run.deinit();
    runtime_live = false;
    try std.testing.expectEqual(executions_before + 4, count(10));
    try std.testing.expectEqual(count(1), count(2));
    try std.testing.expectEqual(count(3), count(4));
    try std.testing.expectEqual(count(8), count(9));
}

test "run-artifact runtime capacity scales execution memory and shares CPU CUDA code" {
    var artifact = testPjrtManifest("unused.exec");
    artifact.pjrt_memory_plan = .{ .platform = .cpu, .artifact_sha256 = @splat(0), .plugin_sha256 = @splat(0), .memory = .{ .code = 64, .arguments = 4096, .outputs = 8, .temporary = 128 } };
    inline for (.{ .cpu, .cuda }) |platform_kind| {
        artifact.pjrt_memory_plan.?.platform = platform_kind;
        const one = try pjrt_admission.Package.planExecution(&.{ artifact, artifact }, 1024, 1);
        const three = try pjrt_admission.Package.planExecution(&.{ artifact, artifact }, 1024, 3);
        try std.testing.expectEqual(one.cpu, three.cpu);
        if (platform_kind == .cpu) {
            try std.testing.expectEqual(@as(usize, 1024 + 2 * 64 + 3 * 4096), three.amounts.host_weight_bytes);
            try std.testing.expectEqual(3 * one.amounts.host_scratch_bytes, three.amounts.host_scratch_bytes);
            try std.testing.expectEqual(@as(usize, 0), three.external_code);
        } else {
            try std.testing.expectEqual(@as(usize, 1024), three.amounts.host_weight_bytes);
            try std.testing.expectEqual(3 * one.amounts.backend_scratch_bytes, three.amounts.backend_scratch_bytes);
            try std.testing.expectEqual(@as(usize, 2 * 64), three.external_code);
            // Allocator rounding is additional to the independently leased
            // runtime buffers; generated code never consumes their pool cap.
            try std.testing.expect(three.amounts.backendTotalBytes() - three.external_code >= 3 * (4096 + 8 + 128));
        }
    }
    try std.testing.expectError(error.ResourceLimitExceeded, pjrt_admission.Package.planExecution(&.{artifact}, 0, 0));
    try std.testing.expectError(error.Overflow, pjrt_admission.Package.planExecution(&.{artifact}, 0, std.math.maxInt(usize)));
}

fn managedPrefillReuseScenario(a: std.mem.Allocator, prefill: []const u8, plugin: [:0]const u8, count: *const fn (usize) callconv(.c) usize) !void {
    var manager = model_manager_mod.ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    const policy = model_manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 128 * 1024 * 1024, .kv_limit_bytes = 8 } };
    try policy.configure(&manager);
    const context = pjrt_admission.Context{ .allocator = a, .manager = &manager, .policy = policy, .decode_steps_limit = 0 };
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const parameter = try builder.parameter("input_ids", ml.graph.Shape.init(.i64, &.{16}));
    try graph.markOutput(parameter);
    var backend: ops.ComputeBackend = undefined;
    try managedExecutorMetadataScenario(a, prefill, plugin, &graph, &backend);

    const executor = try graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifestPaths(a, std.testing.io, prefill, &.{}, &graph, &backend, &context, plugin);
    var owned = executor.modelExecutor();
    defer owned.deinit();
    try platform.allocator.checkAllAllocationFailures(a, managedPrefillFailureScenario, .{ &owned, count });
    var run = try owned.createRuntime(a);
    defer run.deinit();
    const admitted = manager.currentAdmissionAmounts();
    try std.testing.expectEqual(@as(usize, 8), admitted.host_kv_bytes);
    const ids: [16]i64 = @splat(1);
    for (0..2) |_| {
        var output = try run.prefill(a, .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
        defer output.deinit(a);
        try std.testing.expectEqual(@as(usize, 8), count(11));
        try std.testing.expectEqual(@as(usize, 8), count(12));
    }
    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, run.prefill(failure.allocator(), .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill }));
    try std.testing.expectEqual(@as(usize, 0), count(11));
    var retry = try run.prefill(a, .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
    defer retry.deinit(a);
    try std.testing.expectEqual(@as(usize, 8), count(12));
    try std.testing.expectEqualDeep(admitted, manager.currentAdmissionAmounts());
}

fn managedPrefillFailureScenario(a: std.mem.Allocator, owned: *model_runtime.ModelExecutor, count: *const fn (usize) callconv(.c) usize) !void {
    const live = count(8) - count(9);
    defer std.debug.assert(count(8) - count(9) == live);
    var run = try owned.createRuntime(a);
    defer run.deinit();
    const ids: [16]i64 = @splat(1);
    var output = try run.prefill(a, .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
    defer output.deinit(a);
}

test "run-artifact admits bootstrap package and sidecar JSON before model lookup" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(directory);
    const padding = try a.alloc(u8, 2 * 1024 * 1024);
    defer a.free(padding);
    @memset(padding, ' ');
    // Valid JSON padding would previously be read and parsed before any model
    // manager or invocation ledger existed. A denied read must win over the
    // missing model/manifest fields, with every bootstrap allocation refunded.
    @memcpy(padding[0..2], "{}");
    for ([_][]const u8{ "artifact.inference.json", "artifact.antfly-inference-package.json" }) |name| {
        const path = try std.fs.path.join(a, &.{ directory, name });
        defer a.free(path);
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = padding });
        const policy = model_manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 1024 * 1024, .combined_limit_bytes = 1024 * 1024 } };
        try std.testing.expectError(error.ResourceLimitExceeded, runArtifactPrompt(a, io, path, "a", false, true, true, policy));
    }
}

fn managedExecutorMetadataScenario(a: std.mem.Allocator, prefill: []const u8, plugin: [:0]const u8, graph: *const ml.graph.Graph, backend: *const ops.ComputeBackend) !void {
    var parsed = try compiled_artifact.readManifest(a, std.testing.io, prefill);
    defer parsed.deinit();
    const large_name = try a.alloc(u8, 16 * 1024 * 1024);
    defer a.free(large_name);
    @memset(large_name, 'k');
    for (parsed.value.pjrt_output_bindings) |*binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_present_key)) {
            binding.name = large_name;
            break;
        }
    }
    var quota = platform.allocator.reclamation.Quota.init(a, 64 * 1024 * 1024);
    defer quota.deinit();
    {
        var manager = model_manager_mod.ModelManager.init(a, backends.SessionManager.init(a));
        defer manager.deinit();
        const policy = model_manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 8 * 1024 * 1024 } };
        try policy.configure(&manager);
        const context = pjrt_admission.Context{ .allocator = a, .manager = &manager, .policy = policy, .decode_steps_limit = 0 };
        const before = manager.currentAdmissionAmounts();
        try std.testing.expectError(error.ResourceLimitExceeded, graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifests(quota.allocator(), &.{parsed.value}, graph, backend, &context, plugin));
        try std.testing.expectEqualDeep(before, manager.currentAdmissionAmounts());
        try std.testing.expectEqual(@as(usize, 0), quota.live_bytes);
        try std.testing.expect(quota.peak_bytes < large_name.len);
    }
    // With enough capacity, every retained executor allocation remains charged
    // after loading and after the manager and source metadata have gone away.
    var manager = model_manager_mod.ModelManager.init(a, backends.SessionManager.init(a));
    var manager_live = true;
    defer if (manager_live) manager.deinit();
    const policy = model_manager_mod.ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 24 * 1024 * 1024 } };
    try policy.configure(&manager);
    const context = pjrt_admission.Context{ .allocator = a, .manager = &manager, .policy = policy, .decode_steps_limit = 0 };
    const executor = try graph_mod.pjrt_artifact_executor.createAdmittedModelExecutorFromManifests(quota.allocator(), &.{parsed.value}, graph, backend, &context, plugin);
    var owned = executor.modelExecutor();
    defer owned.deinit();
    try std.testing.expect(quota.live_bytes > large_name.len);
    try std.testing.expect(manager.currentAdmissionAmounts().hostTotalBytes() >= quota.live_bytes);
    try std.testing.expect(executor.metadata_budget.?.capacity == null);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, graph_mod.pjrt_artifact_executor.createModelExecutorFromAdmittedArtifactBytes(quota.allocator(), std.testing.io, parsed.value, "fixture paged prefill", graph, backend, &executor.artifact_owner.?.client, &context));
    for (parsed.value.pjrt_output_bindings) |*binding| {
        if (std.mem.eql(u8, binding.kind, compiled_artifact.pjrt_binding_present_key)) binding.name = "explicit.present.key";
    }
    {
        const explicit = try graph_mod.pjrt_artifact_executor.createModelExecutorFromAdmittedArtifactBytes(quota.allocator(), std.testing.io, parsed.value, "fixture paged prefill", graph, backend, &executor.artifact_owner.?.client, &context);
        var explicit_owner = explicit.modelExecutor();
        defer explicit_owner.deinit();
        var no_allocations = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        var explicit_run = try explicit_owner.createRuntime(no_allocations.allocator());
        defer explicit_run.deinit();
        try std.testing.expect(!no_allocations.has_induced_failure);
        const ids: [16]i64 = @splat(1);
        var output = try explicit_run.prefill(quota.allocator(), .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
        defer output.deinit(quota.allocator());
        try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, try output.hostLogits(quota.allocator()));
    }
    manager.deinit();
    manager_live = false;
    var run = try owned.createRuntime(quota.allocator());
    defer run.deinit();
    const controller = &executor.metadata_budget.?.lifetime.domain.admission;
    const admitted = controller.snapshot();
    const retained = quota.live_bytes;
    try std.testing.expect(admitted.hostTotalBytes() >= retained);
    const ids: [16]i64 = @splat(1);
    for (0..3) |_| {
        var output = try run.prefill(quota.allocator(), .{ .input_ids = &ids, .seq_len = 16, .query_seq_len = 16, .attention_mode = .paged_prefill });
        output.deinit(quota.allocator());
        try std.testing.expectEqual(retained, quota.live_bytes);
        try std.testing.expectEqualDeep(admitted, controller.snapshot());
    }
    // An admitted immutable name is shared, not copied for each forward pass.
    try std.testing.expect(quota.peak_bytes < 24 * 1024 * 1024);
}
