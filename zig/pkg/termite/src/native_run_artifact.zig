// Copyright 2026 Antfly, Inc.
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
const tokenizer_mod = @import("termite_tokenizer");
const hf_tokenizer = @import("termite_hf_tokenizer");
const sentencepiece = @import("termite_tokenizer").sentencepiece;
const backends = @import("backends/backends.zig");
const activations = @import("backends/activations.zig");
const OnnxSessionOptions = if (build_options.enable_onnx) backends.onnx.SessionOptions else struct {
    low_memory: bool = false,
};
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
    return platform.env.getenvBool("TERMITE_PJRT_EXEC_DEBUG");
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

const LoadedTokenizerAssets = struct {
    allocator: std.mem.Allocator,
    hf_tok: ?*hf_tokenizer.HfTokenizer = null,
    sp_tok: ?*sentencepiece.Processor = null,
    chat_tmpl: ?*generation.ChatTemplate = null,

    fn tokenizer(self: *const @This()) tokenizer_mod.Tokenizer {
        if (self.hf_tok) |tok| return tok.tokenizer();
        if (self.sp_tok) |tok| return tok.tokenizer();
        unreachable;
    }

    fn deinit(self: *@This()) void {
        if (self.hf_tok) |tok| {
            tok.deinitSelf();
        }
        if (self.sp_tok) |tok| {
            tok.deinit();
            self.allocator.destroy(tok);
        }
        if (self.chat_tmpl) |ct| {
            ct.deinit();
            self.allocator.destroy(ct);
        }
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

    if (std.mem.eql(u8, manifest.backend, "onnx")) {
        if (!build_options.enable_onnx) return error.BackendUnavailable;
        var session = try backends.onnx.createSession(allocator, manifest.artifact_path);
        defer session.close();
        const input_materialization = graph_mod.onnx_artifact_executor.detectInputMaterialization(
            .host_assisted_explicit_kv,
            session.inputInfo(),
            session.outputInfo(),
        );
        return .{
            .backend = try allocator.dupe(u8, manifest.backend),
            .kind = try allocator.dupe(u8, manifest.kind),
            .model_dir = try allocator.dupe(u8, manifest.model_dir),
            .artifact_path = try allocator.dupe(u8, manifest.artifact_path),
            .inputs = session.inputInfo().len,
            .outputs = session.outputInfo().len,
            .seq_len = manifest.seq_len,
            .query_seq_len = manifest.query_seq_len,
            .attention_mode = try allocator.dupe(u8, manifest.attention_mode),
            .runtime_state_ownership = try allocator.dupe(u8, graph_mod.onnx_artifact_executor.stateOwnershipName(input_materialization)),
            .supports_decode = input_materialization == .runtime_owned_host_cache or input_materialization == .backend_owned_kv,
        };
    }

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
) !RunResult {
    if (compiled_artifact.isPackageManifestPath(artifact_or_manifest_path)) {
        const manifest_path = try resolvePackagePrefillManifestPath(
            allocator,
            io,
            artifact_or_manifest_path,
            prompt,
            no_chat_template,
            raw_prompt,
        );
        defer allocator.free(manifest_path);

        var parsed = try compiled_artifact.readManifest(allocator, io, manifest_path);
        defer parsed.deinit();
        return runParsedArtifactPrompt(
            allocator,
            io,
            parsed.value,
            manifest_path,
            prompt,
            compare_host,
            no_chat_template,
            raw_prompt,
        );
    }

    const manifest_path = try compiled_artifact.resolveManifestPath(allocator, artifact_or_manifest_path);
    errdefer allocator.free(manifest_path);

    var parsed = try compiled_artifact.readManifest(allocator, io, manifest_path);
    defer parsed.deinit();
    return runParsedArtifactPrompt(
        allocator,
        io,
        parsed.value,
        manifest_path,
        prompt,
        compare_host,
        no_chat_template,
        raw_prompt,
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
) !?RunResult {
    var model_manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    errdefer model_manifest.deinit();

    var assets = try loadTokenizerAssets(allocator, model_dir, &model_manifest);
    errdefer assets.deinit();

    const apply_chat_template = !raw_prompt and !no_chat_template and assets.chat_tmpl != null;
    const rendered_prompt = if (raw_prompt)
        try allocator.dupe(u8, prompt)
    else if (apply_chat_template)
        try assets.chat_tmpl.?.apply(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }}, true)
    else
        try generation.formatMessages(allocator, &[_]generation.Message{.{ .role = "user", .content = prompt }});
    errdefer allocator.free(rendered_prompt);

    var encoded = try generation.encodePromptForGeneration(
        assets.tokenizer(),
        allocator,
        rendered_prompt,
        4096,
        model_manifest.add_bos_token,
        model_manifest.bos_token,
    );
    errdefer encoded.deinit();

    const prompt_tokens = countPromptTokens(encoded.attention_mask);
    if (prompt_tokens == 0) {
        encoded.deinit();
        allocator.free(rendered_prompt);
        assets.deinit();
        model_manifest.deinit();
        return null;
    }

    if (try findMatchingFullModelPackageManifest(
        allocator,
        io,
        search_dir,
        backend,
        model_dir,
        prompt_tokens,
        prompt_tokens,
        "paged_prefill",
    )) |package_manifest_path| {
        defer allocator.free(package_manifest_path);
        return try runArtifactPrompt(
            allocator,
            io,
            package_manifest_path,
            prompt,
            false,
            no_chat_template,
            raw_prompt,
        );
    }

    var found = (try findMatchingFullModelArtifact(
        allocator,
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
        encoded.deinit();
        allocator.free(rendered_prompt);
        assets.deinit();
        model_manifest.deinit();
        return null;
    };
    errdefer found.deinit(allocator);

    var parsed = try compiled_artifact.readManifest(allocator, io, found.manifest_path);
    errdefer parsed.deinit();

    const artifact = parsed.value;
    if (artifact.seq_len != prompt_tokens or artifact.query_seq_len != prompt_tokens) {
        parsed.deinit();
        found.deinit(allocator);
        encoded.deinit();
        allocator.free(rendered_prompt);
        assets.deinit();
        model_manifest.deinit();
        return null;
    }
    return try runArtifactPrompt(
        allocator,
        io,
        found.manifest_path,
        prompt,
        false,
        no_chat_template,
        raw_prompt,
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
    else if (std.mem.eql(u8, backend, "onnx"))
        &.{
            .{ .kind = "onnx_graph", .parameter_mode = null },
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
    if (std.mem.eql(u8, backend, "xla")) {
        const xla_kinds = [_][]const u8{ "pjrt_executable", "pjrt_hlo" };
        for (xla_kinds) |kind| {
            if (try compiled_artifact.findMatchingArtifactPath(allocator, io, search_dir, .{
                .backend = backend,
                .kind = kind,
                .model_dir = model_dir,
                .seq_len = seq_len,
                .query_seq_len = query_seq_len,
                .attention_mode = attention_mode,
            })) |found| return found;
        }
        return null;
    }

    const kind = if (std.mem.eql(u8, backend, "onnx"))
        "onnx_graph"
    else
        return null;
    return compiled_artifact.findMatchingArtifactPath(allocator, io, search_dir, .{
        .backend = backend,
        .kind = kind,
        .model_dir = model_dir,
        .seq_len = seq_len,
        .query_seq_len = query_seq_len,
        .attention_mode = attention_mode,
    });
}

fn resolvePackagePrefillManifestPath(
    allocator: std.mem.Allocator,
    io: std.Io,
    package_manifest_path: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
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
        parsed.value.model_dir,
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
    model_dir: []const u8,
    prompt: []const u8,
    no_chat_template: bool,
    raw_prompt: bool,
) !usize {
    var model_manifest = try manifest_mod.loadFromDir(allocator, model_dir);
    defer model_manifest.deinit();

    var assets = try loadTokenizerAssets(allocator, model_dir, &model_manifest);
    defer assets.deinit();

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
        if (!std.mem.endsWith(u8, entry.name, ".termite.json")) continue;

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
        if (!(std.mem.eql(u8, manifest.kind, "onnx_graph") or
            std.mem.eql(u8, manifest.kind, "pjrt_hlo") or
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
) !RunResult {
    var model_manifest = try manifest_mod.loadFromDir(allocator, artifact.model_dir);
    defer {
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact model manifest deinit begin", .{});
        model_manifest.deinit();
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact model manifest deinit complete", .{});
    }

    var assets = try loadTokenizerAssets(allocator, artifact.model_dir, &model_manifest);
    defer {
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact tokenizer assets deinit begin", .{});
        assets.deinit();
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) std.log.info("PJRT run-artifact tokenizer assets deinit complete", .{});
    }

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
    );
}

fn runPreparedArtifactPrompt(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    manifest_path: []const u8,
    tokenizer: tokenizer_mod.Tokenizer,
    graph_input_ids: []const i32,
    compare_host: bool,
) !RunResult {
    if (std.mem.eql(u8, artifact.kind, "onnx_graph") or
        std.mem.eql(u8, artifact.kind, "pjrt_hlo") or
        std.mem.eql(u8, artifact.kind, "pjrt_executable"))
    {
        const full_result = if (std.mem.eql(u8, artifact.backend, "onnx"))
            if (compare_host)
                try runOnnxArtifactFull(allocator, artifact, graph_input_ids, tokenizer.vocabSize(), true)
            else
                try runOnnxArtifactFullViaModelExecutor(allocator, io, manifest_path, artifact, graph_input_ids, tokenizer.vocabSize())
        else if (std.mem.eql(u8, artifact.backend, "xla"))
            try runPjrtHloArtifactFull(allocator, io, artifact, graph_input_ids, compare_host)
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
        if (std.mem.eql(u8, artifact.backend, "onnx") and !compare_host) {
            yieldAfterShortLivedOnnxSessionClose();
        } else if (std.mem.eql(u8, artifact.backend, "xla")) {
            if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact post-run yield begin", .{});
            yieldAfterShortLivedPjrtSessionClose();
            if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact post-run yield complete", .{});
        }
        if (pjrtExecDebugEnabled() and std.mem.eql(u8, artifact.backend, "xla")) {
            std.log.info("PJRT run-artifact returning RunResult", .{});
        }
        return result;
    }

    if (std.mem.eql(u8, artifact.kind, "onnx_partition_graph")) {
        var partition_result = try runOnnxPartitionArtifact(
            allocator,
            artifact,
            graph_input_ids,
            compare_host,
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

    if (std.mem.eql(u8, artifact.kind, "pjrt_partition_hlo") or
        std.mem.eql(u8, artifact.kind, "pjrt_partition_executable"))
    {
        var partition_result = try runPjrtPartitionArtifact(
            allocator,
            io,
            artifact,
            graph_input_ids,
            compare_host,
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

    fn deinit(self: @This(), allocator: std.mem.Allocator) void {
        if (self.compare_summary.len > 0) allocator.free(self.compare_summary);
    }
};

fn modelRuntimeAttentionModeFromArtifact(artifact: compiled_artifact.Manifest) !ModelAttentionMode {
    if (std.mem.eql(u8, artifact.attention_mode, "full_recompute")) return .full_recompute;
    if (std.mem.eql(u8, artifact.attention_mode, "paged_prefill")) return .paged_prefill;
    if (std.mem.eql(u8, artifact.attention_mode, "paged_decode")) return .paged_decode;
    return error.UnsupportedArtifactKind;
}

fn runOnnxArtifactFullViaModelExecutor(
    allocator: std.mem.Allocator,
    io: std.Io,
    manifest_path: []const u8,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    fallback_vocab_size: usize,
) !FullModelRunResult {
    if (comptime !build_options.enable_onnx) return error.BackendUnavailable;

    var executor_ctx = try graph_mod.onnx_artifact_executor.createModelExecutorFromManifestPath(
        allocator,
        io,
        manifest_path,
        fallback_vocab_size,
    );
    var executor = executor_ctx.modelExecutor();
    defer executor.deinit();

    var runtime_value = try executor.createRuntime(allocator);
    defer runtime_value.deinit();
    const runtime_caps = runtime_value.capabilities();
    if (runtime_caps.supports_decode) {
        return error.UnsupportedRuntimeCapabilities;
    }

    const input_ids = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(input_ids);
    for (graph_input_ids, 0..) |id, i| input_ids[i] = id;

    var output = try runtime_value.prefill(allocator, .{
        .input_ids = input_ids,
        .seq_len = artifact.seq_len,
        .query_seq_len = artifact.query_seq_len,
        .attention_mode = try modelRuntimeAttentionModeFromArtifact(artifact),
    });
    defer output.deinit(allocator);
    const token_id = activations.argmax(try output.hostLogits(allocator));

    return .{
        .token_id = token_id,
        .compare_summary = &.{},
    };
}

fn runPjrtHloArtifactFull(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    compare_host: bool,
) !FullModelRunResult {
    if (comptime !build_options.enable_pjrt) return error.BackendUnavailable;
    if (artifact.pjrt_input_bindings.len == 0 or artifact.pjrt_output_node_ids.len == 0) return error.MissingArtifactMetadata;

    const plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator) orelse return error.MissingPjrtPlugin;
    defer allocator.free(plugin_path);

    const pjrt_lib = @import("pjrt");
    var client = try pjrt_lib.pjrt.Client.init(plugin_path);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact client deinit begin", .{});
        client.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact client deinit complete", .{});
    }

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    session_manager.preferred_backends = &.{backends.BackendType.native};
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact model manager deinit begin", .{});
        model_manager.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact model manager deinit complete", .{});
    }

    const model = try model_manager.loadFromDirWithPreferredBackends(
        artifact.model_dir,
        &.{backends.BackendType.native},
        true,
    );
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;

    const input_ids = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(input_ids);
    for (graph_input_ids, 0..) |id, i| input_ids[i] = id;

    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer {
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact host backend deinit begin", .{});
        cb.deinit();
        if (pjrtExecDebugEnabled()) std.log.info("PJRT run-artifact host backend deinit complete", .{});
    }

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
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;
    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

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

    var host_last_logits: ?[]f32 = null;
    defer if (host_last_logits) |logits| allocator.free(logits);
    if (compare_host) {
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

    var model_executor_ctx = try graph_mod.pjrt_artifact_executor.createModelExecutorFromArtifact(
        allocator,
        io,
        artifact,
        &entry.graph,
        &cb,
        &client,
    );
    var model_executor = model_executor_ctx.modelExecutor();
    defer model_executor.deinit();

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

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        for (self.items) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
        allocator.free(self.items);
        self.* = undefined;
    }
};

fn cloneTensorInfoList(
    allocator: std.mem.Allocator,
    infos: []const backends.TensorInfo,
) !OwnedTensorInfoList {
    const out = try allocator.alloc(backends.TensorInfo, infos.len);
    errdefer allocator.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
    }
    for (infos, 0..) |info, i| {
        const owned_name = try allocator.dupe(u8, info.name);
        const owned_shape = allocator.dupe(i64, info.shape) catch |err| {
            allocator.free(owned_name);
            return err;
        };
        out[i] = .{
            .name = owned_name,
            .dtype = info.dtype,
            .shape = owned_shape,
        };
        initialized += 1;
    }
    return .{ .items = out };
}

fn deinitTensorSlice(allocator: std.mem.Allocator, tensors: []backends.Tensor) void {
    for (tensors) |*tensor| tensor.deinit();
    allocator.free(tensors);
}

fn onnxArtifactSessionOptions(
    artifact: compiled_artifact.Manifest,
    compare_host: bool,
) OnnxSessionOptions {
    const debug_outputs_enabled = artifact.onnx_output_node_ids.len > 1;
    return if (compare_host or debug_outputs_enabled or artifact.onnx_input_node_ids.len > 0)
        .{ .low_memory = true }
    else
        .{};
}

fn runOnnxArtifactLastLogits(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    fallback_vocab_size: usize,
    query_seq_len: usize,
) ![]f32 {
    if (!build_options.enable_onnx) return error.BackendUnavailable;
    const session_options = onnxArtifactSessionOptions(artifact, false);
    var session = try backends.onnx.createSessionWithOptions(allocator, artifact.artifact_path, session_options);
    defer session.close();
    const last_logits = try runOnnxArtifactLastLogitsWithSession(
        allocator,
        artifact,
        session,
        graph_input_ids,
        fallback_vocab_size,
        query_seq_len,
    );
    yieldAfterShortLivedOnnxSessionClose();
    return last_logits;
}

fn runOnnxArtifactLastLogitsWithSession(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    session: backends.Session,
    graph_input_ids: []const i32,
    fallback_vocab_size: usize,
    query_seq_len: usize,
) ![]f32 {
    const input_info = session.inputInfo();
    if (input_info.len == 0) return error.UnsupportedArtifactInputs;
    const ids_i64 = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(ids_i64);
    for (graph_input_ids, 0..) |id, i| ids_i64[i] = id;

    const full_inputs = try buildOnnxFullInputs(
        allocator,
        artifact,
        input_info,
        graph_input_ids,
        ids_i64,
    );
    var full_inputs_owned = true;
    errdefer if (full_inputs_owned) deinitTensorSlice(allocator, full_inputs);

    const outputs = try session.run(full_inputs, allocator);
    var outputs_owned = true;
    errdefer if (outputs_owned) deinitTensorSlice(allocator, outputs);
    deinitTensorSlice(allocator, full_inputs);
    full_inputs_owned = false;

    if (outputs.len == 0) return error.MissingValue;
    if (outputs[0].dtype != .f32) return error.InvalidArtifactOutput;
    var copied_logits: ?[]f32 = null;
    defer if (copied_logits) |buf| allocator.free(buf);
    const logits = if (outputs[0].asFloat32IfAligned()) |aligned|
        aligned
    else blk: {
        copied_logits = try allocator.alloc(f32, outputs[0].data.len / @sizeOf(f32));
        @memcpy(std.mem.sliceAsBytes(copied_logits.?), outputs[0].data);
        break :blk copied_logits.?;
    };
    const output_logit_width = try inferOnnxOutputLogitWidth(
        outputs[0].shape,
        logits.len,
        query_seq_len,
        fallback_vocab_size,
    );
    const last_logits = try allocator.dupe(f32, logits[logits.len - output_logit_width ..]);
    deinitTensorSlice(allocator, outputs);
    outputs_owned = false;
    return last_logits;
}

fn yieldAfterShortLivedOnnxSessionClose() void {
    // ORT can still be tearing down worker state after a short-lived session
    // close. Yield briefly before the CLI returns to avoid a native fast-exit
    // teardown race in smoke-sized artifact runs.
    platform.time.sleepNs(10_000_000);
}

fn yieldAfterShortLivedPjrtSessionClose() void {
    // The CPU PJRT plugin can still be quiescing runtime/ABSL state after a
    // short compile-load-execute-destroy cycle. Match the ORT CLI guard so
    // smoke-sized artifact runs do not abort during immediate process exit.
    platform.time.sleepNs(250_000_000);
}

fn runOnnxArtifactFull(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    fallback_vocab_size: usize,
    compare_host: bool,
) !FullModelRunResult {
    if (!build_options.enable_onnx) return error.BackendUnavailable;
    const debug_outputs_enabled = artifact.onnx_output_node_ids.len > 1;
    if (compare_host and debug_outputs_enabled and !allowFullOnnxDebugCompare()) {
        std.log.err(
            "refusing full-graph ONNX debug compare because ORT can still materialize the full model and exceed memory limits; set TERMITE_ALLOW_FULL_ONNX_DEBUG_COMPARE=1 to override",
            .{},
        );
        return error.UnsupportedArtifactOutput;
    }
    const session_options = onnxArtifactSessionOptions(artifact, compare_host);

    var metadata_session = try backends.onnx.createSessionWithOptions(allocator, artifact.artifact_path, session_options);
    var metadata_session_open = true;
    defer if (metadata_session_open) metadata_session.close();
    var input_info = try cloneTensorInfoList(allocator, metadata_session.inputInfo());
    defer input_info.deinit(allocator);
    var output_info = try cloneTensorInfoList(allocator, metadata_session.outputInfo());
    defer output_info.deinit(allocator);
    metadata_session.close();
    metadata_session_open = false;
    if (input_info.items.len == 0) return error.UnsupportedArtifactInputs;

    const ids_i64 = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(ids_i64);
    for (graph_input_ids, 0..) |id, i| ids_i64[i] = id;

    const full_inputs = try buildOnnxFullInputs(
        allocator,
        artifact,
        input_info.items,
        graph_input_ids,
        ids_i64,
    );
    var full_inputs_owned = true;
    errdefer if (full_inputs_owned) deinitTensorSlice(allocator, full_inputs);

    const reordered_output_node_ids = if (artifact.onnx_output_node_ids.len == output_info.items.len) blk: {
        break :blk if (artifact.onnx_output_names.len == output_info.items.len)
            try reorderOnnxNodeIdsByName(allocator, artifact.onnx_output_node_ids, artifact.onnx_output_names, output_info.items)
        else
            try allocator.dupe(u32, artifact.onnx_output_node_ids);
    } else null;
    defer if (reordered_output_node_ids) |ids| allocator.free(ids);

    const output_mapping = if (reordered_output_node_ids) |ids|
        try summarizeOnnxOutputMapping(allocator, output_info.items, ids)
    else
        try allocator.dupe(u8, "");
    defer allocator.free(output_mapping);

    var session = try backends.onnx.createSessionWithOptions(allocator, artifact.artifact_path, session_options);
    var session_open = true;
    defer if (session_open) session.close();

    const outputs = try session.run(full_inputs, allocator);
    var outputs_owned = true;
    errdefer if (outputs_owned) deinitTensorSlice(allocator, outputs);
    deinitTensorSlice(allocator, full_inputs);
    full_inputs_owned = false;
    session.close();
    session_open = false;

    if (outputs.len == 0) return error.MissingValue;
    if (outputs[0].dtype != .f32) return error.InvalidArtifactOutput;
    var copied_logits: ?[]f32 = null;
    defer if (copied_logits) |buf| allocator.free(buf);
    const logits = if (outputs[0].asFloat32IfAligned()) |aligned|
        aligned
    else blk: {
        copied_logits = try allocator.alloc(f32, outputs[0].data.len / @sizeOf(f32));
        @memcpy(std.mem.sliceAsBytes(copied_logits.?), outputs[0].data);
        break :blk copied_logits.?;
    };
    const output_logit_width = try inferOnnxOutputLogitWidth(
        outputs[0].shape,
        logits.len,
        artifact.query_seq_len,
        fallback_vocab_size,
    );

    if (compare_host) {
        if (reordered_output_node_ids) |output_node_ids| {
            const token_id = activations.argmax(logits[logits.len - output_logit_width ..]);
            const compare_summary = try compareFullOnnxOutputsWithCapturedNative(
                allocator,
                artifact,
                graph_input_ids,
                logits[logits.len - output_logit_width ..],
                output_mapping,
                output_node_ids,
                outputs,
            );
            deinitTensorSlice(allocator, outputs);
            outputs_owned = false;
            return .{
                .token_id = token_id,
                .compare_summary = compare_summary,
            };
        }

        const last_logits = logits[logits.len - output_logit_width ..];
        const last_logits_copy = try allocator.dupe(f32, last_logits);
        defer allocator.free(last_logits_copy);
        const token_id = activations.argmax(last_logits_copy);
        deinitTensorSlice(allocator, outputs);
        outputs_owned = false;
        const compare_summary = try compareFullModelArtifactOutputs(allocator, artifact, graph_input_ids, last_logits_copy);
        errdefer allocator.free(compare_summary);
        return .{
            .token_id = token_id,
            .compare_summary = compare_summary,
        };
    }

    defer {
        deinitTensorSlice(allocator, outputs);
        outputs_owned = false;
    }
    return finalizeFullModelRunResult(allocator, artifact, graph_input_ids, logits, false);
}

fn allowFullOnnxDebugCompare() bool {
    return platform.env.getenvBool("TERMITE_ALLOW_FULL_ONNX_DEBUG_COMPARE");
}

fn inferOnnxOutputLogitWidth(
    output_shape: []const i64,
    logits_len: usize,
    query_seq_len: usize,
    fallback_vocab_size: usize,
) !usize {
    if (output_shape.len > 0 and output_shape[output_shape.len - 1] > 0) {
        const width: usize = @intCast(output_shape[output_shape.len - 1]);
        if (width > 0 and logits_len >= width) return width;
    }
    if (query_seq_len > 0) {
        const width = logits_len / query_seq_len;
        if (width > 0 and logits_len >= width) return width;
    }
    if (fallback_vocab_size > 0 and logits_len >= fallback_vocab_size) return fallback_vocab_size;
    return error.InvalidArtifactOutput;
}

fn buildOnnxFullInputs(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    input_info: []const backends.TensorInfo,
    graph_input_ids: []const i32,
    ids_i64: []const i64,
) ![]backends.Tensor {
    if (input_info.len == 1 and artifact.onnx_input_node_ids.len == 0) {
        const inputs = try allocator.alloc(backends.Tensor, 1);
        errdefer allocator.free(inputs);
        inputs[0] = switch (input_info[0].dtype) {
            .i64 => try backends.Tensor.initInt64(allocator, input_info[0].name, &.{@intCast(graph_input_ids.len)}, ids_i64),
            else => return error.UnsupportedArtifactInputs,
        };
        return inputs;
    }
    if (artifact.onnx_input_node_ids.len != input_info.len) return error.UnsupportedArtifactInputs;

    const reordered_input_node_ids = if (artifact.onnx_input_names.len == input_info.len)
        try reorderOnnxNodeIdsByName(allocator, artifact.onnx_input_node_ids, artifact.onnx_input_names, input_info)
    else
        try allocator.dupe(u32, artifact.onnx_input_node_ids);
    defer allocator.free(reordered_input_node_ids);

    var materialized: ?MaterializedPartitionInputs = null;
    defer if (materialized) |*m| m.deinit(allocator);
    if (onnxFullInputsNeedMaterializedValues(artifact.attention_bindings)) {
        const capture_node_ids = try collectOnnxFullInputCaptureNodeIds(
            allocator,
            reordered_input_node_ids,
            artifact.attention_bindings,
        );
        defer allocator.free(capture_node_ids);
        materialized = try materializePartitionInputs(
            allocator,
            artifact,
            graph_input_ids,
            capture_node_ids,
        );
    }

    const inputs = try allocator.alloc(backends.Tensor, input_info.len);
    errdefer allocator.free(inputs);
    var initialized: usize = 0;
    errdefer {
        for (inputs[0..initialized]) |*tensor| tensor.deinit();
    }
    for (input_info, reordered_input_node_ids, 0..) |info, node_id_u32, i| {
        if (materialized) |*m| {
            if (findCapturedNodeIndex(m.node_ids, @intCast(node_id_u32))) |capture_idx| {
                inputs[i] = try tensorFromCapturedValue(allocator, info.name, info.shape, info.dtype, m.values[capture_idx], &m.cb);
                initialized += 1;
                continue;
            }
        }
        inputs[i] = try tensorFromGraphInputIds(allocator, info.name, info.shape, info.dtype, graph_input_ids);
        initialized += 1;
    }
    return inputs;
}

fn onnxFullInputsNeedMaterializedValues(attention_bindings: []const compiled_artifact.AttentionBindingMeta) bool {
    for (attention_bindings) |binding| {
        if (binding.skip_kv_write) return true;
    }
    return false;
}

fn collectOnnxFullInputCaptureNodeIds(
    allocator: std.mem.Allocator,
    input_node_ids: []const u32,
    attention_bindings: []const compiled_artifact.AttentionBindingMeta,
) ![]ml.graph.NodeId {
    var out = std.ArrayListUnmanaged(ml.graph.NodeId).empty;
    defer out.deinit(allocator);
    for (input_node_ids) |node_id| {
        if (node_id == std.math.maxInt(u32)) continue;
        try appendUniqueCaptureNodeId(allocator, &out, @intCast(node_id));
    }
    for (attention_bindings) |binding| {
        if (!binding.skip_kv_write) continue;
        try appendUniqueCaptureNodeId(allocator, &out, @intCast(binding.node_id));
    }
    return out.toOwnedSlice(allocator);
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

fn finalizeFullModelRunResult(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    logits: []const f32,
    compare_host: bool,
) !FullModelRunResult {
    const vocab_size = switch (artifact.query_seq_len) {
        0 => return error.InvalidArtifactOutput,
        else => logits.len / artifact.query_seq_len,
    };
    if (vocab_size == 0 or logits.len < vocab_size) return error.InvalidArtifactOutput;
    const last_logits = logits[logits.len - vocab_size ..];
    const compare_summary = if (compare_host)
        try compareFullModelArtifactOutputs(allocator, artifact, graph_input_ids, last_logits)
    else
        try allocator.dupe(u8, "");
    errdefer allocator.free(compare_summary);
    return .{
        .token_id = activations.argmax(last_logits),
        .compare_summary = compare_summary,
    };
}

const MaterializedPartitionInputs = struct {
    values: []ops.CT,
    node_ids: []ml.graph.NodeId,
    cb: ops.ComputeBackend,
    runtime_inputs: []interpreter.RuntimeInput,
    graph: ?ml.graph.Graph = null,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        var capture = interpreter.CapturedValuesResult{ .values = self.values, .allocator = allocator };
        capture.deinit(&self.cb);
        for (self.runtime_inputs) |ri| self.cb.free(ri.value);
        allocator.free(self.runtime_inputs);
        allocator.free(self.node_ids);
        self.cb.deinit();
        if (self.graph) |*graph| graph.deinit();
    }
};

const PartitionRunResult = struct {
    output_count: usize,
    output_shapes_summary: []u8,
    compare_summary: []u8,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        allocator.free(self.output_shapes_summary);
        allocator.free(self.compare_summary);
        self.* = undefined;
    }
};

fn runOnnxPartitionArtifact(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    compare_host: bool,
) !PartitionRunResult {
    if (!build_options.enable_onnx) return error.BackendUnavailable;
    if (artifact.onnx_output_node_ids.len == 0) return error.MissingArtifactMetadata;

    const capture_node_ids = try collectOnnxPartitionNodeIds(
        allocator,
        artifact.onnx_input_node_ids,
        artifact.onnx_output_node_ids,
    );
    defer allocator.free(capture_node_ids);

    var materialized = try materializePartitionInputs(
        allocator,
        artifact,
        graph_input_ids,
        capture_node_ids,
    );
    defer materialized.deinit(allocator);

    var session = try backends.onnx.createSession(allocator, artifact.artifact_path);
    defer session.close();
    const input_info = session.inputInfo();
    const output_info = session.outputInfo();
    if (input_info.len != artifact.onnx_input_node_ids.len) return error.UnsupportedArtifactInputs;
    if (output_info.len != artifact.onnx_output_node_ids.len) return error.InvalidArtifactOutput;

    const reordered_input_node_ids = if (artifact.onnx_input_names.len == input_info.len)
        try reorderOnnxNodeIdsByName(allocator, artifact.onnx_input_node_ids, artifact.onnx_input_names, input_info)
    else
        try allocator.dupe(u32, artifact.onnx_input_node_ids);
    defer allocator.free(reordered_input_node_ids);

    const inputs = try allocator.alloc(backends.Tensor, input_info.len);
    defer {
        for (inputs) |*tensor| tensor.deinit();
        allocator.free(inputs);
    }
    for (input_info, reordered_input_node_ids, 0..) |info, node_id_u32, i| {
        if (findCapturedNodeIndex(materialized.node_ids, @intCast(node_id_u32))) |capture_idx| {
            inputs[i] = try tensorFromCapturedValue(allocator, info.name, info.shape, info.dtype, materialized.values[capture_idx], &materialized.cb);
            continue;
        }
        inputs[i] = try tensorFromGraphInputIds(allocator, info.name, info.shape, info.dtype, graph_input_ids);
    }

    const outputs = try session.run(inputs, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    const output_shapes = try allocator.alloc([]const i64, outputs.len);
    defer allocator.free(output_shapes);
    for (outputs, 0..) |tensor, i| output_shapes[i] = tensor.shape;

    const reordered_output_node_ids = if (artifact.onnx_output_names.len == output_info.len)
        try reorderOnnxNodeIdsByName(allocator, artifact.onnx_output_node_ids, artifact.onnx_output_names, output_info)
    else
        try allocator.dupe(u32, artifact.onnx_output_node_ids);
    defer allocator.free(reordered_output_node_ids);

    const output_shapes_summary = try summarizeShapesConst(allocator, output_shapes);
    errdefer allocator.free(output_shapes_summary);
    const compare_summary = if (compare_host) blk: {
        const diffs = try compareOnnxPartitionOutputs(
            allocator,
            reordered_output_node_ids,
            outputs,
            0,
            materialized.node_ids,
            materialized.values,
            &materialized.cb,
        );
        defer allocator.free(diffs);
        const mapping = try summarizeOnnxOutputMapping(allocator, output_info, reordered_output_node_ids);
        defer allocator.free(mapping);
        break :blk try std.fmt.allocPrint(allocator, "{s}; {s}", .{ mapping, diffs });
    } else try allocator.dupe(u8, "");
    errdefer allocator.free(compare_summary);

    return .{
        .output_count = outputs.len,
        .output_shapes_summary = output_shapes_summary,
        .compare_summary = compare_summary,
    };
}

fn runPjrtPartitionArtifact(
    allocator: std.mem.Allocator,
    io: std.Io,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    compare_host: bool,
) !PartitionRunResult {
    if (comptime !build_options.enable_pjrt) return error.BackendUnavailable;
    if (artifact.pjrt_input_bindings.len == 0 or artifact.pjrt_output_node_ids.len == 0) return error.MissingArtifactMetadata;

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
    );
    defer materialized.deinit(allocator);
    const graph = if (materialized.graph) |*graph| graph else return error.MissingArtifactMetadata;

    const input_bindings = try buildPjrtInputBindings(allocator, artifact.pjrt_input_bindings);
    defer allocator.free(input_bindings);
    const output_node_ids = try clonePjrtOutputNodeIds(allocator, artifact.pjrt_output_node_ids);
    defer allocator.free(output_node_ids);

    const plugin_path = try native_backend_choice.pjrtPluginPathFromEnv(allocator) orelse return error.MissingPjrtPlugin;
    defer allocator.free(plugin_path);

    const pjrt_lib = @import("pjrt");
    var client = try pjrt_lib.pjrt.Client.init(plugin_path);
    defer client.deinit();

    const artifact_bytes = try std.Io.Dir.cwd().readFileAlloc(io, artifact.artifact_path, allocator, .limited(std.math.maxInt(usize)));
    defer allocator.free(artifact_bytes);

    var exec = if (std.mem.eql(u8, artifact.kind, "pjrt_partition_executable"))
        try graph_mod.pjrt_executor.createExecutorFromSerializedExecutable(
            allocator,
            graph,
            artifact_bytes,
            input_bindings,
            output_node_ids,
            &materialized.cb,
            &client,
        )
    else
        try graph_mod.pjrt_executor.createExecutorFromHlo(
            allocator,
            graph,
            artifact_bytes,
            input_bindings,
            output_node_ids,
            &materialized.cb,
            &client,
        );
    defer exec.partitionExecutor().deinitExecutor();

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
) !MaterializedPartitionInputs {
    var session_manager = backends.SessionManager.init(allocator);
    session_manager.preferred_backends = &.{backends.BackendType.native};
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDirWithPreferredBackends(
        artifact.model_dir,
        &.{backends.BackendType.native},
        true,
    );
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;

    const graph_input_ids_i64 = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(graph_input_ids_i64);
    for (graph_input_ids, 0..) |id, i| graph_input_ids_i64[i] = id;

    var cb = try session_factory.getComputeBackend(model.session, allocator);
    errdefer cb.deinit();

    var kv_manager = runtime.kv.manager.KvManager.init(allocator);
    defer kv_manager.deinit();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };
    const kv_dtype = session_factory.recommendedKvDTypeForSession(model.session, backend_kind);
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;

    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

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

fn compareFullModelArtifactOutputs(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    artifact_last_logits: []const f32,
) ![]u8 {
    const host_last_logits = try computeNativeLastLogitsForArtifact(allocator, artifact, graph_input_ids);
    defer allocator.free(host_last_logits);

    if (host_last_logits.len != artifact_last_logits.len) return error.ArtifactShapeMismatch;

    var max_abs_diff: f32 = 0.0;
    var mean_abs_diff: f64 = 0.0;
    for (host_last_logits, artifact_last_logits) |host_v, artifact_v| {
        const diff = @abs(host_v - artifact_v);
        if (diff > max_abs_diff) max_abs_diff = diff;
        mean_abs_diff += diff;
    }
    mean_abs_diff /= @floatFromInt(host_last_logits.len);

    const host_top1 = activations.argmax(host_last_logits);
    const artifact_top1 = activations.argmax(artifact_last_logits);
    return std.fmt.allocPrint(
        allocator,
        "host_top1={d}, artifact_top1={d}, max_abs_diff={d}, mean_abs_diff={d}",
        .{ host_top1, artifact_top1, max_abs_diff, mean_abs_diff },
    );
}

fn compareFullOnnxOutputsWithCapturedNative(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
    artifact_logits: []const f32,
    output_mapping: []const u8,
    reordered_output_node_ids: []const u32,
    outputs: []const backends.Tensor,
) ![]u8 {
    if (reordered_output_node_ids.len == 0 or outputs.len == 0) return error.MissingArtifactMetadata;

    const capture_node_ids = try collectOnnxPartitionNodeIds(allocator, &.{}, reordered_output_node_ids);
    defer allocator.free(capture_node_ids);

    var materialized = try materializePartitionInputs(
        allocator,
        artifact,
        graph_input_ids,
        capture_node_ids,
    );
    defer materialized.deinit(allocator);

    var last_logits_max_abs_diff: f32 = 0.0;
    var last_logits_mean_abs_diff: f64 = 0.0;
    const host_top1 = blk: {
        const capture_idx = findCapturedNodeIndex(materialized.node_ids, @intCast(reordered_output_node_ids[0])) orelse return error.MissingRuntimeInput;
        const host_data = try materialized.cb.toFloat32(materialized.values[capture_idx], allocator);
        defer allocator.free(host_data);
        if (host_data.len < artifact_logits.len) return error.ArtifactShapeMismatch;
        const host_logits = host_data[host_data.len - artifact_logits.len ..];
        for (host_logits, artifact_logits) |host_v, artifact_v| {
            const diff = @abs(host_v - artifact_v);
            if (diff > last_logits_max_abs_diff) last_logits_max_abs_diff = diff;
            last_logits_mean_abs_diff += diff;
        }
        last_logits_mean_abs_diff /= @floatFromInt(artifact_logits.len);
        break :blk activations.argmax(host_logits);
    };
    const artifact_top1 = activations.argmax(artifact_logits);

    const diffs = try compareOnnxPartitionOutputs(
        allocator,
        reordered_output_node_ids,
        outputs,
        0,
        materialized.node_ids,
        materialized.values,
        &materialized.cb,
    );
    defer allocator.free(diffs);
    return std.fmt.allocPrint(
        allocator,
        "host_top1={d}, artifact_top1={d}; last_logits:max_abs_diff={d}, mean_abs_diff={d}; graph_{s}; graph_{s}",
        .{ host_top1, artifact_top1, last_logits_max_abs_diff, last_logits_mean_abs_diff, output_mapping, diffs },
    );
}

fn computeNativeLastLogitsForArtifact(
    allocator: std.mem.Allocator,
    artifact: compiled_artifact.Manifest,
    graph_input_ids: []const i32,
) ![]f32 {
    var session_manager = backends.SessionManager.init(allocator);
    session_manager.preferred_backends = &.{backends.BackendType.native};
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDirWithPreferredBackends(
        artifact.model_dir,
        &.{backends.BackendType.native},
        true,
    );
    const gpt_config = session_factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration;

    const input_ids = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(input_ids);
    for (graph_input_ids, 0..) |id, i| input_ids[i] = id;

    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();

    const backend_kind: runtime.kv.pool.BackendKind = switch (model.session.backend()) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        .pjrt => return error.UnexpectedPjrtBackend,
        .onnx => return error.UnexpectedOnnxBackend,
        .wasm => return error.UnexpectedWasmBackend,
    };

    return computeNativeLastLogitsWithBackend(
        allocator,
        artifact,
        graph_input_ids,
        gpt_config,
        &cb,
        backend_kind,
        session_factory.recommendedKvDTypeForSession(model.session, backend_kind),
        model.shared_moe_cache,
    );
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
    var scratch_arena = std.heap.ArenaAllocator.init(allocator);
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();

    const input_ids = try scratch.alloc(i64, graph_input_ids.len);
    for (graph_input_ids, 0..) |id, i| input_ids[i] = id;

    var kv_manager = runtime.kv.manager.KvManager.init(scratch);
    defer kv_manager.deinit();
    const sliding_window_size: ?u32 = if (gpt_config.position_encoding == .absolute)
        null
    else if (gpt_config.sliding_window > 0)
        gpt_config.sliding_window
    else if (gpt_config.max_position_embeddings > 0)
        gpt_config.max_position_embeddings
    else
        null;
    const pool_id = try kv_manager.addPool(.{
        .backend = backend_kind,
        .dtype = kv_dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(gpt_config.num_hidden_layers),
        .num_kv_heads = gpt_config.maxKvHeads(),
        .head_dim = gpt_config.maxHeadDim(),
        .sliding_window_size = sliding_window_size,
    });

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

fn collectOnnxPartitionNodeIds(
    allocator: std.mem.Allocator,
    input_node_ids: []const u32,
    output_node_ids: []const u32,
) ![]ml.graph.NodeId {
    var out = std.ArrayListUnmanaged(ml.graph.NodeId).empty;
    defer out.deinit(allocator);
    for (input_node_ids) |node_id_u32| {
        const node_id: ml.graph.NodeId = @intCast(node_id_u32);
        var found = false;
        for (out.items) |existing| {
            if (existing == node_id) {
                found = true;
                break;
            }
        }
        if (!found) try out.append(allocator, node_id);
    }
    for (output_node_ids) |node_id_u32| {
        const node_id: ml.graph.NodeId = @intCast(node_id_u32);
        var found = false;
        for (out.items) |existing| {
            if (existing == node_id) {
                found = true;
                break;
            }
        }
        if (!found) try out.append(allocator, node_id);
    }
    return out.toOwnedSlice(allocator);
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

fn loadTokenizerAssets(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    manifest: *const manifest_mod.ModelManifest,
) !LoadedTokenizerAssets {
    var assets = LoadedTokenizerAssets{ .allocator = allocator };
    const tokenizer_type = blk: {
        if (model_manager_mod.shouldPreferSentencePieceOverride(manifest.*, model_dir, allocator)) {
            break :blk manifest_mod.TokenizerType.sentencepiece;
        }
        break :blk manifest.tokenizer_type orelse return error.NoTokenizerFound;
    };
    switch (tokenizer_type) {
        .huggingface => assets.hf_tok = try model_manager_mod.loadHuggingFaceTokenizerFromDir(allocator, model_dir),
        .sentencepiece => {
            const sp = try model_manager_mod.loadSentencePieceTokenizerFromDirOrGguf(allocator, model_dir, manifest.gguf_path);
            if (model_manager_mod.shouldEnableGemmaSentencePieceCompat(manifest.*, model_dir, allocator)) {
                sp.setPreserveInlineSpecialsAfterLiteralBos(true);
            }
            try model_manager_mod.loadSentencePieceAddedTokens(model_dir, allocator, sp);
            assets.sp_tok = sp;
        },
    }
    if (manifest.chat_template) |source| {
        const ct = try allocator.create(generation.ChatTemplate);
        errdefer allocator.destroy(ct);
        ct.* = try generation.ChatTemplate.init(
            allocator,
            source,
            manifest.bos_token,
            manifest.eos_token,
            manifest.unk_token,
            manifest.pad_token,
        );
        assets.chat_tmpl = ct;
    }
    return assets;
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

fn compareOnnxPartitionOutputs(
    allocator: std.mem.Allocator,
    output_node_ids_u32: []const u32,
    outputs: []const backends.Tensor,
    output_index_offset: usize,
    captured_node_ids: []const ml.graph.NodeId,
    captured_values: []const ops.CT,
    cb: *const ops.ComputeBackend,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    for (output_node_ids_u32, outputs, 0..) |node_id_u32, tensor, idx| {
        const capture_idx = findCapturedNodeIndex(captured_node_ids, @intCast(node_id_u32)) orelse return error.MissingRuntimeInput;
        const host_data = try cb.toFloat32(captured_values[capture_idx], allocator);
        defer allocator.free(host_data);
        if (tensor.dtype != .f32) return error.InvalidArtifactOutput;
        var copied_output: ?[]f32 = null;
        defer if (copied_output) |buf| allocator.free(buf);
        const onnx_data = if (tensor.asFloat32IfAligned()) |aligned|
            aligned
        else blk: {
            copied_output = try allocator.alloc(f32, tensor.data.len / @sizeOf(f32));
            @memcpy(std.mem.sliceAsBytes(copied_output.?), tensor.data);
            break :blk copied_output.?;
        };
        if (host_data.len != onnx_data.len) return error.ArtifactShapeMismatch;
        var max_abs_diff: f32 = 0.0;
        for (host_data, onnx_data) |host_v, onnx_v| {
            const diff = @abs(host_v - onnx_v);
            if (diff > max_abs_diff) max_abs_diff = diff;
        }
        if (idx > 0) try out.appendSlice(allocator, ", ");
        const line = try std.fmt.allocPrint(allocator, "out{d}:max_abs_diff={d}", .{ idx + output_index_offset, max_abs_diff });
        defer allocator.free(line);
        try out.appendSlice(allocator, line);
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
        const host_data = try cb.toFloat32(captured_values[capture_idx], allocator);
        defer allocator.free(host_data);
        const pjrt_data = try buffer.toFloat32(allocator);
        defer allocator.free(pjrt_data);
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

fn reorderOnnxNodeIdsByName(
    allocator: std.mem.Allocator,
    node_ids: []const u32,
    node_names: []const []u8,
    info_list: []const backends.TensorInfo,
) ![]u32 {
    const reordered = try allocator.alloc(u32, info_list.len);
    errdefer allocator.free(reordered);
    for (info_list, 0..) |info, idx| {
        const src_idx = findFeatureIndexConst(node_names, info.name) orelse return error.InvalidArtifactOutput;
        reordered[idx] = node_ids[src_idx];
    }
    return reordered;
}

fn findFeatureIndexConst(features: []const []u8, name: []const u8) ?usize {
    for (features, 0..) |feature, idx| {
        if (std.mem.eql(u8, feature, name)) return idx;
    }
    return null;
}

fn summarizeOnnxOutputMapping(
    allocator: std.mem.Allocator,
    output_info: []const backends.TensorInfo,
    reordered_output_node_ids: []const u32,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    try out.appendSlice(allocator, "mapping=");
    for (output_info, reordered_output_node_ids, 0..) |info, node_id, idx| {
        if (idx > 0) try out.appendSlice(allocator, ",");
        const piece = try std.fmt.allocPrint(allocator, "{s}->{d}", .{ info.name, node_id });
        defer allocator.free(piece);
        try out.appendSlice(allocator, piece);
    }
    return out.toOwnedSlice(allocator);
}

fn tensorFromCapturedValue(
    allocator: std.mem.Allocator,
    name: []const u8,
    expected_shape: []const i64,
    expected_dtype: backends.DType,
    value: ops.CT,
    cb: *const ops.ComputeBackend,
) !backends.Tensor {
    const actual_dtype = try cb.tensorDType(value);
    const actual_shape = try cb.tensorShape(value, allocator);
    defer allocator.free(actual_shape);
    if (actual_shape.len != expected_shape.len) return error.ArtifactShapeMismatch;
    for (actual_shape, expected_shape) |actual_dim, expected_dim| {
        if (actual_dim != expected_dim) return error.ArtifactShapeMismatch;
    }

    if (expected_dtype == .f32) {
        const f32_data = try cb.toFloat32(value, allocator);
        defer allocator.free(f32_data);
        return backends.Tensor.initFloat32(allocator, name, expected_shape, f32_data);
    }

    if (actual_dtype == expected_dtype) {
        if (try cb.exportTensorData(value, allocator)) |exported| {
            defer switch (exported.payload) {
                .bytes => |bytes| allocator.free(bytes),
                .quantized_f32 => |quantized| {
                    allocator.free(quantized.raw_bytes);
                    allocator.free(quantized.shape);
                },
            };
            switch (exported.payload) {
                .bytes => |bytes| return .{
                    .data = try allocator.dupe(u8, bytes),
                    .dtype = expected_dtype,
                    .shape = try allocator.dupe(i64, expected_shape),
                    .name = name,
                    .allocator = allocator,
                    .owns_data = true,
                    .owns_shape = true,
                },
                .quantized_f32 => {},
            }
        }
    }

    return error.UnsupportedArtifactInputs;
}

fn tensorFromGraphInputIds(
    allocator: std.mem.Allocator,
    name: []const u8,
    expected_shape: []const i64,
    expected_dtype: backends.DType,
    graph_input_ids: []const i32,
) !backends.Tensor {
    if (expected_dtype != .i64) return error.MissingRuntimeInput;
    if (expected_shape.len != 1 or expected_shape[0] != @as(i64, @intCast(graph_input_ids.len))) {
        return error.ArtifactShapeMismatch;
    }
    const ids_i64 = try allocator.alloc(i64, graph_input_ids.len);
    defer allocator.free(ids_i64);
    for (graph_input_ids, 0..) |id, i| ids_i64[i] = id;
    return backends.Tensor.initInt64(allocator, name, expected_shape, ids_i64);
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
        \\usage: termite run-artifact <artifact-or-manifest> <prompt> [--no-chat-template] [--raw-prompt] [--compare-host]
        \\       termite run-artifact <artifact-or-manifest> [--validate|--dry-run]
        \\
        \\<artifact-or-manifest> may be a raw artifact path, a .termite.json sidecar,
        \\or a .termite-package.json package manifest.
        \\Runs an offline artifact for its exact traced shape and prints the top-1
        \\next token without retracing or recompiling. Package manifests resolve the
        \\matching prefill entry for the prompt shape. Partition artifacts print
        \\output shapes; --compare-host adds a traced native output comparison.
        \\Use --validate/--dry-run for load-only validation without execution; package
        \\validation reports artifact/prefill/decode counts and runtime ownership.
        \\PJRT/XLA HLO artifacts require TERMITE_XLA_PLUGIN, TERMITE_PJRT_PLUGIN,
        \\or PJRT_PLUGIN_PATH.
        \\
    , .{});
}

test "parseArgs accepts artifact path and prompt" {
    const opts = try parseArgs(&.{ "/tmp/model.onnx.termite.json", "hello", "--raw-prompt", "--compare-host" });
    try std.testing.expectEqualStrings("/tmp/model.onnx.termite.json", opts.artifact_or_manifest_path);
    try std.testing.expectEqualStrings("hello", opts.prompt.?);
    try std.testing.expect(opts.raw_prompt);
    try std.testing.expect(opts.compare_host);
}

test "parseArgs accepts validate without prompt" {
    const opts = try parseArgs(&.{ "/tmp/model.onnx.termite.json", "--validate" });
    try std.testing.expectEqualStrings("/tmp/model.onnx.termite.json", opts.artifact_or_manifest_path);
    try std.testing.expectEqual(@as(?[]const u8, null), opts.prompt);
    try std.testing.expect(opts.validate);
}

test "validateArtifact summarizes package manifests" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.prefill.exec.termite.json" });
    defer allocator.free(prefill_manifest_path);
    const decode_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.decode.s3.exec.termite.json" });
    defer allocator.free(decode_manifest_path);
    const package_path = try std.fs.path.join(allocator, &.{ base_dir, "gpt2.xla.pjrt_executable.inputs.termite-package.json" });
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
    const prefill_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.prefill.exec.termite.json" });
    defer allocator.free(prefill_manifest_path);
    const decode_manifest_path = try std.fs.path.join(allocator, &.{ base_dir, "model.decode.s3.exec.termite.json" });
    defer allocator.free(decode_manifest_path);
    const package_path = try std.fs.path.join(allocator, &.{ base_dir, "gpt2.xla.pjrt_executable.embedded.termite-package.json" });
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
                .manifest_path = "/tmp/model.prefill.exec.termite.json",
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
                .manifest_path = "/tmp/model.prefill.embedded.exec.termite.json",
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
                .manifest_path = "/tmp/model.prefill.inputs.exec.termite.json",
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

test "findMatchingFullModelPackageManifest resolves matching ONNX package" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const base_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(base_dir);
    const package_path = try compiled_artifact.packageManifestPath(
        allocator,
        base_dir,
        "onnx",
        "/tmp/model",
        "onnx_graph",
        null,
    );
    defer allocator.free(package_path);
    try compiled_artifact.writePackageManifest(allocator, io, package_path, .{
        .backend = "onnx",
        .model_dir = "/tmp/model",
        .kind = "onnx_graph",
        .artifacts = &.{
            .{
                .manifest_path = "/tmp/model.prefill.onnx.termite.json",
                .artifact_path = "/tmp/model.prefill.onnx",
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
        "onnx",
        "/tmp/model",
        2,
        2,
        "paged_prefill",
    );
    try std.testing.expect(found != null);
    defer allocator.free(found.?);
    try std.testing.expectEqualStrings(package_path, found.?);
}
