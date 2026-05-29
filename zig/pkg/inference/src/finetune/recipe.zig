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

const grpo = @import("grpo.zig");
const preference_loss = @import("preference_loss.zig");
const gemma4 = @import("gemma4.zig");
const gemma4_real_autodiff = @import("gemma4_real_autodiff.zig");
const gemma4_mm_real_autodiff = @import("gemma4_multimodal_real_autodiff.zig");
const qwen2_real_autodiff = @import("qwen2_real_autodiff.zig");
const gemma_chat_data = @import("gemma_chat_data.zig");
const real_autodiff = @import("real_autodiff_trainer.zig");
const gliner2 = @import("gliner2.zig");
const gliner2_boundary = @import("gliner2_boundary.zig");
const gliner2_data = @import("gliner2_data.zig");
const layoutlmv3 = @import("layoutlmv3.zig");
const colqwen2 = @import("colqwen2.zig");
const reranker_data = @import("reranker_data.zig");
const reranker_head = @import("reranker_head.zig");
const reranker_lora = @import("reranker_lora.zig");
const reranker = @import("reranker.zig");
const preference_harness = @import("preference_harness.zig");
const train_eval_gemma4_lora_bundle = @import("train/train_eval_gemma4_lora_bundle.zig");
const train_eval_gliner2_lora_bundle = @import("train/train_eval_gliner2_lora_bundle.zig");
const train_eval_layoutlmv3_lora_sequence = @import("train/train_eval_layoutlmv3_lora_sequence.zig");
const train_eval_layoutlmv3_lora_token = @import("train/train_eval_layoutlmv3_lora_token.zig");
const train_eval_colqwen2_lora_bundle = @import("train/train_eval_colqwen2_lora_bundle.zig");
const train_eval_reranker_lora_top_layer_cached_surrogate = @import("train/train_eval_reranker_lora_top_layer_cached_surrogate.zig");
const generation = @import("../pipelines/generation.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const backends = @import("../backends/backends.zig");
const session_factory = @import("../architectures/session_factory.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const native_backend_choice = @import("../native_backend_choice.zig");
const tokenizer_mod = @import("inference_tokenizer");
const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const command_registry = @import("command_registry.zig");
const ml = @import("ml");
const peft = @import("peft.zig");

const print = std.debug.print;

const default_lora_rank: usize = 16;
const default_policy_lora_rank: usize = 8;
const default_lora_alpha: f32 = 32.0;
const default_lora_target_preset = "all-linear";

const qwen_attention_lora_target_modules = [_][]const u8{ "q_proj", "k_proj", "v_proj", "o_proj" };
const qwen_mlp_lora_target_modules = [_][]const u8{ "gate_proj", "up_proj", "down_proj" };

pub const RecipeKind = enum {
    sft,
    lora_sft,
    qlora_sft,
    dpo,
    grpo,
    reranker,
    vlm_retrieval,
};

pub const ModelConfig = struct {
    path: ?[]const u8 = null,
    reference_path: ?[]const u8 = null,
    name: ?[]const u8 = null,
    family: ?[]const u8 = null,
    projector_path: ?[]const u8 = null,
};

pub const DatasetConfig = struct {
    path: ?[]const u8 = null,
    train_path: ?[]const u8 = null,
    eval_path: ?[]const u8 = null,
    train_split: ?[]const u8 = "train",
    eval_split: ?[]const u8 = null,
    prepared_path: ?[]const u8 = null,
    cache_path: ?[]const u8 = null,
    train_cache_path: ?[]const u8 = null,
    eval_cache_path: ?[]const u8 = null,
    format: ?[]const u8 = null,
    labels: ?[]const u8 = null,
    max_examples: ?usize = null,
    eval_max_examples: ?usize = null,
    max_seq_len: ?usize = null,
};

pub const AdapterConfig = struct {
    path: ?[]const u8 = null,
    rank: ?usize = null,
    alpha: ?f32 = null,
    layer_name: ?[]const u8 = null,
    base_model_name_or_path: ?[]const u8 = null,
    quantization: ?[]const u8 = null,
    target_preset: ?[]const u8 = null,
    target_modules: ?[]const []const u8 = null,
    init_lora_weights: ?[]const u8 = null,
    use_dora: ?bool = null,
    scaling: ?[]const u8 = null,
};

pub const OptimizerConfig = struct {
    learning_rate: ?f32 = null,
    epochs: ?usize = null,
    micro_batch_size: ?usize = null,
    gradient_accumulation_steps: ?u32 = null,
    max_grad_norm: ?f32 = null,
    schedule_free: ?bool = null,
    llrd_decay: ?f32 = null,
};

pub const PreferenceConfig = struct {
    beta: ?f32 = null,
    simpo_gamma: ?f32 = null,
    sft_lambda: ?f32 = null,
    ipo_tau: ?f32 = null,
};

pub const GrpoConfig = struct {
    group_size: ?usize = null,
    clip_epsilon: ?f32 = null,
    kl_coef: ?f32 = null,
    advantage_eps: ?f32 = null,
    normalize_advantage: ?bool = null,
    max_completion_tokens: ?usize = null,
    reward_mode: ?[]const u8 = null,
};

pub const EvalConfig = struct {
    max_examples: ?usize = null,
    split: ?[]const u8 = null,
};

pub const ArtifactConfig = struct {
    root: ?[]const u8 = null,
    manifest_path: ?[]const u8 = null,
    prepared_path: ?[]const u8 = null,
    adapter_dir: ?[]const u8 = null,
    trained_adapter_dir: ?[]const u8 = null,
    materialized_dir: ?[]const u8 = null,
    report_path: ?[]const u8 = null,
};

pub const Recipe = struct {
    recipe: ?[]const u8 = null,
    kind: ?[]const u8 = null,
    model: ModelConfig = .{},
    dataset: DatasetConfig = .{},
    adapter: ?AdapterConfig = null,
    optimizer: OptimizerConfig = .{},
    preference: PreferenceConfig = .{},
    grpo: GrpoConfig = .{},
    eval: ?EvalConfig = null,
    artifacts: ArtifactConfig = .{},
    backend: ?[]const u8 = null,
    trainer: ?[]const u8 = null,
};

const Step = struct {
    kind: StepKind = .command,
    name: []const u8,
    argv: []const []const u8,
};

const StepKind = enum {
    command,
    direct_sft,
    direct_dpo,
    direct_grpo,
};

const Plan = struct {
    steps: []Step,
};

const RunStatus = enum {
    planned,
    running,
    succeeded,
    failed,
};

const StepManifest = struct {
    index: usize,
    name: []const u8,
    argv: []const []const u8,
    status: RunStatus = .planned,
    exit_code: ?u8 = null,
    stdout_bytes: ?usize = null,
    stderr_bytes: ?usize = null,
};

const RunManifest = struct {
    schema_version: []const u8 = "antfly_inference_finetune_recipe_run/v1",
    status: RunStatus,
    recipe: Recipe,
    artifact_root: ?[]const u8,
    steps: []const StepManifest,
};

const TrainingConfigFile = struct {
    schema_version: []const u8 = "antfly_inference_finetune_training_config/v1",
    recipe: Recipe,
    steps: []const StepManifest,
    metadata: StaticMetadata,
};

const TrainingReportFile = struct {
    schema_version: []const u8 = "antfly_inference_finetune_training_report/v1",
    status: RunStatus,
    recipe: Recipe,
    artifact_root: ?[]const u8,
    steps: []const StepManifest,
    metadata: ReportMetadata,
};

const PathFingerprint = struct {
    label: []const u8,
    path: []const u8,
    exists: bool,
    kind: ?[]const u8 = null,
    size_bytes: ?u64 = null,
    entries: ?usize = null,
    digest: ?[]const u8 = null,
};

const BackendBuildInfo = struct {
    inference_version: []const u8,
    enable_native: bool,
    enable_onnx: bool,
    enable_mlx: bool,
    enable_pjrt: bool,
    skip_openapi: bool,
};

const BackendMetadata = struct {
    requested: ?[]const u8,
    build: BackendBuildInfo,
};

const OptimizerSummary = struct {
    learning_rate: ?f32,
    epochs: ?usize,
    micro_batch_size: ?usize,
    gradient_accumulation_steps: ?u32,
    max_grad_norm: ?f32,
    schedule_free: ?bool,
    llrd_decay: ?f32,
};

const StaticMetadata = struct {
    dataset_fingerprints: []const PathFingerprint,
    backend: BackendMetadata,
    optimizer: OptimizerSummary,
};

const ReportMetadata = struct {
    dataset_fingerprints: []const PathFingerprint,
    backend: BackendMetadata,
    optimizer: OptimizerSummary,
    artifact_checksums: ?[]const PathFingerprint = null,
};

const PlannedPath = struct {
    label: []const u8,
    path: []const u8,
};

const DirectoryDigestEntry = struct {
    relative_path: []const u8,
    size_bytes: u64,
    digest: []const u8,
};

const DirectoryDigest = struct {
    digest: []const u8,
    size_bytes: u64,
    entries: usize,
};

const DpoReport = struct {
    schema_version: []const u8 = "antfly_inference_finetune_dpo_report/v1",
    examples: usize,
    loss: f32,
    mean_reward_margin: f32,
    accuracy: f32,
    beta: f32,
};

const SftReport = struct {
    schema_version: []const u8 = "antfly_inference_finetune_sft_report/v1",
    examples: usize,
    supervised_tokens: usize,
    loss: f32,
    epochs: usize,
    trained_adapter_dir: []const u8,
};

const GrpoReport = struct {
    schema_version: []const u8 = "antfly_inference_finetune_grpo_report/v1",
    completions: usize,
    tokens: usize,
    groups: usize,
    loss: f32,
    pg_loss: f32,
    kl_loss: f32,
    clip_fraction: f32,
};

const FastSmokeMode = enum {
    dry_run,
    execute,
    subprocess_execute,
};

const FastSmokeSetup = enum {
    none,
    synthetic_gliner2_execute,
    synthetic_qwen2_dpo_execute,
    synthetic_qwen2_grpo_execute,
    synthetic_gemma_dpo_execute,
    synthetic_gemma_grpo_execute,
};

const FastSmokeCase = struct {
    name: []const u8,
    recipe_path: []const u8,
    mode: FastSmokeMode,
    setup: FastSmokeSetup = .none,
};

const FastSmokeCaseResult = struct {
    name: []const u8,
    recipe_path: []const u8,
    mode: FastSmokeMode,
    status: RunStatus,
    manifest_path: ?[]const u8 = null,
    training_report_path: ?[]const u8 = null,
};

const FastSmokeSummary = struct {
    schema_version: []const u8 = "antfly_inference_finetune_fast_smoke/v1",
    status: RunStatus,
    output_root: []const u8,
    cases: []const FastSmokeCaseResult,
};

const FastSmokeRecipeOverrides = struct {
    model_path: ?[]const u8 = null,
    reference_path: ?[]const u8 = null,
    dataset_path: ?[]const u8 = null,
    dataset_format: ?[]const u8 = null,
    train_path: ?[]const u8 = null,
    eval_path: ?[]const u8 = null,
    labels: ?[]const u8 = null,
    backend: ?[]const u8 = null,
    max_examples: ?usize = null,
    eval_max_examples: ?usize = null,
    max_seq_len: ?usize = null,
};

const SyntheticGlinerAssets = struct {
    model_dir: []const u8,
    train_path: []const u8,
    eval_path: []const u8,
    labels: []const u8,
};

const SyntheticQwen2Assets = struct {
    model_dir: []const u8,
    dpo_path: []const u8,
    grpo_path: []const u8,
};

const SyntheticGemmaAssets = struct {
    model_dir: []const u8,
    dpo_path: []const u8,
    grpo_path: []const u8,
};

const WriteTensorF32 = struct {
    name: []const u8,
    shape: []const usize,
    data: []const f32,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    if (args.len >= 1 and std.mem.eql(u8, args[0], "smoke-fast")) {
        return runFastSmoke(allocator, io, args[1..]);
    }
    if (args.len < 2 or !std.mem.eql(u8, args[0], "run")) {
        usage();
        return;
    }

    const recipe_path = args[1];
    var dry_run = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--dry-run") or std.mem.eql(u8, args[i], "--plan")) {
            dry_run = true;
        } else {
            return usageError();
        }
    }

    var parsed = try loadRecipe(allocator, io, recipe_path);
    defer parsed.deinit();

    const recipe = parsed.value;
    var plan_arena = std.heap.ArenaAllocator.init(allocator);
    defer plan_arena.deinit();
    const plan = try buildPlan(plan_arena.allocator(), recipe);

    try printPlan(io, recipe, plan);
    if (dry_run) return;

    const exe_dir = try std.process.executableDirPathAlloc(io, allocator);
    defer allocator.free(exe_dir);
    const manifest_path = try manifestPath(allocator, recipe);
    defer allocator.free(manifest_path);
    const training_config_path = try defaultArtifactPath(allocator, recipe, "training_config.json");
    defer allocator.free(training_config_path);
    const training_report_path = try defaultArtifactPath(allocator, recipe, "training_report.json");
    defer allocator.free(training_report_path);
    print("manifest: {s}\n", .{manifest_path});
    print("training config: {s}\n", .{training_config_path});
    print("training report: {s}\n", .{training_report_path});
    try runPlan(allocator, io, exe_dir, recipe, plan, manifest_path, training_config_path, training_report_path);
}

pub fn loadRecipe(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !std.json.Parsed(Recipe) {
    const raw = try readFileMax(allocator, io, path, 32 * 1024 * 1024);
    defer allocator.free(raw);
    return std.json.parseFromSlice(Recipe, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn runFastSmoke(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var out_root: []const u8 = "/tmp/antfly-inference-finetune-smoke-fast";
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--out-root")) {
            i += 1;
            if (i >= args.len) return usageError();
            out_root = args[i];
        } else {
            return usageError();
        }
    }

    try std.Io.Dir.cwd().createDirPath(io, out_root);
    const summary_path = try std.fs.path.join(allocator, &.{ out_root, "fast_smoke_summary.json" });
    defer allocator.free(summary_path);

    const cases = [_]FastSmokeCase{
        .{ .name = "gemma4_dry_run", .recipe_path = "pkg/inference/testdata/recipe_gemma4_lora.json", .mode = .dry_run },
        .{ .name = "gliner2_dry_run", .recipe_path = "pkg/inference/testdata/recipe_gliner2_lora.json", .mode = .dry_run },
        .{ .name = "layoutlmv3_dry_run", .recipe_path = "pkg/inference/testdata/recipe_layoutlmv3_lora_token.json", .mode = .dry_run },
        .{ .name = "reranker_head_dry_run", .recipe_path = "pkg/inference/testdata/recipe_reranker_head.json", .mode = .dry_run },
        .{ .name = "reranker_lora_dry_run", .recipe_path = "pkg/inference/testdata/recipe_reranker_lora.json", .mode = .dry_run },
        .{ .name = "colqwen2_dry_run", .recipe_path = "pkg/inference/testdata/recipe_colqwen2_vlm_retrieval.json", .mode = .dry_run },
        .{ .name = "dpo_text_dry_run", .recipe_path = "pkg/inference/testdata/recipe_dpo_text_preference_native_fast.json", .mode = .dry_run },
        .{ .name = "dpo_text_gemma_dry_run", .recipe_path = "pkg/inference/testdata/recipe_dpo_text_preference_gemma_fast.json", .mode = .dry_run },
        .{ .name = "dpo_rendered_text_gemma_dry_run", .recipe_path = "pkg/inference/testdata/recipe_dpo_rendered_text_preference_gemma_fast.json", .mode = .dry_run },
        .{ .name = "dpo_text_qwen2_dry_run", .recipe_path = "pkg/inference/testdata/recipe_dpo_text_preference_qwen2_fast.json", .mode = .dry_run },
        .{ .name = "grpo_text_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_native_fast.json", .mode = .dry_run },
        .{ .name = "grpo_text_gemma_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_gemma_fast.json", .mode = .dry_run },
        .{ .name = "grpo_rendered_text_gemma_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_rendered_text_gemma_fast.json", .mode = .dry_run },
        .{ .name = "grpo_multimodal_gemma_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_multimodal_gemma_fast.json", .mode = .dry_run },
        .{ .name = "grpo_text_qwen2_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_qwen2_fast.json", .mode = .dry_run },
        .{ .name = "grpo_text_colqwen2_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_colqwen2_fast.json", .mode = .dry_run },
        .{ .name = "grpo_ci_text_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_ci_native_fast.json", .mode = .dry_run },
        .{ .name = "grpo_prefix_text_dry_run", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_prefix_native_fast.json", .mode = .dry_run },
        .{ .name = "gliner2_direct_execute", .recipe_path = "pkg/inference/testdata/recipe_gliner2_lora.json", .mode = .execute, .setup = .synthetic_gliner2_execute },
        .{ .name = "qwen2_dpo_execute", .recipe_path = "pkg/inference/testdata/recipe_dpo_text_preference_qwen2_fast.json", .mode = .subprocess_execute, .setup = .synthetic_qwen2_dpo_execute },
        .{ .name = "qwen2_grpo_execute", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_qwen2_fast.json", .mode = .execute, .setup = .synthetic_qwen2_grpo_execute },
        .{ .name = "gemma4_dpo_execute", .recipe_path = "pkg/inference/testdata/recipe_dpo_text_preference_gemma_fast.json", .mode = .execute, .setup = .synthetic_gemma_dpo_execute },
        .{ .name = "gemma4_grpo_execute", .recipe_path = "pkg/inference/testdata/recipe_grpo_text_gemma_fast.json", .mode = .execute, .setup = .synthetic_gemma_grpo_execute },
        .{ .name = "dpo_scalar_execute", .recipe_path = "pkg/inference/testdata/recipe_dpo_scalar.json", .mode = .execute },
        .{ .name = "grpo_scalar_execute", .recipe_path = "pkg/inference/testdata/recipe_grpo_scalar.json", .mode = .execute },
    };

    var results = try allocator.alloc(FastSmokeCaseResult, cases.len);
    defer freeFastSmokeResults(allocator, results);
    for (cases, 0..) |case, idx| {
        results[idx] = try runFastSmokeCase(allocator, io, out_root, case);
    }

    const overall = blk: {
        for (results) |result| {
            if (result.status != .succeeded) break :blk RunStatus.failed;
        }
        break :blk RunStatus.succeeded;
    };
    try writeJsonFile(allocator, io, summary_path, FastSmokeSummary{
        .status = overall,
        .output_root = out_root,
        .cases = results,
    });
    print("fast smoke summary: {s}\n", .{summary_path});
    if (overall != .succeeded) return error.FinetuneStepFailed;
}

fn runFastSmokeCase(
    allocator: std.mem.Allocator,
    io: std.Io,
    out_root: []const u8,
    case: FastSmokeCase,
) !FastSmokeCaseResult {
    print("fast-smoke {s}: {s}\n", .{ case.name, case.recipe_path });
    var path_arena = std.heap.ArenaAllocator.init(allocator);
    defer path_arena.deinit();
    const path_allocator = path_arena.allocator();
    const case_root = try std.fs.path.join(allocator, &.{ out_root, case.name });
    defer allocator.free(case_root);
    const overrides = try setupFastSmokeCase(allocator, io, case_root, case.setup);
    defer freeFastSmokeRecipeOverrides(allocator, overrides);
    const recipe_path = try resolveCwdPath(path_allocator, io, case.recipe_path);
    var parsed = try loadRecipe(allocator, io, recipe_path);
    defer parsed.deinit();
    var recipe = parsed.value;
    try normalizeFastSmokeRecipePaths(path_allocator, io, &recipe);
    applyFastSmokeRecipeOverrides(&recipe, overrides);

    if (case.mode == .dry_run) {
        var plan_arena = std.heap.ArenaAllocator.init(allocator);
        defer plan_arena.deinit();
        const plan = try buildPlan(plan_arena.allocator(), recipe);
        try printPlan(io, recipe, plan);
        return .{
            .name = case.name,
            .recipe_path = case.recipe_path,
            .mode = case.mode,
            .status = .succeeded,
        };
    }

    recipe.artifacts.root = case_root;
    recipe.artifacts.manifest_path = null;
    recipe.artifacts.report_path = null;
    recipe.artifacts.prepared_path = null;
    recipe.artifacts.adapter_dir = null;
    recipe.artifacts.trained_adapter_dir = null;
    recipe.artifacts.materialized_dir = null;

    var plan_arena = std.heap.ArenaAllocator.init(allocator);
    defer plan_arena.deinit();
    const plan = try buildPlan(plan_arena.allocator(), recipe);
    const exe_dir = try std.process.executableDirPathAlloc(io, allocator);
    defer allocator.free(exe_dir);
    const manifest_path = try manifestPath(allocator, recipe);
    defer allocator.free(manifest_path);
    const training_config_path = try defaultArtifactPath(allocator, recipe, "training_config.json");
    defer allocator.free(training_config_path);
    const training_report_path = try defaultArtifactPath(allocator, recipe, "training_report.json");
    defer allocator.free(training_report_path);
    if (case.mode == .subprocess_execute) {
        const smoke_recipe_path = try std.fs.path.join(allocator, &.{ case_root, "smoke_recipe.json" });
        defer allocator.free(smoke_recipe_path);
        try writeJsonFile(allocator, io, smoke_recipe_path, recipe);
        const antfly_path = try std.fs.path.join(allocator, &.{ exe_dir, "antfly" });
        defer allocator.free(antfly_path);
        const result = try std.process.run(allocator, io, .{
            .argv = &.{ antfly_path, "inference", "finetune", "run", smoke_recipe_path },
            .stdout_limit = .limited(16 * 1024 * 1024),
            .stderr_limit = .limited(16 * 1024 * 1024),
        });
        defer allocator.free(result.stdout);
        defer allocator.free(result.stderr);
        if (result.stdout.len > 0) print("{s}", .{result.stdout});
        if (result.stderr.len > 0) print("{s}", .{result.stderr});
        switch (result.term) {
            .exited => |code| if (code != 0) return error.FinetuneStepFailed,
            else => return error.FinetuneStepFailed,
        }
        const dpo_path = try dpoReportPath(allocator, recipe);
        defer allocator.free(dpo_path);
        const trained_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
        defer if (recipe.artifacts.trained_adapter_dir == null and recipe.artifacts.adapter_dir == null) allocator.free(trained_dir);
        try expectPathExists(io, dpo_path);
        try expectPathExists(io, trained_dir);
        try expectRunStatusFile(allocator, io, manifest_path, "succeeded");
        try expectRunStatusFile(allocator, io, training_report_path, "succeeded");
        return .{
            .name = case.name,
            .recipe_path = case.recipe_path,
            .mode = case.mode,
            .status = .succeeded,
            .manifest_path = try allocator.dupe(u8, manifest_path),
            .training_report_path = try allocator.dupe(u8, training_report_path),
        };
    }
    try runPlan(allocator, io, exe_dir, recipe, plan, manifest_path, training_config_path, training_report_path);
    try expectRunStatusFile(allocator, io, manifest_path, "succeeded");
    try expectRunStatusFile(allocator, io, training_report_path, "succeeded");
    return .{
        .name = case.name,
        .recipe_path = case.recipe_path,
        .mode = case.mode,
        .status = .succeeded,
        .manifest_path = try allocator.dupe(u8, manifest_path),
        .training_report_path = try allocator.dupe(u8, training_report_path),
    };
}

fn freeFastSmokeResults(allocator: std.mem.Allocator, results: []FastSmokeCaseResult) void {
    for (results) |result| {
        if (result.manifest_path) |path| allocator.free(path);
        if (result.training_report_path) |path| allocator.free(path);
    }
    allocator.free(results);
}

fn freeFastSmokeRecipeOverrides(allocator: std.mem.Allocator, overrides: FastSmokeRecipeOverrides) void {
    var freed: [5][]const u8 = undefined;
    var freed_len: usize = 0;
    freeUniqueOptionalPath(allocator, &freed, &freed_len, overrides.model_path);
    freeUniqueOptionalPath(allocator, &freed, &freed_len, overrides.reference_path);
    freeUniqueOptionalPath(allocator, &freed, &freed_len, overrides.dataset_path);
    freeUniqueOptionalPath(allocator, &freed, &freed_len, overrides.train_path);
    freeUniqueOptionalPath(allocator, &freed, &freed_len, overrides.eval_path);
}

fn freeUniqueOptionalPath(
    allocator: std.mem.Allocator,
    freed: *[5][]const u8,
    freed_len: *usize,
    maybe_path: ?[]const u8,
) void {
    const path = maybe_path orelse return;
    for (freed[0..freed_len.*]) |existing| {
        if (existing.ptr == path.ptr and existing.len == path.len) return;
    }
    allocator.free(path);
    freed[freed_len.*] = path;
    freed_len.* += 1;
}

fn normalizeFastSmokeRecipePaths(allocator: std.mem.Allocator, io: std.Io, recipe: *Recipe) !void {
    recipe.dataset.path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.path);
    recipe.dataset.train_path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.train_path);
    recipe.dataset.eval_path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.eval_path);
    recipe.dataset.prepared_path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.prepared_path);
    recipe.dataset.cache_path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.cache_path);
    recipe.dataset.train_cache_path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.train_cache_path);
    recipe.dataset.eval_cache_path = try resolveOptionalCwdPath(allocator, io, recipe.dataset.eval_cache_path);
}

fn resolveOptionalCwdPath(allocator: std.mem.Allocator, io: std.Io, maybe_path: ?[]const u8) !?[]const u8 {
    const path = maybe_path orelse return null;
    return try resolveCwdPath(allocator, io, path);
}

fn resolveCwdPath(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]const u8 {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return try allocator.dupe(u8, path);
    if (cwdPathExists(io, path)) return try allocator.dupe(u8, path);

    const package_prefix = "pkg/inference/";
    if (std.mem.startsWith(u8, path, package_prefix)) {
        const package_relative = path[package_prefix.len..];
        if (cwdPathExists(io, package_relative)) return try allocator.dupe(u8, package_relative);
    } else {
        const repo_relative = try std.fs.path.join(allocator, &.{ package_prefix[0 .. package_prefix.len - 1], path });
        if (cwdPathExists(io, repo_relative)) return repo_relative;
        allocator.free(repo_relative);
    }
    return try allocator.dupe(u8, path);
}

fn cwdPathExists(io: std.Io, path: []const u8) bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch return false;
    return true;
}

fn setupFastSmokeCase(
    allocator: std.mem.Allocator,
    io: std.Io,
    case_root: []const u8,
    setup: FastSmokeSetup,
) !FastSmokeRecipeOverrides {
    return switch (setup) {
        .none => .{},
        .synthetic_gliner2_execute => blk: {
            const assets = try writeSyntheticGliner2SmokeAssets(allocator, io, case_root);
            break :blk .{
                .model_path = assets.model_dir,
                .train_path = assets.train_path,
                .eval_path = assets.eval_path,
                .labels = assets.labels,
                .backend = "native",
                .max_examples = 2,
                .eval_max_examples = 1,
                .max_seq_len = 16,
            };
        },
        .synthetic_qwen2_dpo_execute => blk: {
            const assets = try writeSyntheticQwen2SmokeAssets(allocator, io, case_root);
            allocator.free(assets.grpo_path);
            break :blk .{
                .model_path = assets.model_dir,
                .reference_path = assets.model_dir,
                .dataset_path = assets.dpo_path,
                .dataset_format = "rendered-text-preference",
                .backend = "auto",
                .max_examples = 1,
                .max_seq_len = 32,
            };
        },
        .synthetic_qwen2_grpo_execute => blk: {
            const assets = try writeSyntheticQwen2SmokeAssets(allocator, io, case_root);
            allocator.free(assets.dpo_path);
            break :blk .{
                .model_path = assets.model_dir,
                .reference_path = assets.model_dir,
                .dataset_path = assets.grpo_path,
                .dataset_format = "rendered-text-grpo",
                .backend = "auto",
                .max_examples = 1,
                .max_seq_len = 32,
            };
        },
        .synthetic_gemma_dpo_execute => blk: {
            const assets = try writeSyntheticGemmaSmokeAssets(allocator, io, case_root);
            allocator.free(assets.grpo_path);
            break :blk .{
                .model_path = assets.model_dir,
                .reference_path = assets.model_dir,
                .dataset_path = assets.dpo_path,
                .dataset_format = "rendered-text-preference",
                .backend = "auto",
                .max_examples = 1,
                .max_seq_len = 32,
            };
        },
        .synthetic_gemma_grpo_execute => blk: {
            const assets = try writeSyntheticGemmaSmokeAssets(allocator, io, case_root);
            allocator.free(assets.dpo_path);
            break :blk .{
                .model_path = assets.model_dir,
                .reference_path = assets.model_dir,
                .dataset_path = assets.grpo_path,
                .dataset_format = "rendered-text-grpo",
                .backend = "auto",
                .max_examples = 1,
                .max_seq_len = 32,
            };
        },
    };
}

fn applyFastSmokeRecipeOverrides(recipe: *Recipe, overrides: FastSmokeRecipeOverrides) void {
    if (overrides.model_path) |value| recipe.model.path = value;
    if (overrides.reference_path) |value| recipe.model.reference_path = value;
    if (overrides.dataset_path) |value| recipe.dataset.path = value;
    if (overrides.dataset_format) |value| recipe.dataset.format = value;
    if (overrides.train_path) |value| recipe.dataset.train_path = value;
    if (overrides.eval_path) |value| recipe.dataset.eval_path = value;
    if (overrides.labels) |value| recipe.dataset.labels = value;
    if (overrides.backend) |value| recipe.backend = value;
    if (overrides.max_examples) |value| recipe.dataset.max_examples = value;
    if (overrides.eval_max_examples) |value| recipe.dataset.eval_max_examples = value;
    if (overrides.max_seq_len) |value| recipe.dataset.max_seq_len = value;
}

fn writeSyntheticGliner2SmokeAssets(allocator: std.mem.Allocator, io: std.Io, case_root: []const u8) !SyntheticGlinerAssets {
    const assets_root = try std.fs.path.join(allocator, &.{ case_root, "synthetic_gliner2" });
    defer allocator.free(assets_root);
    try std.Io.Dir.cwd().createDirPath(io, assets_root);

    const model_dir = try std.fs.path.join(allocator, &.{ assets_root, "model" });
    errdefer allocator.free(model_dir);
    const encoder_config_dir = try std.fs.path.join(allocator, &.{ model_dir, "encoder_config" });
    defer allocator.free(encoder_config_dir);
    try std.Io.Dir.cwd().createDirPath(io, encoder_config_dir);

    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "config.json" }),
        \\{"model_name":"synthetic/gliner2-fast-smoke","model_type":"deberta-v3","counting_layer":"count_embed","token_pooling":"first","max_width":4,"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"max_position_embeddings":16,"position_buckets":16,"count_embed_dim":4,"count_embed_layers":1,"count_embed_heads":1,"count_embed_ffn":8,"max_count_embed":8}
    );
    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "encoder_config", "config.json" }),
        \\{"vocab_size":8192,"hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"max_position_embeddings":16,"type_vocab_size":0,"position_buckets":16,"relative_attention":true,"hidden_dropout_prob":0.0,"attention_probs_dropout_prob":0.0,"layer_norm_eps":1e-7}
    );
    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "tokenizer_config.json" }),
        \\{"model_max_length":16,"unk_token":"[UNK]","pad_token":"[PAD]","cls_token":"[CLS]","sep_token":"[SEP]"}
    );
    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "special_tokens_map.json" }),
        \\{"unk_token":"[UNK]","pad_token":"[PAD]","cls_token":"[CLS]","sep_token":"[SEP]"}
    );
    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "tokenizer.json" }),
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"WhitespaceSplit"},
        \\  "post_processor":{"type":"TemplateProcessing","single":[{"SpecialToken":{"id":"[CLS]","type_id":0}},{"Sequence":{"id":"A","type_id":0}},{"SpecialToken":{"id":"[SEP]","type_id":0}}],"special_tokens":{"[CLS]":{"id":"[CLS]","ids":[2],"tokens":["[CLS]"]},"[SEP]":{"id":"[SEP]","ids":[3],"tokens":["[SEP]"]}}},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"},
        \\    {"id":4,"content":"[E]"},
        \\    {"id":5,"content":"[P]"},
        \\    {"id":6,"content":"[SEP_TEXT]"}
        \\  ],
        \\  "model":{
        \\    "type":"Unigram",
        \\    "unk_id":1,
        \\    "vocab":[
        \\      ["[PAD]",0.0],["[UNK]",0.0],["[CLS]",0.0],["[SEP]",0.0],["[E]",0.0],["[P]",0.0],["[SEP_TEXT]",0.0],
        \\      ["john",0.0],["works",0.0],["at",0.0],["acme",0.0],["hello",0.0],["world",0.0],["person",0.0],["organization",0.0],["location",0.0]
        \\    ]
        \\  }
        \\}
    );

    const checkpoint_path = try std.fs.path.join(allocator, &.{ model_dir, gliner2.checkpoint_file_name });
    defer allocator.free(checkpoint_path);
    try writeSyntheticGliner2Checkpoint(allocator, checkpoint_path);

    const train_path = try std.fs.path.join(allocator, &.{ assets_root, "train-00000.jsonl" });
    errdefer allocator.free(train_path);
    const eval_path = try std.fs.path.join(allocator, &.{ assets_root, "eval-00000.jsonl" });
    errdefer allocator.free(eval_path);
    try writeTextFile(io, train_path,
        \\{"text":"john works at acme","entities":[{"text":"john","label":"person","start":0,"end":4},{"text":"acme","label":"organization","start":14,"end":18}]}
        \\{"text":"hello world","entities":[{"text":"world","label":"location","start":6,"end":11}]}
    );
    try writeTextFile(io, eval_path,
        \\{"text":"john works at acme","entities":[{"text":"john","label":"person","start":0,"end":4}]}
    );

    return .{
        .model_dir = model_dir,
        .train_path = train_path,
        .eval_path = eval_path,
        .labels = "person,organization,location",
    };
}

fn writeSyntheticQwen2SmokeAssets(allocator: std.mem.Allocator, io: std.Io, case_root: []const u8) !SyntheticQwen2Assets {
    const assets_root = try std.fs.path.join(allocator, &.{ case_root, "synthetic_qwen2" });
    defer allocator.free(assets_root);
    try std.Io.Dir.cwd().createDirPath(io, assets_root);

    const model_dir = try std.fs.path.join(allocator, &.{ assets_root, "model" });
    errdefer allocator.free(model_dir);
    try std.Io.Dir.cwd().createDirPath(io, model_dir);

    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "config.json" }),
        \\{"model_type":"qwen2","vocab_size":200000,"hidden_size":32,"num_hidden_layers":1,"num_attention_heads":4,"num_key_value_heads":2,"intermediate_size":64,"max_position_embeddings":32,"rope_theta":10000.0,"rms_norm_eps":1e-6,"tie_word_embeddings":true}
    );

    try copySmokeArtifactFromQwenTokenizerBundle(allocator, io, model_dir, "tokenizer.json");
    try copySmokeArtifactFromQwenTokenizerBundle(allocator, io, model_dir, "tokenizer_config.json");
    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "special_tokens_map.json" }),
        \\{"bos_token":"<|endoftext|>","eos_token":"<|im_end|>","pad_token":"<|endoftext|>"}
    );

    const checkpoint_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(checkpoint_path);
    try writeSyntheticQwen2Checkpoint(allocator, checkpoint_path);

    const dpo_path = try std.fs.path.join(allocator, &.{ assets_root, "dpo.jsonl" });
    errdefer allocator.free(dpo_path);
    const grpo_path = try std.fs.path.join(allocator, &.{ assets_root, "grpo.jsonl" });
    errdefer allocator.free(grpo_path);
    try writeTextFile(io, dpo_path,
        \\{"prompt":"Answer with one word: yes or no?\nAnswer:","chosen":" yes","rejected":" no"}
    );
    try writeTextFile(io, grpo_path,
        \\{"prompt":"Answer with one word: yes\nAnswer:","target":"yes"}
    );

    return .{
        .model_dir = model_dir,
        .dpo_path = dpo_path,
        .grpo_path = grpo_path,
    };
}

fn writeSyntheticGemmaSmokeAssets(allocator: std.mem.Allocator, io: std.Io, case_root: []const u8) !SyntheticGemmaAssets {
    const assets_root = try std.fs.path.join(allocator, &.{ case_root, "synthetic_gemma4" });
    defer allocator.free(assets_root);
    try std.Io.Dir.cwd().createDirPath(io, assets_root);

    const model_dir = try std.fs.path.join(allocator, &.{ assets_root, "model" });
    errdefer allocator.free(model_dir);
    try std.Io.Dir.cwd().createDirPath(io, model_dir);

    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "config.json" }),
        \\{"model_type":"gemma4_text","hidden_size":32,"num_hidden_layers":1,"num_attention_heads":4,"num_key_value_heads":2,"attention_head_dim":8,"intermediate_size":64,"max_position_embeddings":32,"rope_theta":10000.0,"rms_norm_eps":1e-6,"tie_word_embeddings":true,"vocab_size":200000}
    );

    try copySmokeArtifactFromQwenTokenizerBundle(allocator, io, model_dir, "tokenizer.json");
    try copySmokeArtifactFromQwenTokenizerBundle(allocator, io, model_dir, "tokenizer_config.json");
    try writeOwnedTextFile(allocator, io, try std.fs.path.join(allocator, &.{ model_dir, "special_tokens_map.json" }),
        \\{"bos_token":"<|endoftext|>","eos_token":"<|im_end|>","pad_token":"<|endoftext|>"}
    );

    const checkpoint_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(checkpoint_path);
    try writeSyntheticGemmaCheckpoint(allocator, checkpoint_path);

    const dpo_path = try std.fs.path.join(allocator, &.{ assets_root, "dpo.jsonl" });
    errdefer allocator.free(dpo_path);
    const grpo_path = try std.fs.path.join(allocator, &.{ assets_root, "grpo.jsonl" });
    errdefer allocator.free(grpo_path);
    try writeTextFile(io, dpo_path,
        \\{"prompt":"Answer with one word: yes or no?\nAnswer:","chosen":" yes","rejected":" no"}
    );
    try writeTextFile(io, grpo_path,
        \\{"prompt":"Answer with one word: yes\nAnswer:","target":"yes"}
    );

    return .{
        .model_dir = model_dir,
        .dpo_path = dpo_path,
        .grpo_path = grpo_path,
    };
}

fn copySmokeArtifactFromQwenTokenizerBundle(
    allocator: std.mem.Allocator,
    io: std.Io,
    out_dir: []const u8,
    file_name: []const u8,
) !void {
    const src_root = "/tmp/antfly-inference-models/Qwen/Qwen2.5-0.5B-Instruct-GGUF";
    const src_path = try std.fs.path.join(allocator, &.{ src_root, file_name });
    defer allocator.free(src_path);
    const dst_path = try std.fs.path.join(allocator, &.{ out_dir, file_name });
    defer allocator.free(dst_path);
    if (c_file.fileExists(allocator, src_path)) {
        const contents = try c_file.readFile(allocator, src_path);
        defer allocator.free(contents);
        try compat.cwd().writeFile(io, .{ .sub_path = dst_path, .data = contents });
        return;
    }

    if (std.mem.eql(u8, file_name, "tokenizer.json")) {
        try writeTextFile(io, dst_path,
            \\{
            \\  "version":"1.0",
            \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
            \\  "pre_tokenizer":{"type":"WhitespaceSplit"},
            \\  "added_tokens":[
            \\    {"id":0,"content":"<pad>"},
            \\    {"id":1,"content":"<unk>"},
            \\    {"id":2,"content":"<bos>"},
            \\    {"id":3,"content":"<eos>"}
            \\  ],
            \\  "model":{
            \\    "type":"Unigram",
            \\    "unk_id":1,
            \\    "vocab":[
            \\      ["<pad>",0.0],["<unk>",0.0],["<bos>",0.0],["<eos>",0.0],
            \\      ["answer",0.0],["with",0.0],["one",0.0],["word:",0.0],["yes",0.0],["or",0.0],["no?",0.0],["answer:",0.0],["no",0.0]
            \\    ]
            \\  }
            \\}
        );
        return;
    }
    if (std.mem.eql(u8, file_name, "tokenizer_config.json")) {
        try writeTextFile(io, dst_path,
            \\{"model_max_length":32,"unk_token":"<unk>","pad_token":"<pad>","bos_token":"<bos>","eos_token":"<eos>"}
        );
        return;
    }
    return error.FileNotFound;
}

fn writeSyntheticQwen2Checkpoint(allocator: std.mem.Allocator, path: []const u8) !void {
    const hidden: usize = 32;
    const intermediate: usize = 64;
    const vocab: usize = 200000;
    const num_layers: usize = 1;
    const q_dim: usize = 32;
    const kv_dim: usize = 16;

    var tensors: std.ArrayList(WriteTensorF32) = .empty;
    defer tensors.deinit(allocator);
    var owned_names: std.ArrayList([]const u8) = .empty;
    defer {
        for (owned_names.items) |name| allocator.free(name);
        owned_names.deinit(allocator);
    }

    try tensors.append(allocator, .{ .name = "model.embed_tokens.weight", .shape = &.{ vocab, hidden }, .data = try makeRampF32(allocator, vocab * hidden, 0.00001) });
    try tensors.append(allocator, .{ .name = "model.norm.weight", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 1.0) });

    var layer: usize = 0;
    while (layer < num_layers) : (layer += 1) {
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.input_layernorm.weight", layer, &.{hidden}, try makeFilledF32(allocator, hidden, 1.0));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.q_proj.weight", layer, &.{ q_dim, hidden }, try makeRampF32(allocator, q_dim * hidden, 0.0002));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.q_proj.bias", layer, &.{q_dim}, try makeFilledF32(allocator, q_dim, 0.0));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.k_proj.weight", layer, &.{ kv_dim, hidden }, try makeRampF32(allocator, kv_dim * hidden, 0.00025));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.k_proj.bias", layer, &.{kv_dim}, try makeFilledF32(allocator, kv_dim, 0.0));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.v_proj.weight", layer, &.{ kv_dim, hidden }, try makeRampF32(allocator, kv_dim * hidden, 0.0003));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.v_proj.bias", layer, &.{kv_dim}, try makeFilledF32(allocator, kv_dim, 0.0));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.o_proj.weight", layer, &.{ hidden, q_dim }, try makeRampF32(allocator, hidden * q_dim, 0.00035));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.post_attention_layernorm.weight", layer, &.{hidden}, try makeFilledF32(allocator, hidden, 1.0));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.mlp.gate_proj.weight", layer, &.{ intermediate, hidden }, try makeRampF32(allocator, intermediate * hidden, 0.0004));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.mlp.up_proj.weight", layer, &.{ intermediate, hidden }, try makeRampF32(allocator, intermediate * hidden, 0.00045));
        try appendOwnedQwenTensor(allocator, &tensors, &owned_names, "model.layers.{d}.mlp.down_proj.weight", layer, &.{ hidden, intermediate }, try makeRampF32(allocator, hidden * intermediate, 0.0005));
    }

    try writeHeaderAndTensorsF32(allocator, path, tensors.items);
}

fn writeSyntheticGemmaCheckpoint(allocator: std.mem.Allocator, path: []const u8) !void {
    const hidden: usize = 32;
    const intermediate: usize = 64;
    const vocab: usize = 200000;
    const num_layers: usize = 1;
    const q_dim: usize = 32;
    const kv_dim: usize = 16;
    const head_dim: usize = 8;

    var tensors: std.ArrayList(WriteTensorF32) = .empty;
    defer tensors.deinit(allocator);
    var owned_names: std.ArrayList([]const u8) = .empty;
    defer {
        for (owned_names.items) |name| allocator.free(name);
        owned_names.deinit(allocator);
    }

    try tensors.append(allocator, .{ .name = "model.embed_tokens.weight", .shape = &.{ vocab, hidden }, .data = try makeRampF32(allocator, vocab * hidden, 0.00001) });
    try tensors.append(allocator, .{ .name = "model.norm.weight", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 1.0) });

    var layer: usize = 0;
    while (layer < num_layers) : (layer += 1) {
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.input_layernorm.weight", layer, &.{hidden}, try makeFilledF32(allocator, hidden, 1.0));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.q_proj.weight", layer, &.{ q_dim, hidden }, try makeRampF32(allocator, q_dim * hidden, 0.0002));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.k_proj.weight", layer, &.{ kv_dim, hidden }, try makeRampF32(allocator, kv_dim * hidden, 0.00025));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.v_proj.weight", layer, &.{ kv_dim, hidden }, try makeRampF32(allocator, kv_dim * hidden, 0.0003));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.o_proj.weight", layer, &.{ hidden, q_dim }, try makeRampF32(allocator, hidden * q_dim, 0.00035));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.q_norm.weight", layer, &.{head_dim}, try makeFilledF32(allocator, head_dim, 1.0));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.self_attn.k_norm.weight", layer, &.{head_dim}, try makeFilledF32(allocator, head_dim, 1.0));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.post_attention_layernorm.weight", layer, &.{hidden}, try makeFilledF32(allocator, hidden, 1.0));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.pre_feedforward_layernorm.weight", layer, &.{hidden}, try makeFilledF32(allocator, hidden, 1.0));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.mlp.gate_proj.weight", layer, &.{ intermediate, hidden }, try makeRampF32(allocator, intermediate * hidden, 0.0004));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.mlp.up_proj.weight", layer, &.{ intermediate, hidden }, try makeRampF32(allocator, intermediate * hidden, 0.00045));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.mlp.down_proj.weight", layer, &.{ hidden, intermediate }, try makeRampF32(allocator, hidden * intermediate, 0.0005));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.post_feedforward_layernorm.weight", layer, &.{hidden}, try makeFilledF32(allocator, hidden, 1.0));
        try appendOwnedGemmaTensor(allocator, &tensors, &owned_names, "model.layers.{d}.layer_scalar", layer, &.{1}, try makeFilledF32(allocator, 1, 1.0));
    }

    try writeHeaderAndTensorsF32(allocator, path, tensors.items);
}

fn appendOwnedQwenTensor(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayList(WriteTensorF32),
    owned_names: *std.ArrayList([]const u8),
    comptime fmt: []const u8,
    layer: usize,
    shape: []const usize,
    data: []f32,
) !void {
    const name = try std.fmt.allocPrint(allocator, fmt, .{layer});
    try owned_names.append(allocator, name);
    try tensors.append(allocator, .{
        .name = name,
        .shape = shape,
        .data = data,
    });
}

fn appendOwnedGemmaTensor(
    allocator: std.mem.Allocator,
    tensors: *std.ArrayList(WriteTensorF32),
    owned_names: *std.ArrayList([]const u8),
    comptime fmt: []const u8,
    layer: usize,
    shape: []const usize,
    data: []f32,
) !void {
    const name = try std.fmt.allocPrint(allocator, fmt, .{layer});
    try owned_names.append(allocator, name);
    try tensors.append(allocator, .{
        .name = name,
        .shape = shape,
        .data = data,
    });
}

fn writeSyntheticGliner2Checkpoint(allocator: std.mem.Allocator, path: []const u8) !void {
    const hidden: usize = 4;
    const intermediate: usize = 8;
    const vocab: usize = 8192;
    const positions: usize = 16;

    try writeHeaderAndTensorsF32(allocator, path, &.{
        .{ .name = "deberta.embeddings.word_embeddings.weight", .shape = &.{ vocab, hidden }, .data = try makeRampF32(allocator, vocab * hidden, 0.001) },
        .{ .name = "deberta.embeddings.LayerNorm.weight", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 1.0) },
        .{ .name = "deberta.embeddings.LayerNorm.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.rel_embeddings.weight", .shape = &.{ positions, hidden }, .data = try makeRampF32(allocator, positions * hidden, 0.0005) },
        .{ .name = "deberta.encoder.LayerNorm.weight", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 1.0) },
        .{ .name = "deberta.encoder.LayerNorm.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ hidden, hidden }, .data = try makeRampF32(allocator, hidden * hidden, 0.001) },
        .{ .name = "deberta.encoder.layer.0.attention.self.query_proj.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ hidden, hidden }, .data = try makeRampF32(allocator, hidden * hidden, 0.0015) },
        .{ .name = "deberta.encoder.layer.0.attention.self.key_proj.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ hidden, hidden }, .data = try makeRampF32(allocator, hidden * hidden, 0.002) },
        .{ .name = "deberta.encoder.layer.0.attention.self.value_proj.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.attention.output.dense.weight", .shape = &.{ hidden, hidden }, .data = try makeRampF32(allocator, hidden * hidden, 0.0025) },
        .{ .name = "deberta.encoder.layer.0.attention.output.dense.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 1.0) },
        .{ .name = "deberta.encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.intermediate.dense.weight", .shape = &.{ intermediate, hidden }, .data = try makeRampF32(allocator, intermediate * hidden, 0.001) },
        .{ .name = "deberta.encoder.layer.0.intermediate.dense.bias", .shape = &.{intermediate}, .data = try makeFilledF32(allocator, intermediate, 0.0) },
        .{ .name = "deberta.encoder.layer.0.output.dense.weight", .shape = &.{ hidden, intermediate }, .data = try makeRampF32(allocator, hidden * intermediate, 0.001) },
        .{ .name = "deberta.encoder.layer.0.output.dense.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
        .{ .name = "deberta.encoder.layer.0.output.LayerNorm.weight", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 1.0) },
        .{ .name = "deberta.encoder.layer.0.output.LayerNorm.bias", .shape = &.{hidden}, .data = try makeFilledF32(allocator, hidden, 0.0) },
    });
}

fn writeOwnedTextFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8, contents: []const u8) !void {
    defer allocator.free(path);
    try writeTextFile(io, path, contents);
}

fn writeTextFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    if (std.fs.path.dirname(path)) |dir_name| try compat.cwd().createDirPath(io, dir_name);
    try compat.cwd().writeFile(io, .{ .sub_path = path, .data = contents });
}

fn writeHeaderAndTensorsF32(allocator: std.mem.Allocator, path: []const u8, tensors: []const WriteTensorF32) !void {
    defer {
        for (tensors) |tensor| allocator.free(tensor.data);
    }

    var header_buf: std.Io.Writer.Allocating = .init(allocator);
    defer header_buf.deinit();
    const writer = &header_buf.writer;
    try writer.writeByte('{');
    var offset: u64 = 0;
    for (tensors, 0..) |tensor, idx| {
        if (idx != 0) try writer.writeByte(',');
        const byte_len = tensor.data.len * @sizeOf(f32);
        try writer.print("\"{s}\":{{\"dtype\":\"F32\",\"shape\":[", .{tensor.name});
        for (tensor.shape, 0..) |dim, dim_idx| {
            if (dim_idx != 0) try writer.writeByte(',');
            try writer.print("{}", .{dim});
        }
        try writer.print("],\"data_offsets\":[{},{}]}}", .{ offset, offset + byte_len });
        offset += byte_len;
    }
    try writer.writeByte('}');

    var file = try compat.cwd().createFile(compat.io(), path, .{ .truncate = true });
    defer file.close(compat.io());

    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, header_buf.written().len, .little);
    try file.writeStreamingAll(compat.io(), &len_buf);
    try file.writeStreamingAll(compat.io(), header_buf.written());
    for (tensors) |tensor| try file.writeStreamingAll(compat.io(), std.mem.sliceAsBytes(tensor.data));
}

fn makeFilledF32(allocator: std.mem.Allocator, len: usize, value: f32) ![]f32 {
    const data = try allocator.alloc(f32, len);
    @memset(data, value);
    return data;
}

fn makeRampF32(allocator: std.mem.Allocator, len: usize, scale: f32) ![]f32 {
    const data = try allocator.alloc(f32, len);
    for (data, 0..) |*item, idx| item.* = @as(f32, @floatFromInt(idx + 1)) * scale;
    return data;
}

fn buildPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    const kind = try parseKind(recipe.recipe orelse recipe.kind orelse return error.MissingRecipeKind);
    const family = recipe.model.family orelse try inferFamily(recipe);

    return switch (kind) {
        .sft, .lora_sft, .qlora_sft => try buildLoraSftPlan(allocator, recipe, family),
        .dpo => try buildDpoPlan(allocator, recipe),
        .grpo => try buildGrpoPlan(allocator, recipe),
        .reranker => try buildRerankerPlan(allocator, recipe, family),
        .vlm_retrieval => try buildVlmRetrievalPlan(allocator, recipe, family),
    };
}

fn buildLoraSftPlan(allocator: std.mem.Allocator, recipe: Recipe, family: []const u8) !Plan {
    if (isQwen35Family(family)) return buildQwen35TextSftPlan(allocator, recipe);
    if (eqlAny(family, &.{ "gemma4", "gemma" })) {
        return buildGemma4LoraPlan(allocator, recipe);
    }
    if (eqlAny(family, &.{"gliner2"})) {
        return buildGliner2LoraPlan(allocator, recipe);
    }
    if (eqlAny(family, &.{"layoutlmv3"})) {
        return buildLayoutLmv3LoraPlan(allocator, recipe);
    }
    if (eqlAny(family, &.{ "reranker", "text-reranker", "deberta", "modernbert" })) {
        return buildRerankerLoraPlan(allocator, recipe);
    }
    return error.UnsupportedRecipeFamily;
}

fn buildQwen35TextSftPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    _ = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    return .{ .steps = try allocator.dupe(Step, &.{
        .{
            .kind = .direct_sft,
            .name = "train-eval",
            .argv = try argv(allocator, &.{"antfly-inference-internal-sft"}),
        },
    }) };
}

fn buildGemma4LoraPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    const model_path = recipe.model.path orelse return error.MissingModelPath;
    const dataset_path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const prepared_path = recipe.artifacts.prepared_path orelse recipe.dataset.prepared_path orelse try defaultArtifactPath(allocator, recipe, "prepared_inputs.json");
    const bootstrap_dir = adapterBootstrapDir(recipe) orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    const trained_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    const adapter = recipe.adapter orelse AdapterConfig{};

    var steps: std.ArrayList(Step) = .empty;
    errdefer freeSteps(allocator, steps.items);

    var prepare_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &prepare_argv, &.{
        "prepare-gemma4-lora-inputs",
        model_path,
        dataset_path,
        recipe.dataset.train_split orelse "-",
        prepared_path,
        "--max-examples",
        try fmtInt(allocator, recipe.dataset.max_examples orelse 0),
        "--max-seq-len",
        try fmtInt(allocator, recipe.dataset.max_seq_len orelse 512),
    });
    if (recipe.model.projector_path) |path| {
        try appendMany(allocator, &prepare_argv, &.{ "--gguf-projector", path });
    }
    try steps.append(allocator, .{ .name = "prepare", .argv = try prepare_argv.toOwnedSlice(allocator) });

    var bootstrap_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &bootstrap_argv, &.{
        "bootstrap-gemma4-lora",
        model_path,
        bootstrap_dir,
    });
    try appendGemmaBootstrapAdapterArgs(allocator, &bootstrap_argv, adapter, .lora_sft);
    try steps.append(allocator, .{ .name = "bootstrap-adapter", .argv = try bootstrap_argv.toOwnedSlice(allocator) });

    var train_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &train_argv, &.{
        "train-eval-gemma4-lora-bundle",
        model_path,
        bootstrap_dir,
        prepared_path,
        trained_dir,
        "--lr",
        try fmtFloat(allocator, recipe.optimizer.learning_rate orelse 0.001),
        "--max-examples",
        try fmtInt(allocator, recipe.dataset.max_examples orelse 32),
        "--epochs",
        try fmtInt(allocator, recipe.optimizer.epochs orelse 1),
    });
    if (recipe.eval) |eval| if (eval.max_examples) |max| try appendMany(allocator, &train_argv, &.{ "--eval-max-examples", try fmtInt(allocator, max) });
    if (adapter.layer_name) |layer| try appendMany(allocator, &train_argv, &.{ "--layer-name", layer });
    if (recipe.optimizer.gradient_accumulation_steps) |steps_count| try appendMany(allocator, &train_argv, &.{ "--grad-accum", try fmtInt(allocator, steps_count) });
    if (recipe.optimizer.max_grad_norm) |norm| try appendMany(allocator, &train_argv, &.{ "--max-grad-norm", try fmtFloat(allocator, norm) });
    if (recipe.optimizer.llrd_decay) |decay| try appendMany(allocator, &train_argv, &.{ "--llrd-decay", try fmtFloat(allocator, decay) });
    if (recipe.optimizer.schedule_free orelse false) try train_argv.append(allocator, "--schedule-free");
    if (recipe.backend) |backend| try appendMany(allocator, &train_argv, &.{ "--backend", backend });
    if (recipe.trainer) |trainer| try appendMany(allocator, &train_argv, &.{ "--trainer", trainer });
    if (recipe.model.projector_path) |path| try appendMany(allocator, &train_argv, &.{ "--gguf-projector", path });
    try steps.append(allocator, .{ .name = "train-eval", .argv = try train_argv.toOwnedSlice(allocator) });

    return .{ .steps = try steps.toOwnedSlice(allocator) };
}

fn buildGliner2LoraPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    const model_path = recipe.model.path orelse return error.MissingModelPath;
    const train_path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const eval_path = evalDatasetPath(recipe);
    const train_cache_path = trainCachePath(recipe) orelse recipe.artifacts.prepared_path orelse try defaultArtifactPath(allocator, recipe, "gliner2_train_boundary_cache.json");
    const eval_cache_path = evalCachePath(recipe) orelse if (eval_path != null) try defaultArtifactPath(allocator, recipe, "gliner2_eval_boundary_cache.json") else train_cache_path;
    const bootstrap_dir = adapterBootstrapDir(recipe) orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    const trained_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    const adapter = recipe.adapter orelse AdapterConfig{};

    var steps: std.ArrayList(Step) = .empty;
    errdefer freeSteps(allocator, steps.items);
    var bootstrap_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &bootstrap_argv, &.{ "bootstrap-gliner2-lora", model_path, bootstrap_dir });
    try appendGenericBootstrapAdapterArgs(allocator, &bootstrap_argv, adapter, .lora_sft);
    try steps.append(allocator, .{ .name = "bootstrap-adapter", .argv = try bootstrap_argv.toOwnedSlice(allocator) });
    const entity_types = recipe.dataset.labels orelse recipe.dataset.format orelse return error.MissingEntityTypes;
    try steps.append(allocator, .{ .name = "prepare", .argv = try argv(allocator, &.{
        "prepare-gliner2-top-layer-boundary-cache",
        model_path,
        train_path,
        entity_types,
        train_cache_path,
        recipe.dataset.train_split orelse "train",
        recipe.backend orelse "native",
        try fmtInt(allocator, recipe.dataset.max_examples orelse 128),
        try fmtInt(allocator, recipe.dataset.max_seq_len orelse 256),
        "8",
        "1",
    }) });
    if (eval_path) |path| {
        try steps.append(allocator, .{ .name = "prepare-eval", .argv = try argv(allocator, &.{
            "prepare-gliner2-top-layer-boundary-cache",
            model_path,
            path,
            entity_types,
            eval_cache_path,
            recipe.dataset.eval_split orelse "eval",
            recipe.backend orelse "native",
            try fmtInt(allocator, recipe.dataset.eval_max_examples orelse evalMaxExamples(recipe) orelse 128),
            try fmtInt(allocator, recipe.dataset.max_seq_len orelse 256),
            "8",
            "1",
        }) });
    }
    try steps.append(allocator, .{ .name = "train-eval", .argv = try argv(allocator, &.{
        "train-eval-gliner2-lora-bundle", model_path,                                                           bootstrap_dir,    train_cache_path,                                             eval_cache_path, trained_dir,
        "--lr",                           try fmtFloat(allocator, recipe.optimizer.learning_rate orelse 0.001), "--max-examples", try fmtInt(allocator, recipe.dataset.max_examples orelse 32), "--epochs",      try fmtInt(allocator, recipe.optimizer.epochs orelse 1),
        "--backend",                      recipe.backend orelse "native",
    }) });
    if (recipe.artifacts.materialized_dir) |out_dir| {
        try steps.append(allocator, .{ .name = "materialize", .argv = try argv(allocator, &.{
            "materialize-gliner2-lora", model_path, trained_dir, out_dir,
        }) });
    }
    return .{ .steps = try steps.toOwnedSlice(allocator) };
}

fn buildLayoutLmv3LoraPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    const model_path = recipe.model.path orelse return error.MissingModelPath;
    const train_path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const eval_path = evalDatasetPath(recipe) orelse train_path;
    const bootstrap_dir = adapterBootstrapDir(recipe) orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    const trained_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    const task = recipe.dataset.format orelse "sequence";
    const train_cmd = if (std.mem.eql(u8, task, "token")) "train-eval-layoutlmv3-lora-token" else "train-eval-layoutlmv3-lora-sequence";
    const adapter = recipe.adapter orelse AdapterConfig{};

    var steps: std.ArrayList(Step) = .empty;
    errdefer freeSteps(allocator, steps.items);
    var bootstrap_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &bootstrap_argv, &.{ "bootstrap-layoutlmv3-lora", model_path, bootstrap_dir });
    try appendGenericBootstrapAdapterArgs(allocator, &bootstrap_argv, adapter, .lora_sft);
    try steps.append(allocator, .{ .name = "bootstrap-adapter", .argv = try bootstrap_argv.toOwnedSlice(allocator) });
    try steps.append(allocator, .{ .name = "train-eval", .argv = try argv(allocator, &.{
        train_cmd,                                                    model_path,                                                           bootstrap_dir,                                            train_path,                                              eval_path, trained_dir,
        try fmtInt(allocator, recipe.dataset.max_examples orelse 32), try fmtFloat(allocator, recipe.optimizer.learning_rate orelse 0.001), try fmtInt(allocator, evalMaxExamples(recipe) orelse 32), try fmtInt(allocator, recipe.optimizer.epochs orelse 1),
    }) });
    if (recipe.artifacts.materialized_dir) |out_dir| {
        try steps.append(allocator, .{ .name = "materialize", .argv = try argv(allocator, &.{
            "materialize-layoutlmv3-checkpoint", model_path, trained_dir, task, out_dir,
        }) });
    }
    return .{ .steps = try steps.toOwnedSlice(allocator) };
}

fn buildRerankerPlan(allocator: std.mem.Allocator, recipe: Recipe, family: []const u8) !Plan {
    if (eqlAny(family, &.{ "reranker", "text-reranker", "deberta", "modernbert" })) {
        const model_path = recipe.model.path orelse return error.MissingModelPath;
        const train_path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
        const eval_path = evalDatasetPath(recipe);
        const train_cache_path = trainCachePath(recipe) orelse recipe.artifacts.prepared_path orelse try defaultArtifactPath(allocator, recipe, "reranker_train_pooled_cache.json");
        const eval_cache_path = evalCachePath(recipe) orelse if (eval_path != null) try defaultArtifactPath(allocator, recipe, "reranker_eval_pooled_cache.json") else train_cache_path;
        const out_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "reranker-head");
        var steps: std.ArrayList(Step) = .empty;
        errdefer freeSteps(allocator, steps.items);
        try steps.append(allocator, .{ .name = "prepare", .argv = try argv(allocator, &.{
            "prepare-reranker-pooled-cache", model_path, train_path, train_cache_path, recipe.dataset.train_split orelse "train", "--backend", recipe.backend orelse "auto", "--max-examples", try fmtInt(allocator, recipe.dataset.max_examples orelse 256),
        }) });
        if (eval_path) |path| {
            try steps.append(allocator, .{ .name = "prepare-eval", .argv = try argv(allocator, &.{
                "prepare-reranker-pooled-cache", model_path, path, eval_cache_path, recipe.dataset.eval_split orelse "eval", "--backend", recipe.backend orelse "auto", "--max-examples", try fmtInt(allocator, evalMaxExamples(recipe) orelse 256),
            }) });
        }
        try steps.append(allocator, .{ .name = "train-eval", .argv = try argv(allocator, &.{
            "train-eval-reranker-head-cached",                             model_path,                                                           train_cache_path,             eval_cache_path,                                         out_dir,
            "--learning-rate",                                             try fmtFloat(allocator, recipe.optimizer.learning_rate orelse 0.001), "--epochs",                   try fmtInt(allocator, recipe.optimizer.epochs orelse 1), "--max-examples",
            try fmtInt(allocator, recipe.dataset.max_examples orelse 256), "--backend",                                                          recipe.backend orelse "auto",
        }) });
        if (recipe.artifacts.materialized_dir) |materialized_dir| {
            try steps.append(allocator, .{ .name = "materialize", .argv = try argv(allocator, &.{
                "materialize-reranker-head", model_path, out_dir, materialized_dir,
            }) });
        }
        return .{ .steps = try steps.toOwnedSlice(allocator) };
    }
    return error.UnsupportedRecipeFamily;
}

fn buildRerankerLoraPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    const model_path = recipe.model.path orelse return error.MissingModelPath;
    const train_path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const eval_path = evalDatasetPath(recipe);
    const train_cache_path = trainCachePath(recipe) orelse recipe.artifacts.prepared_path orelse try defaultArtifactPath(allocator, recipe, "reranker_train_top_layer_cache.json");
    const eval_cache_path = evalCachePath(recipe) orelse if (eval_path != null) try defaultArtifactPath(allocator, recipe, "reranker_eval_top_layer_cache.json") else train_cache_path;
    const bootstrap_dir = adapterBootstrapDir(recipe) orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    const trained_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    const adapter = recipe.adapter orelse AdapterConfig{};
    var steps: std.ArrayList(Step) = .empty;
    errdefer freeSteps(allocator, steps.items);
    var bootstrap_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &bootstrap_argv, &.{ "bootstrap-reranker-lora", model_path, bootstrap_dir });
    try appendGenericBootstrapAdapterArgs(allocator, &bootstrap_argv, adapter, .lora_sft);
    try steps.append(allocator, .{ .name = "bootstrap-adapter", .argv = try bootstrap_argv.toOwnedSlice(allocator) });
    try steps.append(allocator, .{ .name = "prepare", .argv = try argv(allocator, &.{
        "prepare-reranker-top-layer-cache", model_path, train_path, train_cache_path, recipe.dataset.train_split orelse "train", "--backend", recipe.backend orelse "auto", "--max-examples", try fmtInt(allocator, recipe.dataset.max_examples orelse 128),
    }) });
    if (eval_path) |path| {
        try steps.append(allocator, .{ .name = "prepare-eval", .argv = try argv(allocator, &.{
            "prepare-reranker-top-layer-cache", model_path, path, eval_cache_path, recipe.dataset.eval_split orelse "eval", "--backend", recipe.backend orelse "auto", "--max-examples", try fmtInt(allocator, evalMaxExamples(recipe) orelse 128),
        }) });
    }
    const head_input = recipe.artifacts.report_path orelse return error.MissingRerankerHeadInput;
    try steps.append(allocator, .{ .name = "train-eval", .argv = try argv(allocator, &.{
        "train-eval-reranker-lora-top-layer-cached-surrogate", model_path,                                                           bootstrap_dir, head_input,                                              train_cache_path, eval_cache_path,                                               trained_dir,
        "--learning-rate",                                     try fmtFloat(allocator, recipe.optimizer.learning_rate orelse 0.001), "--epochs",    try fmtInt(allocator, recipe.optimizer.epochs orelse 1), "--max-examples", try fmtInt(allocator, recipe.dataset.max_examples orelse 128), "--backend",
        recipe.backend orelse "auto",
    }) });
    if (recipe.artifacts.materialized_dir) |out_dir| {
        try steps.append(allocator, .{ .name = "materialize", .argv = try argv(allocator, &.{
            "materialize-reranker-lora", model_path, trained_dir, out_dir,
        }) });
    }
    return .{ .steps = try steps.toOwnedSlice(allocator) };
}

fn buildVlmRetrievalPlan(allocator: std.mem.Allocator, recipe: Recipe, family: []const u8) !Plan {
    if (!eqlAny(family, &.{ "colqwen2", "colqwen", "qwen2vl" })) return error.UnsupportedRecipeFamily;
    const model_path = recipe.model.path orelse return error.MissingModelPath;
    const dataset_path = recipe.dataset.path orelse return error.MissingDatasetRoot;
    const examples_jsonl = trainDatasetPath(recipe) orelse recipe.dataset.prepared_path orelse return error.MissingExamplesJsonl;
    const prepared_path = recipe.artifacts.prepared_path orelse try defaultArtifactPath(allocator, recipe, "colqwen2_inputs.json");
    const bootstrap_dir = adapterBootstrapDir(recipe) orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    const trained_dir = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    const adapter = recipe.adapter orelse AdapterConfig{};

    var steps: std.ArrayList(Step) = .empty;
    errdefer freeSteps(allocator, steps.items);
    try steps.append(allocator, .{ .name = "prepare", .argv = try argv(allocator, &.{
        "prepare-colqwen2-inputs", model_path, dataset_path, examples_jsonl, prepared_path, try fmtInt(allocator, recipe.dataset.max_examples orelse 32),
    }) });
    var bootstrap_argv: std.ArrayList([]const u8) = .empty;
    try appendMany(allocator, &bootstrap_argv, &.{ "bootstrap-colqwen2-lora", model_path, bootstrap_dir });
    try appendGenericBootstrapAdapterArgs(allocator, &bootstrap_argv, adapter, .vlm_retrieval);
    try steps.append(allocator, .{ .name = "bootstrap-adapter", .argv = try bootstrap_argv.toOwnedSlice(allocator) });
    try steps.append(allocator, .{ .name = "train-eval", .argv = try argv(allocator, &.{
        "train-eval-colqwen2-lora-bundle",                       model_path,                                                           bootstrap_dir,    prepared_path,                                                trained_dir,
        "--lr",                                                  try fmtFloat(allocator, recipe.optimizer.learning_rate orelse 0.001), "--max-examples", try fmtInt(allocator, recipe.dataset.max_examples orelse 32), "--epochs",
        try fmtInt(allocator, recipe.optimizer.epochs orelse 1),
    }) });
    return .{ .steps = try steps.toOwnedSlice(allocator) };
}

fn buildDpoPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    _ = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    return .{ .steps = try allocator.dupe(Step, &.{
        .{
            .kind = .direct_dpo,
            .name = "train-eval",
            .argv = try argv(allocator, &.{"antfly-inference-internal-dpo"}),
        },
    }) };
}

fn buildGrpoPlan(allocator: std.mem.Allocator, recipe: Recipe) !Plan {
    _ = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    return .{ .steps = try allocator.dupe(Step, &.{
        .{
            .kind = .direct_grpo,
            .name = "train-eval",
            .argv = try argv(allocator, &.{"antfly-inference-internal-grpo"}),
        },
    }) };
}

fn runPlan(
    allocator: std.mem.Allocator,
    io: std.Io,
    exe_dir: []const u8,
    recipe: Recipe,
    plan: Plan,
    manifest_path: []const u8,
    training_config_path: []const u8,
    training_report_path: []const u8,
) !void {
    var step_manifests = try initStepManifests(allocator, plan);
    defer allocator.free(step_manifests);
    var static_meta_arena = std.heap.ArenaAllocator.init(allocator);
    defer static_meta_arena.deinit();
    const static_metadata = try collectStaticMetadata(static_meta_arena.allocator(), io, recipe);
    try writeTrainingConfig(allocator, io, training_config_path, .{
        .recipe = recipe,
        .steps = step_manifests,
        .metadata = static_metadata,
    });
    try writeRunManifest(allocator, io, manifest_path, .{
        .status = .planned,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
    });
    try writeTrainingReport(allocator, io, training_report_path, .{
        .status = .planned,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
        .metadata = .{
            .dataset_fingerprints = static_metadata.dataset_fingerprints,
            .backend = static_metadata.backend,
            .optimizer = static_metadata.optimizer,
        },
    });

    for (plan.steps, 0..) |step, idx| {
        step_manifests[idx].status = .running;
        try writeRunManifest(allocator, io, manifest_path, .{
            .status = .running,
            .recipe = recipe,
            .artifact_root = recipe.artifacts.root,
            .steps = step_manifests,
        });
        try writeTrainingReport(allocator, io, training_report_path, .{
            .status = .running,
            .recipe = recipe,
            .artifact_root = recipe.artifacts.root,
            .steps = step_manifests,
            .metadata = .{
                .dataset_fingerprints = static_metadata.dataset_fingerprints,
                .backend = static_metadata.backend,
                .optimizer = static_metadata.optimizer,
            },
        });

        print("finetune[{d}/{d}] {s}: ", .{ idx + 1, plan.steps.len, step.name });
        switch (step.kind) {
            .direct_sft => {
                print("{s}\n", .{step.argv[0]});
                const report_path = try sftReportPath(allocator, recipe);
                defer allocator.free(report_path);
                runDirectSft(allocator, io, recipe, report_path) catch |err| {
                    step_manifests[idx].status = .failed;
                    try writeRunManifest(allocator, io, manifest_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                    });
                    try writeTrainingReport(allocator, io, training_report_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                        .metadata = try collectReportMetadata(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
                    });
                    return err;
                };
                step_manifests[idx].stdout_bytes = 0;
                step_manifests[idx].stderr_bytes = 0;
                step_manifests[idx].exit_code = 0;
                step_manifests[idx].status = .succeeded;
                if (idx + 1 == plan.steps.len) {
                    try writeSucceededRunStatus(allocator, io, recipe, step_manifests, manifest_path, training_report_path, static_metadata);
                }
                continue;
            },
            .direct_dpo => {
                print("{s}\n", .{step.argv[0]});
                const report_path = try dpoReportPath(allocator, recipe);
                defer allocator.free(report_path);
                runDirectDpo(allocator, io, recipe, report_path) catch |err| {
                    step_manifests[idx].status = .failed;
                    try writeRunManifest(allocator, io, manifest_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                    });
                    try writeTrainingReport(allocator, io, training_report_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                        .metadata = try collectReportMetadata(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
                    });
                    return err;
                };
                step_manifests[idx].stdout_bytes = 0;
                step_manifests[idx].stderr_bytes = 0;
                step_manifests[idx].exit_code = 0;
                step_manifests[idx].status = .succeeded;
                if (idx + 1 == plan.steps.len) {
                    try writeSucceededRunStatus(allocator, io, recipe, step_manifests, manifest_path, training_report_path, static_metadata);
                }
                continue;
            },
            .direct_grpo => {
                print("{s}\n", .{step.argv[0]});
                const report_path = try grpoReportPath(allocator, recipe);
                defer allocator.free(report_path);
                runDirectGrpo(allocator, io, recipe, report_path) catch |err| {
                    step_manifests[idx].status = .failed;
                    try writeRunManifest(allocator, io, manifest_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                    });
                    try writeTrainingReport(allocator, io, training_report_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                        .metadata = try collectReportMetadata(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
                    });
                    return err;
                };
                step_manifests[idx].stdout_bytes = 0;
                step_manifests[idx].stderr_bytes = 0;
                step_manifests[idx].exit_code = 0;
                step_manifests[idx].status = .succeeded;
                if (idx + 1 == plan.steps.len) {
                    try writeSucceededRunStatus(allocator, io, recipe, step_manifests, manifest_path, training_report_path, static_metadata);
                }
                continue;
            },
            .command => {},
        }

        const direct_adapter_ran = runDirectCommandAdapter(allocator, io, recipe, step) catch |err| {
            step_manifests[idx].status = .failed;
            try writeFailedRunStatus(allocator, io, recipe, plan, step_manifests, manifest_path, training_config_path, training_report_path, static_metadata);
            return err;
        };
        if (direct_adapter_ran) {
            step_manifests[idx].stdout_bytes = 0;
            step_manifests[idx].stderr_bytes = 0;
            step_manifests[idx].exit_code = 0;
            step_manifests[idx].status = .succeeded;
            continue;
        }

        const peer_path = try std.fs.path.join(allocator, &.{ exe_dir, step.argv[0] });
        defer allocator.free(peer_path);

        var full_argv: std.ArrayList([]const u8) = .empty;
        defer full_argv.deinit(allocator);
        var cwd: std.process.Child.Cwd = .inherit;
        if (std.Io.Dir.cwd().access(io, peer_path, .{})) |_| {
            try full_argv.append(allocator, peer_path);
            try full_argv.appendSlice(allocator, step.argv[1..]);
        } else |_| {
            const pkg_root = try installedPackageRoot(allocator, exe_dir);
            defer allocator.free(pkg_root);
            try full_argv.appendSlice(allocator, &.{ "zig", "build", step.argv[0], "--" });
            try full_argv.appendSlice(allocator, step.argv[1..]);
            cwd = .{ .path = pkg_root };
        }

        for (full_argv.items, 0..) |part, part_idx| {
            if (part_idx != 0) print(" ", .{});
            print("{s}", .{part});
        }
        print("\n", .{});
        const result = std.process.run(allocator, io, .{
            .argv = full_argv.items,
            .cwd = cwd,
            .stdout_limit = .limited(16 * 1024 * 1024),
            .stderr_limit = .limited(16 * 1024 * 1024),
        }) catch |err| {
            step_manifests[idx].status = .failed;
            try writeRunManifest(allocator, io, manifest_path, .{
                .status = .failed,
                .recipe = recipe,
                .artifact_root = recipe.artifacts.root,
                .steps = step_manifests,
            });
            try writeTrainingReport(allocator, io, training_report_path, .{
                .status = .failed,
                .recipe = recipe,
                .artifact_root = recipe.artifacts.root,
                .steps = step_manifests,
                .metadata = try collectReportMetadata(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
            });
            return err;
        };
        defer allocator.free(result.stdout);
        defer allocator.free(result.stderr);
        if (result.stdout.len > 0) print("{s}", .{result.stdout});
        if (result.stderr.len > 0) print("{s}", .{result.stderr});
        step_manifests[idx].stdout_bytes = result.stdout.len;
        step_manifests[idx].stderr_bytes = result.stderr.len;
        switch (result.term) {
            .exited => |code| {
                step_manifests[idx].exit_code = code;
                step_manifests[idx].status = if (code == 0) .succeeded else .failed;
                if (code != 0) {
                    try writeRunManifest(allocator, io, manifest_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                    });
                    try writeTrainingReport(allocator, io, training_report_path, .{
                        .status = .failed,
                        .recipe = recipe,
                        .artifact_root = recipe.artifacts.root,
                        .steps = step_manifests,
                        .metadata = try collectReportMetadata(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
                    });
                    return error.FinetuneStepFailed;
                }
            },
            else => {
                step_manifests[idx].status = .failed;
                try writeRunManifest(allocator, io, manifest_path, .{
                    .status = .failed,
                    .recipe = recipe,
                    .artifact_root = recipe.artifacts.root,
                    .steps = step_manifests,
                });
                try writeTrainingReport(allocator, io, training_report_path, .{
                    .status = .failed,
                    .recipe = recipe,
                    .artifact_root = recipe.artifacts.root,
                    .steps = step_manifests,
                    .metadata = try collectReportMetadata(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
                });
                return error.FinetuneStepFailed;
            },
        }
    }
    try finalizeSucceededRun(allocator, io, recipe, plan, step_manifests, manifest_path, training_config_path, training_report_path, static_metadata);
}

fn writeSucceededRunStatus(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    step_manifests: []const StepManifest,
    manifest_path: []const u8,
    training_report_path: []const u8,
    static_metadata: StaticMetadata,
) !void {
    try writeRunManifest(allocator, io, manifest_path, .{
        .status = .succeeded,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
    });
    try writeTrainingReport(allocator, io, training_report_path, .{
        .status = .succeeded,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
        .metadata = .{
            .dataset_fingerprints = static_metadata.dataset_fingerprints,
            .backend = static_metadata.backend,
            .optimizer = static_metadata.optimizer,
        },
    });
}

fn writeFailedRunStatus(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    plan: Plan,
    step_manifests: []const StepManifest,
    manifest_path: []const u8,
    training_config_path: []const u8,
    training_report_path: []const u8,
    static_metadata: StaticMetadata,
) !void {
    var report_arena = std.heap.ArenaAllocator.init(allocator);
    defer report_arena.deinit();
    try writeRunManifest(allocator, io, manifest_path, .{
        .status = .failed,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
    });
    try writeTrainingReport(allocator, io, training_report_path, .{
        .status = .failed,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
        .metadata = try collectReportMetadata(report_arena.allocator(), io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
    });
}

fn finalizeSucceededRun(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    plan: Plan,
    step_manifests: []const StepManifest,
    manifest_path: []const u8,
    training_config_path: []const u8,
    training_report_path: []const u8,
    static_metadata: StaticMetadata,
) !void {
    try writeSucceededRunStatus(allocator, io, recipe, step_manifests, manifest_path, training_report_path, static_metadata);
    var report_arena = std.heap.ArenaAllocator.init(allocator);
    defer report_arena.deinit();
    try writeTrainingReport(allocator, io, training_report_path, .{
        .status = .succeeded,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = step_manifests,
        .metadata = try collectReportMetadata(report_arena.allocator(), io, recipe, plan, manifest_path, training_config_path, training_report_path, static_metadata),
    });
}

fn runDirectCommandAdapter(allocator: std.mem.Allocator, io: std.Io, recipe: Recipe, step: Step) !bool {
    _ = recipe;
    const command = step.argv[0];
    if (!isDirectCommandAdapter(command)) return false;
    if (std.mem.eql(u8, command, "prepare-gemma4-lora-inputs")) {
        try runDirectPrepareGemma4LoraInputs(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "bootstrap-gemma4-lora")) {
        try runDirectBootstrapGemma4Lora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-gemma4-lora-bundle")) {
        try runDirectTrainEvalGemma4LoraBundle(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "bootstrap-gliner2-lora")) {
        try runDirectBootstrapGliner2Lora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "prepare-gliner2-top-layer-boundary-cache")) {
        try runDirectPrepareGliner2TopLayerBoundaryCache(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-gliner2-lora-bundle")) {
        try runDirectTrainEvalGliner2LoraBundle(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "materialize-gliner2-lora")) {
        try runDirectMaterializeGliner2Lora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "bootstrap-layoutlmv3-lora")) {
        try runDirectBootstrapLayoutlmv3Lora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-layoutlmv3-lora-sequence")) {
        try runDirectTrainEvalLayoutlmv3LoraSequence(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-layoutlmv3-lora-token")) {
        try runDirectTrainEvalLayoutlmv3LoraToken(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "materialize-layoutlmv3-checkpoint")) {
        try runDirectMaterializeLayoutlmv3Checkpoint(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "bootstrap-reranker-lora")) {
        try runDirectBootstrapRerankerLora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "prepare-reranker-top-layer-cache")) {
        try runDirectPrepareRerankerTopLayerCache(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-reranker-lora-top-layer-cached-surrogate")) {
        try runDirectTrainEvalRerankerLoraTopLayerCachedSurrogate(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "materialize-reranker-lora")) {
        try runDirectMaterializeRerankerLora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "prepare-colqwen2-inputs")) {
        try runDirectPrepareColqwen2Inputs(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "bootstrap-colqwen2-lora")) {
        try runDirectBootstrapColqwen2Lora(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-colqwen2-lora-bundle")) {
        try runDirectTrainEvalColqwen2LoraBundle(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "prepare-reranker-pooled-cache")) {
        try runDirectPrepareRerankerPooledCache(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "train-eval-reranker-head-cached")) {
        try runDirectTrainEvalRerankerHeadCached(allocator, io, step.argv);
        return true;
    }
    if (std.mem.eql(u8, command, "materialize-reranker-head")) {
        try runDirectMaterializeRerankerHead(allocator, io, step.argv);
        return true;
    }
    return false;
}

fn isDirectCommandAdapter(command: []const u8) bool {
    return command_registry.isDirectCommandAdapter(command);
}

fn printPlan(io: std.Io, recipe: Recipe, plan: Plan) !void {
    _ = io;
    print("recipe: {s}\n", .{recipe.recipe orelse recipe.kind orelse "unknown"});
    print("steps: {d}\n", .{plan.steps.len});
    for (plan.steps, 0..) |step, idx| {
        print("  {d}. {s}: ", .{ idx + 1, step.name });
        for (step.argv, 0..) |part, part_idx| {
            if (part_idx != 0) print(" ", .{});
            print("{s}", .{part});
        }
        print("\n", .{});
    }
}

fn parseKind(value: []const u8) !RecipeKind {
    if (eqlName(value, "sft")) return .sft;
    if (eqlName(value, "lora-sft") or eqlName(value, "lora_sft")) return .lora_sft;
    if (eqlName(value, "qlora-sft") or eqlName(value, "qlora_sft")) return .qlora_sft;
    if (eqlName(value, "dpo")) return .dpo;
    if (eqlName(value, "grpo")) return .grpo;
    if (eqlName(value, "reranker")) return .reranker;
    if (eqlName(value, "vlm-retrieval") or eqlName(value, "vlm_retrieval")) return .vlm_retrieval;
    return error.UnsupportedRecipeKind;
}

fn inferFamily(recipe: Recipe) ![]const u8 {
    if (recipe.model.path) |path| {
        if (inferFamilyFromModelPath(path)) |family| return family;
    }
    return error.MissingModelFamily;
}

fn inferFamilyFromModelPath(path: []const u8) ?[]const u8 {
    if (containsIgnoreCase(path, "gemma")) return "gemma4";
    if (containsQwen35Signal(path)) return "qwen3_5";
    if (containsIgnoreCase(path, "colqwen")) return "colqwen2";
    if (containsIgnoreCase(path, "qwen")) return "qwen2";
    if (containsIgnoreCase(path, "gliner")) return "gliner2";
    if (containsIgnoreCase(path, "layoutlmv3")) return "layoutlmv3";
    if (containsIgnoreCase(path, "reranker") or containsIgnoreCase(path, "deberta") or containsIgnoreCase(path, "modernbert")) return "reranker";
    return null;
}

fn requestsAdapterTraining(recipe: Recipe) bool {
    return recipe.artifacts.trained_adapter_dir != null or recipe.artifacts.adapter_dir != null or recipe.adapter != null;
}

fn rejectUnsupportedQwen35AdapterTraining(recipe: Recipe) !void {
    if (!requestsAdapterTraining(recipe)) return;
    const family = recipe.model.family orelse blk: {
        if (recipe.model.path) |path| break :blk inferFamilyFromModelPath(path) orelse return;
        return;
    };
    if (isQwen35Family(family)) return error.UnsupportedQwen35FinetuneGraph;
}

fn qwenLoraTargetModulesForFamily(family: []const u8) []const []const u8 {
    if (isQwen35Family(family)) return qwen2_real_autodiff.qwen35_lora_target_modules[0..];
    return qwen2_real_autodiff.default_lora_target_modules[0..];
}

fn defaultLoraRankForKind(kind: RecipeKind) usize {
    return switch (kind) {
        .grpo => default_policy_lora_rank,
        else => default_lora_rank,
    };
}

fn adapterRank(adapter: AdapterConfig, kind: RecipeKind) usize {
    return adapter.rank orelse defaultLoraRankForKind(kind);
}

fn adapterAlpha(adapter: AdapterConfig) f32 {
    return adapter.alpha orelse default_lora_alpha;
}

fn validateAdapterScaling(adapter: AdapterConfig) !void {
    const scaling = adapter.scaling orelse return;
    if (eqlName(scaling, "standard") or
        eqlName(scaling, "alpha/r") or
        eqlName(scaling, "alpha-over-r"))
    {
        return;
    }
    return error.UnsupportedLoRAScaling;
}

fn validateAdapterTargetSelection(adapter: AdapterConfig) !void {
    if (adapter.target_modules != null and adapter.target_preset != null) return error.ConflictingLoRATargetSelection;
}

fn validateGemmaAdapterOptions(adapter: AdapterConfig) !void {
    try validateAdapterScaling(adapter);
    try validateAdapterTargetSelection(adapter);
    if (adapter.target_modules == null) _ = try parseAdapterTargetPreset(adapter.target_preset orelse default_lora_target_preset);
}

fn validateNonGemmaAdapterOptions(adapter: AdapterConfig) !void {
    try validateAdapterScaling(adapter);
    try validateAdapterTargetSelection(adapter);
    if (adapter.use_dora orelse false) return error.UnsupportedLoRAOption;
    if (adapter.init_lora_weights != null) return error.UnsupportedLoRAOption;
}

fn validateGenericBootstrapAdapterOptions(adapter: AdapterConfig) !void {
    try validateNonGemmaAdapterOptions(adapter);
    if (adapter.target_preset != null) return error.UnsupportedLoRATargetPreset;
}

fn parseAdapterTargetPreset(name: []const u8) !peft.TargetPreset {
    return peft.parseTargetPreset(name) orelse error.UnsupportedLoRATargetPreset;
}

fn gemmaTargetPreset(adapter: AdapterConfig) !?peft.TargetPreset {
    if (adapter.target_modules != null) return null;
    return try parseAdapterTargetPreset(adapter.target_preset orelse default_lora_target_preset);
}

fn adapterTargetModulesForQwen(adapter: AdapterConfig, default_target_modules: []const []const u8) ![]const []const u8 {
    try validateAdapterTargetSelection(adapter);
    if (adapter.target_modules) |modules| return modules;
    const preset_name = adapter.target_preset orelse return default_target_modules;
    const preset = try parseAdapterTargetPreset(preset_name);
    return switch (preset) {
        .all_linear => default_target_modules,
        .attention_only => qwen_attention_lora_target_modules[0..],
        .mlp_only => qwen_mlp_lora_target_modules[0..],
        .moe_experts => error.UnsupportedLoRATargetPreset,
    };
}

fn appendTargetModulesCsv(allocator: std.mem.Allocator, list: *std.ArrayList([]const u8), modules: []const []const u8) !void {
    try appendMany(allocator, list, &.{ "--target-modules", try joinCsv(allocator, modules) });
}

fn appendGemmaBootstrapAdapterArgs(
    allocator: std.mem.Allocator,
    list: *std.ArrayList([]const u8),
    adapter: AdapterConfig,
    kind: RecipeKind,
) !void {
    try validateGemmaAdapterOptions(adapter);
    try appendMany(allocator, list, &.{
        try fmtInt(allocator, adapterRank(adapter, kind)),
        try fmtFloat(allocator, adapterAlpha(adapter)),
    });
    if (adapter.base_model_name_or_path) |base_name| try list.append(allocator, base_name);
    if (adapter.target_modules) |modules| {
        try appendTargetModulesCsv(allocator, list, modules);
    } else {
        _ = try parseAdapterTargetPreset(adapter.target_preset orelse default_lora_target_preset);
        try appendMany(allocator, list, &.{ "--target-preset", adapter.target_preset orelse default_lora_target_preset });
    }
    if (adapter.layer_name) |layer| try appendMany(allocator, list, &.{ "--layer-name", layer });
    if (adapter.use_dora orelse false) try list.append(allocator, "--use-dora");
    if (adapter.init_lora_weights) |init| try appendMany(allocator, list, &.{ "--init-lora-weights", init });
}

fn appendGenericBootstrapAdapterArgs(
    allocator: std.mem.Allocator,
    list: *std.ArrayList([]const u8),
    adapter: AdapterConfig,
    kind: RecipeKind,
) !void {
    try validateGenericBootstrapAdapterOptions(adapter);
    try appendMany(allocator, list, &.{
        try fmtInt(allocator, adapterRank(adapter, kind)),
        try fmtFloat(allocator, adapterAlpha(adapter)),
    });
    if (adapter.base_model_name_or_path) |base_name| try list.append(allocator, base_name);
    if (adapter.target_modules) |modules| try appendTargetModulesCsv(allocator, list, modules);
}

fn adapterBootstrapDir(recipe: Recipe) ?[]const u8 {
    if (recipe.adapter) |adapter| if (adapter.path) |path| return path;
    return recipe.artifacts.adapter_dir;
}

fn trainDatasetPath(recipe: Recipe) ?[]const u8 {
    return recipe.dataset.train_path orelse recipe.dataset.path;
}

fn evalDatasetPath(recipe: Recipe) ?[]const u8 {
    return recipe.dataset.eval_path;
}

fn trainCachePath(recipe: Recipe) ?[]const u8 {
    return recipe.dataset.train_cache_path orelse recipe.dataset.cache_path;
}

fn evalCachePath(recipe: Recipe) ?[]const u8 {
    return recipe.dataset.eval_cache_path;
}

fn evalMaxExamples(recipe: Recipe) ?usize {
    if (recipe.eval) |eval| if (eval.max_examples) |max| return max;
    return recipe.dataset.eval_max_examples;
}

fn manifestPath(allocator: std.mem.Allocator, recipe: Recipe) ![]const u8 {
    if (recipe.artifacts.manifest_path) |path| return allocator.dupe(u8, path);
    return defaultArtifactPath(allocator, recipe, "recipe_run_manifest.json");
}

fn defaultArtifactPath(allocator: std.mem.Allocator, recipe: Recipe, leaf: []const u8) ![]const u8 {
    const root = recipe.artifacts.root orelse "antfly-inference-finetune-out";
    return std.fs.path.join(allocator, &.{ root, leaf });
}

fn initStepManifests(allocator: std.mem.Allocator, plan: Plan) ![]StepManifest {
    const steps = try allocator.alloc(StepManifest, plan.steps.len);
    for (plan.steps, 0..) |step, idx| {
        steps[idx] = .{
            .index = idx,
            .name = step.name,
            .argv = step.argv,
        };
    }
    return steps;
}

fn writeRunManifest(allocator: std.mem.Allocator, io: std.Io, path: []const u8, manifest: RunManifest) !void {
    try writeJsonFile(allocator, io, path, manifest);
}

fn writeTrainingConfig(allocator: std.mem.Allocator, io: std.Io, path: []const u8, config: TrainingConfigFile) !void {
    try writeJsonFile(allocator, io, path, config);
}

fn writeTrainingReport(allocator: std.mem.Allocator, io: std.Io, path: []const u8, report: TrainingReportFile) !void {
    try writeJsonFile(allocator, io, path, report);
}

fn collectStaticMetadata(allocator: std.mem.Allocator, io: std.Io, recipe: Recipe) !StaticMetadata {
    return .{
        .dataset_fingerprints = try collectDatasetFingerprints(allocator, io, recipe),
        .backend = .{
            .requested = recipe.backend,
            .build = .{
                .inference_version = build_options.inference_version,
                .enable_native = build_options.enable_native,
                .enable_onnx = build_options.enable_onnx,
                .enable_mlx = build_options.enable_mlx,
                .enable_pjrt = build_options.enable_pjrt,
                .skip_openapi = build_options.skip_openapi,
            },
        },
        .optimizer = .{
            .learning_rate = recipe.optimizer.learning_rate,
            .epochs = recipe.optimizer.epochs,
            .micro_batch_size = recipe.optimizer.micro_batch_size,
            .gradient_accumulation_steps = recipe.optimizer.gradient_accumulation_steps,
            .max_grad_norm = recipe.optimizer.max_grad_norm,
            .schedule_free = recipe.optimizer.schedule_free,
            .llrd_decay = recipe.optimizer.llrd_decay,
        },
    };
}

fn collectReportMetadata(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    plan: Plan,
    manifest_path: []const u8,
    training_config_path: []const u8,
    training_report_path: []const u8,
    static_metadata: StaticMetadata,
) !ReportMetadata {
    return .{
        .dataset_fingerprints = static_metadata.dataset_fingerprints,
        .backend = static_metadata.backend,
        .optimizer = static_metadata.optimizer,
        .artifact_checksums = try collectArtifactChecksums(allocator, io, recipe, plan, manifest_path, training_config_path, training_report_path),
    };
}

fn collectDatasetFingerprints(allocator: std.mem.Allocator, io: std.Io, recipe: Recipe) ![]const PathFingerprint {
    var planned: std.ArrayListUnmanaged(PlannedPath) = .empty;
    errdefer planned.deinit(allocator);
    try appendUniquePlannedPath(allocator, &planned, "dataset", recipe.dataset.path);
    try appendUniquePlannedPath(allocator, &planned, "train_dataset", recipe.dataset.train_path);
    try appendUniquePlannedPath(allocator, &planned, "eval_dataset", recipe.dataset.eval_path);
    try appendUniquePlannedPath(allocator, &planned, "dataset_cache", recipe.dataset.cache_path);
    try appendUniquePlannedPath(allocator, &planned, "train_cache", recipe.dataset.train_cache_path);
    try appendUniquePlannedPath(allocator, &planned, "eval_cache", recipe.dataset.eval_cache_path);
    try appendUniquePlannedPath(allocator, &planned, "prepared_dataset", recipe.dataset.prepared_path);
    return try fingerprintPlannedPaths(allocator, io, planned.items);
}

fn collectArtifactChecksums(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    plan: Plan,
    manifest_path: []const u8,
    training_config_path: []const u8,
    training_report_path: []const u8,
) ![]const PathFingerprint {
    var planned: std.ArrayListUnmanaged(PlannedPath) = .empty;
    errdefer planned.deinit(allocator);
    try appendUniquePlannedPath(allocator, &planned, "manifest", manifest_path);
    try appendUniquePlannedPath(allocator, &planned, "training_config", training_config_path);
    try appendArtifactPathsFromPlan(allocator, &planned, recipe, plan);
    removePlannedPath(&planned, training_report_path);
    return try fingerprintPlannedPaths(allocator, io, planned.items);
}

fn appendArtifactPathsFromPlan(allocator: std.mem.Allocator, planned: *std.ArrayListUnmanaged(PlannedPath), recipe: Recipe, plan: Plan) !void {
    for (plan.steps) |step| {
        switch (step.kind) {
            .direct_sft => {
                const report_path = try sftReportPath(allocator, recipe);
                defer allocator.free(report_path);
                try appendUniquePlannedPathOwned(allocator, planned, "sft_report", report_path);
            },
            .direct_dpo => {
                const report_path = try dpoReportPath(allocator, recipe);
                defer allocator.free(report_path);
                try appendUniquePlannedPathOwned(allocator, planned, "dpo_report", report_path);
            },
            .direct_grpo => {
                const report_path = try grpoReportPath(allocator, recipe);
                defer allocator.free(report_path);
                try appendUniquePlannedPathOwned(allocator, planned, "grpo_report", report_path);
            },
            .command => {
                const command = step.argv[0];
                if (std.mem.eql(u8, command, "prepare-gemma4-lora-inputs")) {
                    try appendUniquePlannedPath(allocator, planned, "prepared_inputs", step.argv[4]);
                } else if (std.mem.eql(u8, command, "bootstrap-gemma4-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "adapter_bootstrap", step.argv[2]);
                } else if (std.mem.eql(u8, command, "train-eval-gemma4-lora-bundle")) {
                    try appendUniquePlannedPath(allocator, planned, "trained_adapter", step.argv[4]);
                } else if (std.mem.eql(u8, command, "prepare-gliner2-top-layer-boundary-cache")) {
                    const label = if (std.mem.eql(u8, step.name, "prepare-eval")) "eval_cache" else "train_cache";
                    try appendUniquePlannedPath(allocator, planned, label, step.argv[4]);
                } else if (std.mem.eql(u8, command, "bootstrap-gliner2-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "adapter_bootstrap", step.argv[2]);
                } else if (std.mem.eql(u8, command, "train-eval-gliner2-lora-bundle")) {
                    try appendUniquePlannedPath(allocator, planned, "trained_adapter", step.argv[5]);
                } else if (std.mem.eql(u8, command, "materialize-gliner2-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "materialized_model", step.argv[3]);
                } else if (std.mem.eql(u8, command, "bootstrap-layoutlmv3-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "adapter_bootstrap", step.argv[2]);
                } else if (std.mem.eql(u8, command, "train-eval-layoutlmv3-lora-sequence") or std.mem.eql(u8, command, "train-eval-layoutlmv3-lora-token")) {
                    try appendUniquePlannedPath(allocator, planned, "trained_adapter", step.argv[5]);
                } else if (std.mem.eql(u8, command, "materialize-layoutlmv3-checkpoint")) {
                    try appendUniquePlannedPath(allocator, planned, "materialized_model", step.argv[4]);
                } else if (std.mem.eql(u8, command, "prepare-reranker-pooled-cache")) {
                    const label = if (std.mem.eql(u8, step.name, "prepare-eval")) "eval_cache" else "train_cache";
                    try appendUniquePlannedPath(allocator, planned, label, step.argv[3]);
                } else if (std.mem.eql(u8, command, "train-eval-reranker-head-cached")) {
                    try appendUniquePlannedPath(allocator, planned, "trained_head", step.argv[4]);
                } else if (std.mem.eql(u8, command, "materialize-reranker-head")) {
                    try appendUniquePlannedPath(allocator, planned, "materialized_model", step.argv[3]);
                } else if (std.mem.eql(u8, command, "bootstrap-reranker-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "adapter_bootstrap", step.argv[2]);
                } else if (std.mem.eql(u8, command, "prepare-reranker-top-layer-cache")) {
                    const label = if (std.mem.eql(u8, step.name, "prepare-eval")) "eval_cache" else "train_cache";
                    try appendUniquePlannedPath(allocator, planned, label, step.argv[3]);
                } else if (std.mem.eql(u8, command, "train-eval-reranker-lora-top-layer-cached-surrogate")) {
                    try appendUniquePlannedPath(allocator, planned, "trained_adapter", step.argv[6]);
                } else if (std.mem.eql(u8, command, "materialize-reranker-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "materialized_model", step.argv[3]);
                } else if (std.mem.eql(u8, command, "prepare-colqwen2-inputs")) {
                    try appendUniquePlannedPath(allocator, planned, "prepared_inputs", step.argv[4]);
                } else if (std.mem.eql(u8, command, "bootstrap-colqwen2-lora")) {
                    try appendUniquePlannedPath(allocator, planned, "adapter_bootstrap", step.argv[2]);
                } else if (std.mem.eql(u8, command, "train-eval-colqwen2-lora-bundle")) {
                    try appendUniquePlannedPath(allocator, planned, "trained_adapter", step.argv[4]);
                }
            },
        }
    }
}

fn appendUniquePlannedPath(allocator: std.mem.Allocator, planned: *std.ArrayListUnmanaged(PlannedPath), label: []const u8, maybe_path: ?[]const u8) !void {
    const path = maybe_path orelse return;
    for (planned.items) |item| {
        if (std.mem.eql(u8, item.path, path)) return;
    }
    try planned.append(allocator, .{
        .label = label,
        .path = try allocator.dupe(u8, path),
    });
}

fn appendUniquePlannedPathOwned(allocator: std.mem.Allocator, planned: *std.ArrayListUnmanaged(PlannedPath), label: []const u8, path: []const u8) !void {
    for (planned.items) |item| {
        if (std.mem.eql(u8, item.path, path)) return;
    }
    try planned.append(allocator, .{
        .label = label,
        .path = try allocator.dupe(u8, path),
    });
}

fn removePlannedPath(planned: *std.ArrayListUnmanaged(PlannedPath), path: []const u8) void {
    var i: usize = 0;
    while (i < planned.items.len) {
        if (std.mem.eql(u8, planned.items[i].path, path)) {
            _ = planned.swapRemove(i);
            continue;
        }
        i += 1;
    }
}

fn fingerprintPlannedPaths(allocator: std.mem.Allocator, io: std.Io, planned: []const PlannedPath) ![]const PathFingerprint {
    const out = try allocator.alloc(PathFingerprint, planned.len);
    for (planned, 0..) |item, idx| {
        out[idx] = try fingerprintPath(allocator, io, item.label, item.path);
    }
    return out;
}

fn fingerprintPath(allocator: std.mem.Allocator, io: std.Io, label: []const u8, path: []const u8) !PathFingerprint {
    const stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return .{
            .label = label,
            .path = path,
            .exists = false,
        },
        else => return err,
    };
    switch (stat.kind) {
        .file => {
            const digest = try sha256FileAlloc(allocator, io, path);
            return .{
                .label = label,
                .path = path,
                .exists = true,
                .kind = "file",
                .size_bytes = stat.size,
                .digest = digest,
            };
        },
        .directory => {
            const summary = try digestDirectoryAlloc(allocator, io, path);
            return .{
                .label = label,
                .path = path,
                .exists = true,
                .kind = "directory",
                .size_bytes = summary.size_bytes,
                .entries = summary.entries,
                .digest = summary.digest,
            };
        },
        else => {
            return .{
                .label = label,
                .path = path,
                .exists = true,
                .kind = @tagName(stat.kind),
            };
        },
    }
}

fn sha256FileAlloc(allocator: std.mem.Allocator, io: std.Io, path: []const u8) ![]const u8 {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buf: [64 * 1024]u8 = undefined;
    while (true) {
        const n = file.readStreaming(io, &.{buf[0..]}) catch |err| switch (err) {
            error.EndOfStream => 0,
            else => return err,
        };
        if (n == 0) break;
        hasher.update(buf[0..n]);
    }

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.allocPrint(allocator, "sha256:{s}", .{std.fmt.bytesToHex(digest, .lower)});
}

fn digestDirectoryAlloc(allocator: std.mem.Allocator, io: std.Io, dir_path: []const u8) !DirectoryDigest {
    var entries: std.ArrayListUnmanaged(DirectoryDigestEntry) = .empty;
    errdefer entries.deinit(allocator);
    try appendDirectoryDigestEntries(allocator, io, dir_path, "", &entries);
    std.sort.heap(DirectoryDigestEntry, entries.items, {}, lessThanDirectoryDigestEntry);

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var total_size: u64 = 0;
    for (entries.items) |entry| {
        hasher.update(entry.relative_path);
        hasher.update(&.{0});
        hasher.update(entry.digest);
        hasher.update(&.{0});
        const size_text = try std.fmt.allocPrint(allocator, "{d}", .{entry.size_bytes});
        defer allocator.free(size_text);
        hasher.update(size_text);
        hasher.update(&.{'\n'});
        total_size += entry.size_bytes;
    }

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return .{
        .digest = try std.fmt.allocPrint(allocator, "sha256:{s}", .{std.fmt.bytesToHex(digest, .lower)}),
        .size_bytes = total_size,
        .entries = entries.items.len,
    };
}

fn appendDirectoryDigestEntries(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir_path: []const u8,
    rel_prefix: []const u8,
    entries: *std.ArrayListUnmanaged(DirectoryDigestEntry),
) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, dir_path, .{ .iterate = true });
    defer dir.close(io);

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file and entry.kind != .directory) continue;
        const child_path = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
        defer allocator.free(child_path);
        const rel_path = if (rel_prefix.len == 0)
            try allocator.dupe(u8, entry.name)
        else
            try std.fmt.allocPrint(allocator, "{s}/{s}", .{ rel_prefix, entry.name });
        errdefer allocator.free(rel_path);

        if (entry.kind == .directory) {
            try appendDirectoryDigestEntries(allocator, io, child_path, rel_path, entries);
            continue;
        }

        const stat = try std.Io.Dir.cwd().statFile(io, child_path, .{});
        const digest = try sha256FileAlloc(allocator, io, child_path);
        try entries.append(allocator, .{
            .relative_path = rel_path,
            .size_bytes = stat.size,
            .digest = digest,
        });
    }
}

fn lessThanDirectoryDigestEntry(_: void, lhs: DirectoryDigestEntry, rhs: DirectoryDigestEntry) bool {
    return std.mem.order(u8, lhs.relative_path, rhs.relative_path) == .lt;
}

fn writeJsonFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8, value: anytype) !void {
    if (std.fs.path.dirname(path)) |parent| {
        if (parent.len > 0) try std.Io.Dir.cwd().createDirPath(io, parent);
    }
    const rendered = try std.json.Stringify.valueAlloc(allocator, value, .{ .whitespace = .indent_2 });
    defer allocator.free(rendered);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = rendered });
}

fn sftReportPath(allocator: std.mem.Allocator, recipe: Recipe) ![]const u8 {
    if (recipe.artifacts.report_path) |path| return allocator.dupe(u8, path);
    return defaultArtifactPath(allocator, recipe, "sft_report.json");
}

fn dpoReportPath(allocator: std.mem.Allocator, recipe: Recipe) ![]const u8 {
    if (recipe.artifacts.report_path) |path| return allocator.dupe(u8, path);
    return defaultArtifactPath(allocator, recipe, "dpo_report.json");
}

fn grpoReportPath(allocator: std.mem.Allocator, recipe: Recipe) ![]const u8 {
    if (recipe.artifacts.report_path) |path| return allocator.dupe(u8, path);
    return defaultArtifactPath(allocator, recipe, "grpo_report.json");
}

fn expectRunStatusFile(allocator: std.mem.Allocator, io: std.Io, path: []const u8, expected_status: []const u8) !void {
    const raw = try readFileMax(allocator, io, path, 16 * 1024 * 1024);
    defer allocator.free(raw);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSmokeArtifact;
    const status_val = parsed.value.object.get("status") orelse return error.InvalidSmokeArtifact;
    if (status_val != .string) return error.InvalidSmokeArtifact;
    if (!std.mem.eql(u8, status_val.string, expected_status)) return error.InvalidSmokeArtifact;
}

fn expectPathExists(io: std.Io, path: []const u8) !void {
    _ = try std.Io.Dir.cwd().statFile(io, path, .{});
}

fn runDirectPrepareGemma4LoraInputs(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 5) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const dataset_path = argv_in[2];
    const split_arg = argv_in[3];
    const out_path = argv_in[4];
    const split = if (std.mem.eql(u8, split_arg, "-")) null else split_arg;

    var max_examples: usize = 0;
    var max_seq_len: usize = 512;
    var gguf_projector_path: ?[]const u8 = null;
    var i: usize = 5;
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--max-examples")) {
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            max_examples = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else if (std.mem.eql(u8, arg, "--max-seq-len")) {
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            max_seq_len = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else if (std.mem.eql(u8, arg, "--gguf-projector")) {
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            gguf_projector_path = argv_in[i];
        } else {
            return error.InvalidArguments;
        }
    }

    var loaded = try gemma_chat_data.loadExamples(allocator, dataset_path, split);
    defer loaded.deinit();
    const has_multimodal = gemmaMessagesHaveMedia(loaded.examples);
    if (has_multimodal and gguf_projector_path == null) return error.MissingGgufProjector;
    var summary = if (has_multimodal)
        try gemma4.prepareMultimodalInputsFromChatData(
            allocator,
            model_dir,
            gguf_projector_path.?,
            loaded.examples,
            max_examples,
            max_seq_len,
        )
    else
        try gemma4.prepareInputsFromChatData(
            allocator,
            model_dir,
            loaded.examples,
            max_examples,
            max_seq_len,
        );
    defer gemma4.freePreparedInputsSummary(allocator, &summary);
    try gemma4.savePreparedInputsSummary(allocator, out_path, summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectBootstrapGemma4Lora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 3) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const out_dir = argv_in[2];
    var rank = default_lora_rank;
    var alpha = default_lora_alpha;
    var rank_set = false;
    var alpha_set = false;
    var rank_alpha_flag_seen = false;

    var base_model_name_or_path: ?[]const u8 = null;
    var layer_name: ?[]const u8 = null;
    var target_preset: ?peft.TargetPreset = null;
    var target_modules: ?[]const []const u8 = null;
    defer if (target_modules) |modules| allocator.free(modules);
    var use_dora = false;
    var init_lora_weights: ?[]const u8 = null;
    var i: usize = 3;
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--rank")) {
            if (rank_set) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            rank = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
            rank_set = true;
            rank_alpha_flag_seen = true;
        } else if (std.mem.eql(u8, arg, "--alpha")) {
            if (alpha_set) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            alpha = try std.fmt.parseFloat(f32, argv_in[i]);
            alpha_set = true;
            rank_alpha_flag_seen = true;
        } else if (std.mem.eql(u8, arg, "--layer-name") or std.mem.eql(u8, arg, "--layer")) {
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            layer_name = argv_in[i];
        } else if (std.mem.eql(u8, arg, "--target-preset")) {
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            target_preset = peft.parseTargetPreset(argv_in[i]) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--target-modules")) {
            if (target_modules != null) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            target_modules = try parseCsvBorrowed(allocator, argv_in[i]);
        } else if (std.mem.eql(u8, arg, "--use-dora")) {
            use_dora = true;
        } else if (std.mem.eql(u8, arg, "--init-lora-weights")) {
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            init_lora_weights = argv_in[i];
        } else if (!rank_alpha_flag_seen and !rank_set) {
            rank = try std.fmt.parseUnsigned(usize, arg, 10);
            rank_set = true;
        } else if (!rank_alpha_flag_seen and !alpha_set) {
            alpha = try std.fmt.parseFloat(f32, arg);
            alpha_set = true;
        } else if (base_model_name_or_path == null) {
            base_model_name_or_path = arg;
        } else {
            return error.InvalidArguments;
        }
    }
    if (target_modules != null and target_preset != null) return error.InvalidArguments;
    const effective_target_preset = if (target_modules == null) target_preset orelse .all_linear else null;

    var summary = try gemma4.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_name_or_path,
        .layer_name = layer_name,
        .target_modules = target_modules,
        .target_preset = effective_target_preset,
        .use_dora = use_dora,
        .init_lora_weights = init_lora_weights,
    });
    defer gemma4.freeBootstrapSummary(allocator, &summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalGemma4LoraBundle(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 5) return error.InvalidArguments;
    try train_eval_gemma4_lora_bundle.runFromArgs(allocator, io, argv_in[1..]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn gemmaMessagesHaveMedia(examples: []const gemma_chat_data.Example) bool {
    for (examples) |example| {
        if (example.image_paths.len > 0 or example.audio_paths.len > 0) return true;
    }
    return false;
}

fn runDirectBootstrapGliner2Lora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 3) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const out_dir = argv_in[2];
    var rank = default_lora_rank;
    var alpha = default_lora_alpha;
    var base_model_name_or_path: ?[]const u8 = null;
    var target_modules: ?[]const []const u8 = null;
    defer if (target_modules) |modules| allocator.free(modules);
    var i: usize = 3;
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        rank = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        i += 1;
    }
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        alpha = try std.fmt.parseFloat(f32, argv_in[i]);
        i += 1;
    }
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--target-modules")) {
            if (target_modules != null) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            target_modules = try parseCsvBorrowed(allocator, argv_in[i]);
        } else if (base_model_name_or_path == null) {
            base_model_name_or_path = arg;
        } else {
            return error.InvalidArguments;
        }
    }

    var summary = try gliner2.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_name_or_path,
        .target_modules = target_modules,
    });
    defer gliner2.freeBootstrapSummary(allocator, &summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectPrepareGliner2TopLayerBoundaryCache(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 5) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const input_path = argv_in[2];
    const entity_types_csv = argv_in[3];
    const out_path = argv_in[4];
    const split = if (argv_in.len >= 6) argv_in[5] else null;
    const backend_arg = if (argv_in.len >= 7) argv_in[6] else "native";
    const max_examples = if (argv_in.len >= 8) try std.fmt.parseUnsigned(usize, argv_in[7], 10) else 128;
    const max_length = if (argv_in.len >= 9) try std.fmt.parseUnsigned(usize, argv_in[8], 10) else 256;
    const max_span_width = if (argv_in.len >= 10) try std.fmt.parseUnsigned(usize, argv_in[9], 10) else 8;
    const top_layer_count = if (argv_in.len >= 11) try std.fmt.parseUnsigned(usize, argv_in[10], 10) else 1;

    const backend = try parseGlinerBackend(backend_arg);
    const entity_types = try parseCsvOwned(allocator, entity_types_csv);
    defer {
        for (entity_types) |item| allocator.free(item);
        allocator.free(entity_types);
    }

    var loaded = try gliner2_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();
    const summary = try gliner2_boundary.prepareCachedBoundarySummary(
        allocator,
        model_dir,
        input_path,
        split,
        loaded.examples,
        entity_types,
        backend,
        max_examples,
        max_length,
        max_span_width,
        top_layer_count,
    );
    defer {
        var owned = summary;
        gliner2_boundary.freeCachedBoundarySummary(allocator, &owned);
    }
    try gliner2_boundary.saveCachedBoundarySummary(allocator, out_path, summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalGliner2LoraBundle(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 6) return error.InvalidArguments;
    try train_eval_gliner2_lora_bundle.runFromArgs(allocator, io, argv_in[1..]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectMaterializeGliner2Lora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len != 4) return error.InvalidArguments;
    const summary = try gliner2.materializeMergedModel(allocator, argv_in[1], argv_in[2], argv_in[3]);
    defer {
        allocator.free(summary.artifact_family_version);
        allocator.free(summary.base_model_dir);
        allocator.free(summary.adapter_model_dir);
        allocator.free(summary.output_dir);
        allocator.free(summary.output_checkpoint_path);
    }
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn parseGlinerBackend(value: []const u8) !reranker.BackendChoice {
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return error.InvalidBackend;
}

fn parseCsvOwned(allocator: std.mem.Allocator, value: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (out.items) |item| allocator.free(item);
        out.deinit(allocator);
    }
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try out.append(allocator, try allocator.dupe(u8, item));
    }
    if (out.items.len == 0) return error.EmptyEntityTypes;
    return try out.toOwnedSlice(allocator);
}

fn parseCsvBorrowed(allocator: std.mem.Allocator, value: []const u8) ![]const []const u8 {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    errdefer out.deinit(allocator);
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try out.append(allocator, item);
    }
    if (out.items.len == 0) return error.InvalidArguments;
    return try out.toOwnedSlice(allocator);
}

fn runDirectBootstrapLayoutlmv3Lora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 3) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const out_dir = argv_in[2];
    var rank = default_lora_rank;
    var alpha = default_lora_alpha;
    var base_model_name_or_path: ?[]const u8 = null;
    var target_modules: ?[]const []const u8 = null;
    defer if (target_modules) |modules| allocator.free(modules);
    var i: usize = 3;
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        rank = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        i += 1;
    }
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        alpha = try std.fmt.parseFloat(f32, argv_in[i]);
        i += 1;
    }
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--target-modules")) {
            if (target_modules != null) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            target_modules = try parseCsvBorrowed(allocator, argv_in[i]);
        } else if (base_model_name_or_path == null) {
            base_model_name_or_path = arg;
        } else {
            return error.InvalidArguments;
        }
    }

    var summary = try layoutlmv3.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_name_or_path,
        .target_modules = target_modules,
    });
    defer layoutlmv3.freeBootstrapSummary(allocator, &summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalLayoutlmv3LoraSequence(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 6) return error.InvalidArguments;
    try train_eval_layoutlmv3_lora_sequence.runFromArgs(allocator, io, argv_in[1..]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalLayoutlmv3LoraToken(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 6) return error.InvalidArguments;
    try train_eval_layoutlmv3_lora_token.runFromArgs(allocator, io, argv_in[1..]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectMaterializeLayoutlmv3Checkpoint(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 5) return error.InvalidArguments;
    var summary = try layoutlmv3.materializeMergedModel(allocator, argv_in[1], argv_in[2], argv_in[3], argv_in[4]);
    defer layoutlmv3.freeMaterializeSummary(allocator, &summary);
    if (argv_in.len >= 6) {
        try writeJsonFile(allocator, io, argv_in[5], summary);
    }
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectBootstrapRerankerLora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 3) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const out_dir = argv_in[2];
    var rank = default_lora_rank;
    var alpha = default_lora_alpha;
    var top_layer_count: usize = 1;
    var base_model_name_or_path: ?[]const u8 = null;
    var target_modules: ?[]const []const u8 = null;
    defer if (target_modules) |modules| allocator.free(modules);
    var i: usize = 3;
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        rank = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        i += 1;
    }
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        alpha = try std.fmt.parseFloat(f32, argv_in[i]);
        i += 1;
    }
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        top_layer_count = std.fmt.parseUnsigned(usize, argv_in[i], 10) catch |err| switch (err) {
            error.InvalidCharacter => blk: {
                base_model_name_or_path = argv_in[i];
                break :blk top_layer_count;
            },
            else => return err,
        };
        i += 1;
    }
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--target-modules")) {
            if (target_modules != null) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            target_modules = try parseCsvBorrowed(allocator, argv_in[i]);
        } else if (std.mem.eql(u8, arg, "--base-model-name-or-path") or std.mem.eql(u8, arg, "--base-model")) {
            if (base_model_name_or_path != null) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            base_model_name_or_path = argv_in[i];
        } else {
            return error.InvalidArguments;
        }
    }

    var summary = try reranker_lora.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .top_layer_count = top_layer_count,
        .base_model_name_or_path = base_model_name_or_path,
        .target_modules = target_modules,
    });
    defer reranker_lora.freeBootstrapSummary(allocator, &summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectPrepareRerankerTopLayerCache(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 4) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const input_path = argv_in[2];
    const out_path = argv_in[3];
    const split = if (argv_in.len >= 5 and !std.mem.startsWith(u8, argv_in[4], "--")) argv_in[4] else null;

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 128;
    var top_layer_count: usize = 1;
    var i: usize = if (split == null) 4 else 5;
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingBackendValue;
            backend = parseRerankerBackendChoice(argv_in[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else if (std.mem.eql(u8, arg, "--top-layer-count")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingTopLayerCount;
            top_layer_count = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else {
            return error.InvalidArguments;
        }
    }

    var loaded = try reranker_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();
    var summary = try reranker_head.prepareCachedTopLayerSummary(
        allocator,
        model_dir,
        input_path,
        split,
        loaded.examples,
        backend,
        max_examples,
        top_layer_count,
    );
    defer reranker_head.freeCachedTopLayerSummary(allocator, &summary);
    try reranker_head.saveCachedTopLayerSummary(allocator, out_path, summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalRerankerLoraTopLayerCachedSurrogate(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 7) return error.InvalidArguments;
    try train_eval_reranker_lora_top_layer_cached_surrogate.runFromArgs(allocator, io, argv_in[1..]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectMaterializeRerankerLora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len != 4) return error.InvalidArguments;
    var summary = try reranker_lora.materializeMergedModel(allocator, argv_in[1], argv_in[2], argv_in[3]);
    defer reranker_lora.freeMaterializeSummary(allocator, &summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectPrepareColqwen2Inputs(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 5) return error.InvalidArguments;
    const model_input = argv_in[1];
    const dataset_root = argv_in[2];
    const examples_jsonl = argv_in[3];
    const out_path = argv_in[4];
    const max_examples = if (argv_in.len >= 6) try std.fmt.parseUnsigned(usize, argv_in[5], 10) else 32;

    const examples = try colqwen2.loadExamples(allocator, examples_jsonl);
    defer colqwen2.freeExamples(allocator, examples);
    var summary = try colqwen2.prepareInputsAgainstExamples(allocator, model_input, dataset_root, examples, max_examples);
    defer colqwen2.freePreparedInputsSummary(allocator, &summary);
    try colqwen2.savePreparedInputsSummary(allocator, out_path, summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectBootstrapColqwen2Lora(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 3) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const out_dir = argv_in[2];
    var rank = default_lora_rank;
    var alpha = default_lora_alpha;
    var base_model_name_or_path: ?[]const u8 = null;
    var target_modules: ?[]const []const u8 = null;
    defer if (target_modules) |modules| allocator.free(modules);
    var i: usize = 3;
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        rank = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        i += 1;
    }
    if (i < argv_in.len and !std.mem.startsWith(u8, argv_in[i], "--")) {
        alpha = try std.fmt.parseFloat(f32, argv_in[i]);
        i += 1;
    }
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--target-modules")) {
            if (target_modules != null) return error.InvalidArguments;
            i += 1;
            if (i >= argv_in.len) return error.InvalidArguments;
            target_modules = try parseCsvBorrowed(allocator, argv_in[i]);
        } else if (base_model_name_or_path == null) {
            base_model_name_or_path = arg;
        } else {
            return error.InvalidArguments;
        }
    }

    var summary = try colqwen2.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_name_or_path,
        .target_modules = target_modules,
    });
    defer colqwen2.freeBootstrapSummary(allocator, &summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalColqwen2LoraBundle(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    if (argv_in.len < 5) return error.InvalidArguments;
    try train_eval_colqwen2_lora_bundle.runFromArgs(allocator, io, argv_in[1..]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectPrepareRerankerPooledCache(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 4) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const input_path = argv_in[2];
    const out_path = argv_in[3];
    const split = if (argv_in.len >= 5 and !std.mem.startsWith(u8, argv_in[4], "--")) argv_in[4] else null;

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 256;
    var i: usize = if (split == null) 4 else 5;
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingBackendValue;
            backend = parseRerankerBackendChoice(argv_in[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else {
            return error.InvalidArguments;
        }
    }

    var loaded = try reranker_data.loadExamples(allocator, input_path, split);
    defer loaded.deinit();
    var summary = try reranker_head.prepareCachedPooledSummary(
        allocator,
        model_dir,
        input_path,
        split,
        loaded.examples,
        backend,
        max_examples,
    );
    defer reranker_head.freeCachedPooledSummary(allocator, &summary);
    try reranker_head.saveCachedPooledSummary(allocator, out_path, summary);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectTrainEvalRerankerHeadCached(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len < 5) return error.InvalidArguments;
    const model_dir = argv_in[1];
    const train_cache_path = argv_in[2];
    const eval_cache_path = argv_in[3];
    const out_dir = argv_in[4];

    var backend: reranker_head.BackendChoice = .auto;
    var max_examples: usize = 256;
    var epochs: usize = 1;
    var learning_rate: f32 = 0.001;
    var i: usize = 5;
    while (i < argv_in.len) : (i += 1) {
        const arg = argv_in[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingBackendValue;
            backend = parseRerankerBackendChoice(argv_in[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingMaxExamples;
            max_examples = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingEpochs;
            epochs = try std.fmt.parseUnsigned(usize, argv_in[i], 10);
        } else if (std.mem.eql(u8, arg, "--learning-rate")) {
            i += 1;
            if (i >= argv_in.len) return error.MissingLearningRate;
            learning_rate = try std.fmt.parseFloat(f32, argv_in[i]);
        } else {
            return error.InvalidArguments;
        }
    }

    var train_summary = try reranker_head.loadCachedPooledSummary(allocator, train_cache_path);
    defer reranker_head.freeCachedPooledSummary(allocator, &train_summary);
    var eval_summary = try reranker_head.loadCachedPooledSummary(allocator, eval_cache_path);
    defer reranker_head.freeCachedPooledSummary(allocator, &eval_summary);
    const summary = try reranker_head.trainEvalHeadCachedSummary(
        allocator,
        model_dir,
        &train_summary,
        &eval_summary,
        out_dir,
        backend,
        epochs,
        .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
        },
    );
    defer allocator.free(summary.output_dir);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn runDirectMaterializeRerankerHead(allocator: std.mem.Allocator, io: std.Io, argv_in: []const []const u8) !void {
    _ = io;
    if (argv_in.len != 4) return error.InvalidArguments;
    try reranker_head.materializeHeadFromDir(allocator, argv_in[1], argv_in[2], argv_in[3]);
    print("direct adapter: {s}\n", .{argv_in[0]});
}

fn parseRerankerBackendChoice(value: []const u8) ?reranker_head.BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    return null;
}

const DpoScalarRow = struct {
    policy_chosen_logp: f32,
    policy_rejected_logp: f32,
    ref_chosen_logp: f32,
    ref_rejected_logp: f32,
    chosen_length: ?u32 = null,
    rejected_length: ?u32 = null,
    sft_chosen_loss: ?f32 = null,
};

const DpoTextRow = struct {
    prompt: []const u8,
    chosen: []const u8,
    rejected: []const u8,
    sft_chosen_loss: ?f32 = null,
};

const SftTextRow = struct {
    prompt: []const u8,
    response: ?[]const u8 = null,
    completion: ?[]const u8 = null,
    chosen: ?[]const u8 = null,
};

const DpoBatchOwned = struct {
    policy_chosen_logps: []f32,
    policy_rejected_logps: []f32,
    ref_chosen_logps: []f32,
    ref_rejected_logps: []f32,
    chosen_lengths: []u32,
    rejected_lengths: []u32,
    sft_chosen_loss: []f32,

    fn deinit(self: DpoBatchOwned, allocator: std.mem.Allocator) void {
        allocator.free(self.policy_chosen_logps);
        allocator.free(self.policy_rejected_logps);
        allocator.free(self.ref_chosen_logps);
        allocator.free(self.ref_rejected_logps);
        allocator.free(self.chosen_lengths);
        allocator.free(self.rejected_lengths);
        allocator.free(self.sft_chosen_loss);
    }

    fn batch(self: DpoBatchOwned) preference_loss.PairedBatch {
        return .{
            .policy_chosen_logps = self.policy_chosen_logps,
            .policy_rejected_logps = self.policy_rejected_logps,
            .ref_chosen_logps = self.ref_chosen_logps,
            .ref_rejected_logps = self.ref_rejected_logps,
            .chosen_lengths = self.chosen_lengths,
            .rejected_lengths = self.rejected_lengths,
            .sft_chosen_loss = self.sft_chosen_loss,
        };
    }
};

const DpoPreferenceSamplesOwned = struct {
    arena: std.heap.ArenaAllocator,
    samples: []const preference_harness.PreferenceSample,

    fn deinit(self: *DpoPreferenceSamplesOwned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const SftPreparedExamplesOwned = struct {
    examples: []gemma4.PreparedExampleInput,

    fn deinit(self: *SftPreparedExamplesOwned, allocator: std.mem.Allocator) void {
        for (self.examples) |*example| freeGemmaPreparedExample(allocator, example);
        allocator.free(self.examples);
        self.* = undefined;
    }
};

const GrpoTextRow = struct {
    prompt: []const u8,
    target: []const u8,
    image_paths: ?[]const []const u8 = null,
    audio_paths: ?[]const []const u8 = null,
};

const GrpoPromptBatchOwned = struct {
    arena: std.heap.ArenaAllocator,
    prompts: []const []const i32,
    targets: []const []const u8,

    fn deinit(self: *GrpoPromptBatchOwned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

const GemmaPreparedPromptBatchOwned = struct {
    allocator: std.mem.Allocator,
    prompts: []const *const gemma4.PreparedExampleInput,
    summaries: []gemma4.PreparedInputsSummary,
    targets: []const []const u8,

    fn deinit(self: *GemmaPreparedPromptBatchOwned) void {
        for (self.summaries) |*summary| gemma4.freePreparedInputsSummary(self.allocator, summary);
        self.allocator.free(self.prompts);
        self.allocator.free(self.summaries);
        for (self.targets) |target| self.allocator.free(target);
        self.allocator.free(self.targets);
        self.* = undefined;
    }
};

const DecoderLogprobScorer = struct {
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    max_seq_len: usize,

    fn modelForward(
        ctx: *anyopaque,
        prompts: []const []const i32,
        completion_tokens: []const []const i32,
        out_logps: []f32,
    ) !void {
        const self: *DecoderLogprobScorer = @ptrCast(@alignCast(ctx));
        if (prompts.len != completion_tokens.len or prompts.len != out_logps.len) return error.BatchSizeMismatch;

        const gpt_config = session_factory.getGptConfig(self.model.session) orelse return error.InvalidModelForGeneration;
        var cb = try session_factory.getComputeBackend(self.model.session, self.allocator);
        defer cb.deinit();

        for (prompts, completion_tokens, out_logps) |prompt_ids, completion_ids, *out_logp| {
            if (prompt_ids.len == 0) return error.EmptyPrompt;
            if (completion_ids.len == 0) return error.EmptyCompletion;
            const total_len = prompt_ids.len + completion_ids.len;
            if (total_len > self.max_seq_len) return error.SequenceTooLong;

            const input_ids = try self.allocator.alloc(i64, total_len);
            defer self.allocator.free(input_ids);
            for (prompt_ids, 0..) |token_id, idx| input_ids[idx] = token_id;
            for (completion_ids, 0..) |token_id, idx| input_ids[prompt_ids.len + idx] = token_id;

            const logits = try gpt_arch.forward(&cb, self.allocator, gpt_config, input_ids, 1, total_len, null);
            defer self.allocator.free(logits);

            const vocab_size: usize = @intCast(gpt_config.vocab_size);
            var sum_logp: f32 = 0.0;
            for (completion_ids, 0..) |token_id, comp_idx| {
                const row_idx = prompt_ids.len + comp_idx - 1;
                const row = logits[row_idx * vocab_size ..][0..vocab_size];
                sum_logp += logProbAtToken(row, token_id);
            }
            out_logp.* = sum_logp;
        }
    }

    fn tokenLogprobs(
        ctx: *anyopaque,
        prompt: []const i32,
        completion: []const i32,
        out_per_token_logp: []f32,
    ) !void {
        const self: *DecoderLogprobScorer = @ptrCast(@alignCast(ctx));
        if (completion.len != out_per_token_logp.len) return error.LogpLenMismatch;
        if (prompt.len == 0) return error.EmptyPrompt;
        if (completion.len == 0) return error.EmptyCompletion;

        const gpt_config = session_factory.getGptConfig(self.model.session) orelse return error.InvalidModelForGeneration;
        const total_len = prompt.len + completion.len;
        if (total_len > self.max_seq_len) return error.SequenceTooLong;

        var cb = try session_factory.getComputeBackend(self.model.session, self.allocator);
        defer cb.deinit();

        const input_ids = try self.allocator.alloc(i64, total_len);
        defer self.allocator.free(input_ids);
        for (prompt, 0..) |token_id, idx| input_ids[idx] = token_id;
        for (completion, 0..) |token_id, idx| input_ids[prompt.len + idx] = token_id;

        const logits = try gpt_arch.forward(&cb, self.allocator, gpt_config, input_ids, 1, total_len, null);
        defer self.allocator.free(logits);

        const vocab_size: usize = @intCast(gpt_config.vocab_size);
        for (completion, 0..) |token_id, comp_idx| {
            const row_idx = prompt.len + comp_idx - 1;
            const row = logits[row_idx * vocab_size ..][0..vocab_size];
            out_per_token_logp[comp_idx] = logProbAtToken(row, token_id);
        }
    }
};

const DecoderGrpoSampler = struct {
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    max_seq_len: usize,
    max_completion_tokens: usize,

    fn sample(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        prompt: []const i32,
        num_samples: usize,
        out_tokens: *std.ArrayList([]i32),
        out_old_logps: *std.ArrayList([]f32),
    ) !void {
        const self: *DecoderGrpoSampler = @ptrCast(@alignCast(ctx));
        if (prompt.len == 0) return error.EmptyPrompt;
        if (num_samples == 0) return;

        const gpt_config = session_factory.getGptConfig(self.model.session) orelse return error.InvalidModelForGeneration;
        var cb = try session_factory.getComputeBackend(self.model.session, self.allocator);
        defer cb.deinit();

        const top_rank_cap: usize = @min(num_samples, 8);
        const eos_id = self.model.getTokenizer().specialTokens().sep_id;

        for (0..num_samples) |sample_idx| {
            var seq = std.ArrayListUnmanaged(i64).empty;
            defer seq.deinit(allocator);
            try seq.ensureTotalCapacity(allocator, prompt.len + self.max_completion_tokens);
            for (prompt) |token_id| try seq.append(allocator, token_id);

            var completion = std.ArrayListUnmanaged(i32).empty;
            defer completion.deinit(allocator);
            var old_logps = std.ArrayListUnmanaged(f32).empty;
            defer old_logps.deinit(allocator);

            var step: usize = 0;
            while (step < self.max_completion_tokens and seq.items.len < self.max_seq_len) : (step += 1) {
                const logits = try gpt_arch.forward(&cb, self.allocator, gpt_config, seq.items, 1, seq.items.len, null);
                defer self.allocator.free(logits);
                const vocab_size: usize = @intCast(gpt_config.vocab_size);
                const row = logits[(seq.items.len - 1) * vocab_size ..][0..vocab_size];
                const token_id = try selectRankedTokenFromLogits(allocator, row, sample_idx % top_rank_cap);
                const token_logp = logProbAtToken(row, token_id);
                try completion.append(allocator, token_id);
                try old_logps.append(allocator, token_logp);
                try seq.append(allocator, token_id);
                if (eos_id >= 0 and token_id == eos_id) break;
            }

            if (completion.items.len == 0) return error.EmptyCompletion;
            try out_tokens.append(allocator, try completion.toOwnedSlice(allocator));
            try out_old_logps.append(allocator, try old_logps.toOwnedSlice(allocator));
        }
    }
};

const TextRewardMode = enum {
    exact_match,
    exact_match_ci,
    prefix_match,
};

const TextRewardCtx = struct {
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    targets: []const []const u8,
    mode: TextRewardMode,

    fn score(
        ctx: *anyopaque,
        prompt_idx: usize,
        completion_tokens: []const i32,
    ) !f32 {
        const self: *TextRewardCtx = @ptrCast(@alignCast(ctx));
        if (prompt_idx >= self.targets.len) return error.InvalidPromptIndex;
        const decoded = try self.tokenizer.decode(self.allocator, completion_tokens);
        defer self.allocator.free(decoded);
        const completion_trimmed = std.mem.trim(u8, decoded, " \t\r\n");
        const target_trimmed = std.mem.trim(u8, self.targets[prompt_idx], " \t\r\n");
        return scoreTextReward(self.mode, completion_trimmed, target_trimmed);
    }
};

fn scoreTextReward(mode: TextRewardMode, completion_trimmed: []const u8, target_trimmed: []const u8) f32 {
    return switch (mode) {
        .exact_match => blk: {
            if (std.mem.eql(u8, completion_trimmed, target_trimmed)) break :blk 1.0;
            if (std.mem.indexOf(u8, completion_trimmed, target_trimmed) != null) break :blk 0.5;
            break :blk 0.0;
        },
        .exact_match_ci => if (std.ascii.eqlIgnoreCase(completion_trimmed, target_trimmed)) 1.0 else 0.0,
        .prefix_match => if (std.mem.startsWith(u8, completion_trimmed, target_trimmed)) 1.0 else 0.0,
    };
}

fn runDirectSft(allocator: std.mem.Allocator, io: std.Io, recipe: Recipe, report_path: []const u8) !void {
    const path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const format = recipe.dataset.format orelse "text-sft";
    if (!std.mem.eql(u8, format, "text-sft") and !std.mem.eql(u8, format, "rendered-text-sft")) {
        return error.UnsupportedSftFormat;
    }
    const family = recipe.model.family orelse try inferFamily(recipe);
    if (!isQwen35Family(family)) return error.UnsupportedRecipeFamily;
    try runOptimizerBackedQwen2Sft(allocator, io, recipe, path, report_path);
}

fn shouldRunOptimizerBackedQwen35Sft(recipe: Recipe, format: []const u8) !bool {
    if (!std.mem.eql(u8, format, "text-sft") and !std.mem.eql(u8, format, "rendered-text-sft")) return false;
    const family = recipe.model.family orelse try inferFamily(recipe);
    return isQwen35Family(family) and requestsAdapterTraining(recipe);
}

fn runDirectDpo(allocator: std.mem.Allocator, io: std.Io, recipe: Recipe, report_path: []const u8) !void {
    const path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const format = recipe.dataset.format orelse "scalar-logprobs";
    if (std.mem.eql(u8, format, "scalar-logprobs")) {
        const batch = try loadDpoScalarJsonl(allocator, io, path);
        defer batch.deinit(allocator);
        var result = try preference_loss.pairedPreferenceLoss(allocator, batch.batch(), .{
            .kind = .dpo,
            .beta = recipe.preference.beta orelse 0.1,
            .simpo_gamma = recipe.preference.simpo_gamma orelse 0.5,
            .sft_lambda = recipe.preference.sft_lambda orelse 1.0,
            .ipo_tau = recipe.preference.ipo_tau orelse 0.1,
        });
        defer result.deinit();
        try writeJsonFile(allocator, io, report_path, DpoReport{
            .examples = batch.policy_chosen_logps.len,
            .loss = result.loss,
            .mean_reward_margin = result.mean_reward_margin,
            .accuracy = result.accuracy,
            .beta = recipe.preference.beta orelse 0.1,
        });
        print("dpo report: {s}\n", .{report_path});
        return;
    }
    if (!std.mem.eql(u8, format, "text-preference") and !std.mem.eql(u8, format, "rendered-text-preference")) {
        return error.UnsupportedDpoFormat;
    }
    if (try shouldRunOptimizerBackedQwen2Dpo(recipe, format)) {
        try runOptimizerBackedQwen2Dpo(allocator, io, recipe, path, report_path);
        return;
    }
    if (try shouldRunOptimizerBackedGemmaDpo(recipe, format)) {
        try runOptimizerBackedGemmaDpo(allocator, io, recipe, path, report_path);
        return;
    }

    const policy_path = recipe.model.path orelse return error.MissingModelPath;
    const reference_path = recipe.model.reference_path orelse policy_path;
    const backend_choice = try parseRecipeBackendChoice(recipe.backend);

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, backend_choice);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const policy_model = try model_manager.loadFromDir(policy_path);
    const reference_model = if (std.mem.eql(u8, reference_path, policy_path))
        policy_model
    else
        try model_manager.loadFromDir(reference_path);

    var samples = try loadDpoTextPreferenceSamples(allocator, io, path, recipe, policy_model);
    defer samples.deinit();

    var policy_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = policy_model,
        .max_seq_len = recipe.dataset.max_seq_len orelse 2048,
    };
    var ref_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = reference_model,
        .max_seq_len = recipe.dataset.max_seq_len orelse 2048,
    };
    var result = try preference_harness.pairedStep(allocator, .{
        .ctx = &policy_scorer,
        .call = DecoderLogprobScorer.modelForward,
    }, .{
        .ctx = &ref_scorer,
        .call = DecoderLogprobScorer.modelForward,
    }, samples.samples, .{
        .pref = .{
            .kind = .dpo,
            .beta = recipe.preference.beta orelse 0.1,
            .simpo_gamma = recipe.preference.simpo_gamma orelse 0.5,
            .sft_lambda = recipe.preference.sft_lambda orelse 1.0,
            .ipo_tau = recipe.preference.ipo_tau orelse 0.1,
        },
        .reference_from_disabled_adapter = false,
    });
    defer result.deinit();
    try writeJsonFile(allocator, io, report_path, DpoReport{
        .examples = samples.samples.len,
        .loss = result.loss,
        .mean_reward_margin = result.mean_reward_margin,
        .accuracy = result.accuracy,
        .beta = recipe.preference.beta orelse 0.1,
    });
    print("dpo report: {s}\n", .{report_path});
}

fn shouldRunOptimizerBackedGemmaDpo(recipe: Recipe, format: []const u8) !bool {
    if (!std.mem.eql(u8, format, "text-preference") and !std.mem.eql(u8, format, "rendered-text-preference")) return false;
    const family = recipe.model.family orelse try inferFamily(recipe);
    if (!eqlAny(family, &.{ "gemma4", "gemma" })) return false;
    return recipe.artifacts.trained_adapter_dir != null or recipe.artifacts.adapter_dir != null or recipe.adapter != null;
}

fn shouldRunOptimizerBackedQwen2Dpo(recipe: Recipe, format: []const u8) !bool {
    if (!std.mem.eql(u8, format, "text-preference") and !std.mem.eql(u8, format, "rendered-text-preference")) return false;
    const family = recipe.model.family orelse try inferFamily(recipe);
    if (!isQwen35Family(family) and !eqlAny(family, &.{ "qwen2", "qwen", "colqwen2", "colqwen", "qwen2vl" })) return false;
    return recipe.artifacts.trained_adapter_dir != null or recipe.artifacts.adapter_dir != null or recipe.adapter != null;
}

fn runOptimizerBackedQwen2Sft(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    dataset_path: []const u8,
    report_path: []const u8,
) !void {
    const base_model_dir = recipe.model.path orelse return error.MissingModelPath;
    const adapter = recipe.adapter orelse AdapterConfig{};
    const bootstrap_dir_config = adapter.path orelse adapterBootstrapDir(recipe);
    const bootstrap_dir = bootstrap_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    defer if (bootstrap_dir_config == null) allocator.free(bootstrap_dir);
    const trained_dir_config = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir;
    const trained_dir = trained_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    defer if (trained_dir_config == null) allocator.free(trained_dir);
    const backend_kind: qwen2_real_autodiff.BackendKind = if (std.mem.eql(u8, recipe.backend orelse "native", "mlx")) .mlx else .native;
    const max_examples = recipe.dataset.max_examples orelse 32;
    const max_seq_len = recipe.dataset.max_seq_len orelse 512;
    const family = recipe.model.family orelse try inferFamily(recipe);
    const default_target_modules = qwenLoraTargetModulesForFamily(family);
    try validateNonGemmaAdapterOptions(adapter);
    const bootstrap_target_modules = try adapterTargetModulesForQwen(adapter, default_target_modules);

    compat.cwd().access(compat.io(), bootstrap_dir, .{}) catch {
        var bootstrap = try colqwen2.bootstrapLoRABundle(allocator, base_model_dir, bootstrap_dir, .{
            .rank = adapterRank(adapter, .lora_sft),
            .alpha = adapterAlpha(adapter),
            .base_model_name_or_path = adapter.base_model_name_or_path,
            .target_modules = bootstrap_target_modules,
        });
        defer colqwen2.freeBootstrapSummary(allocator, &bootstrap);
    };

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, try parseRecipeBackendChoice(recipe.backend));
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const tokenizer_model = try model_manager.loadFromDir(base_model_dir);

    var prepared = try loadSftPreparedExamples(allocator, io, dataset_path, recipe, tokenizer_model, max_examples, max_seq_len);
    defer prepared.deinit(allocator);

    const graph_config = try qwen2_real_autodiff.loadGraphConfig(allocator, base_model_dir);
    var backend = try qwen2_real_autodiff.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var adapter_inspect = try colqwen2.inspectCheckpoint(allocator, bootstrap_dir);
    defer colqwen2.freeInspectionSummary(allocator, &adapter_inspect);
    const lora_rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const lora_alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse bootstrap_target_modules;
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = @intCast(lora_rank),
        .alpha = lora_alpha,
        .target_patterns = target_modules,
    };

    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = recipe.optimizer.gradient_accumulation_steps orelse 1,
        .hidden_size_hint = graph_config.arch.hidden_size,
        .num_layers_hint = graph_config.arch.num_hidden_layers,
    });
    defer trainer.deinit();

    var ctx = qwen2_real_autodiff.Qwen2AutodiffCtx.init(graph_config);
    const bootstrap_example = qwen2_real_autodiff.findFirstSupervisedExample(prepared.examples) orelse return error.NoTrainingData;
    try qwen2_real_autodiff.initializeTrainerFromAdapterDir(allocator, &trainer, &ctx, bootstrap_dir, bootstrap_example, @intCast(max_seq_len));

    const epochs = recipe.optimizer.epochs orelse 1;
    var total_loss: f64 = 0.0;
    var examples_seen: usize = 0;
    var supervised_tokens: usize = 0;

    var epoch_idx: usize = 0;
    while (epoch_idx < epochs) : (epoch_idx += 1) {
        for (prepared.examples) |*example| {
            if (example.num_supervised_tokens == 0) continue;
            var input = try qwen2_real_autodiff.makeTrainerInputForExample(allocator, &ctx, example, @intCast(max_seq_len));
            errdefer input.deinit(allocator);
            const step = try trainer.step(input.trainer_input);
            input.deinit(allocator);
            total_loss += step.loss;
            examples_seen += 1;
            supervised_tokens += example.num_supervised_tokens;
        }
    }
    if (examples_seen == 0) return error.NoTrainingData;

    try qwen2_real_autodiff.saveTrainerAsQwenAdapterDir(allocator, &trainer, base_model_dir, bootstrap_dir, trained_dir);

    const denom = @as(f64, @floatFromInt(examples_seen));
    try writeJsonFile(allocator, io, report_path, SftReport{
        .examples = examples_seen,
        .supervised_tokens = supervised_tokens,
        .loss = @floatCast(total_loss / denom),
        .epochs = epochs,
        .trained_adapter_dir = trained_dir,
    });
    print("sft report: {s}\ntrained adapter: {s}\n", .{ report_path, trained_dir });
}

const DpoTextRowsOwned = struct {
    arena: std.heap.ArenaAllocator,
    rows: []DpoTextRow,

    fn deinit(self: *DpoTextRowsOwned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn loadDpoTextRows(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    max_examples: usize,
) !DpoTextRowsOwned {
    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    var rows: std.ArrayListUnmanaged(DpoTextRow) = .empty;
    errdefer rows.deinit(aa);
    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        if (rows.items.len >= max_examples) break;
        try rows.append(aa, try std.json.parseFromSliceLeaky(DpoTextRow, aa, line, .{ .ignore_unknown_fields = true }));
    }
    if (rows.items.len == 0) return error.EmptyBatch;
    return .{
        .arena = arena,
        .rows = try rows.toOwnedSlice(aa),
    };
}

fn loadSftPreparedExamples(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    recipe: Recipe,
    model: *model_manager_mod.LoadedModel,
    max_examples: usize,
    max_seq_len: usize,
) !SftPreparedExamplesOwned {
    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var examples: std.ArrayList(gemma4.PreparedExampleInput) = .empty;
    errdefer {
        for (examples.items) |*example| freeGemmaPreparedExample(allocator, example);
        examples.deinit(allocator);
    }

    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        if (examples.items.len >= max_examples) break;
        const row = try std.json.parseFromSliceLeaky(SftTextRow, aa, line, .{ .ignore_unknown_fields = true });
        const completion = row.response orelse row.completion orelse row.chosen orelse return error.EmptyCompletion;
        var prepared = try tokenizeSftTextRow(allocator, model, recipe, row.prompt, completion, max_seq_len);
        errdefer freeGemmaPreparedExample(allocator, &prepared);
        try examples.append(allocator, prepared);
    }
    if (examples.items.len == 0) return error.EmptyBatch;
    return .{ .examples = try examples.toOwnedSlice(allocator) };
}

fn tokenizeSftTextRow(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    recipe: Recipe,
    prompt: []const u8,
    completion: []const u8,
    max_seq_len: usize,
) !gemma4.PreparedExampleInput {
    const tokenizer = model.getTokenizer();
    const render_prompt = !std.mem.eql(u8, recipe.dataset.format orelse "text-sft", "rendered-text-sft");
    const prompt_text = if (render_prompt)
        try renderDpoPrompt(allocator, model, prompt)
    else
        try allocator.dupe(u8, prompt);
    defer allocator.free(prompt_text);

    var prompt_encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        prompt_text,
        max_seq_len,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer prompt_encoded.deinit();

    const prompt_len = countAttentionMask(prompt_encoded.attention_mask);
    if (prompt_len == 0) return error.EmptyPrompt;
    const remaining_budget = max_seq_len - prompt_len;
    if (remaining_budget == 0) return error.NoCompletionBudget;

    const completion_tokens = try tokenizeCompletion(allocator, tokenizer, completion, remaining_budget);
    defer allocator.free(completion_tokens);
    if (completion_tokens.len == 0) return error.EmptyCompletion;

    const prompt_tokens = try allocator.alloc(i32, prompt_len);
    defer allocator.free(prompt_tokens);
    for (0..prompt_len) |idx| prompt_tokens[idx] = prompt_encoded.ids[idx];
    return buildGemmaPreparedExampleFromTokens(allocator, prompt_tokens, completion_tokens, max_seq_len);
}

fn runOptimizerBackedGemmaDpo(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    dataset_path: []const u8,
    report_path: []const u8,
) !void {
    const base_model_dir = recipe.model.path orelse return error.MissingModelPath;
    const adapter = recipe.adapter orelse AdapterConfig{};
    const bootstrap_dir_config = adapter.path orelse adapterBootstrapDir(recipe);
    const bootstrap_dir = bootstrap_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    defer if (bootstrap_dir_config == null) allocator.free(bootstrap_dir);
    const trained_dir_config = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir;
    const trained_dir = trained_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    defer if (trained_dir_config == null) allocator.free(trained_dir);
    const reference_path = recipe.model.reference_path orelse base_model_dir;
    const backend_kind: gemma4_real_autodiff.BackendKind = if (std.mem.eql(u8, recipe.backend orelse "native", "mlx")) .mlx else .native;
    const max_examples = recipe.dataset.max_examples orelse 32;
    const max_seq_len = recipe.dataset.max_seq_len orelse 512;
    try validateGemmaAdapterOptions(adapter);

    compat.cwd().access(compat.io(), bootstrap_dir, .{}) catch {
        var bootstrap = try gemma4.bootstrapLoRABundle(allocator, base_model_dir, bootstrap_dir, .{
            .rank = adapterRank(adapter, .dpo),
            .alpha = adapterAlpha(adapter),
            .base_model_name_or_path = adapter.base_model_name_or_path,
            .target_modules = adapter.target_modules,
            .target_preset = try gemmaTargetPreset(adapter),
            .use_dora = adapter.use_dora orelse false,
            .init_lora_weights = adapter.init_lora_weights,
        });
        defer gemma4.freeBootstrapSummary(allocator, &bootstrap);
    };

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, try parseRecipeBackendChoice(recipe.backend));
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const reference_model = try model_manager.loadFromDir(reference_path);

    var samples = try loadDpoTextPreferenceSamples(allocator, io, dataset_path, recipe, reference_model);
    defer samples.deinit();

    var chosen_prepared = try prepareGemmaDpoPreparedExamplesFromSamples(allocator, base_model_dir, samples.samples, max_examples, max_seq_len, .chosen);
    defer gemma4.freePreparedInputsSummary(allocator, &chosen_prepared);
    var rejected_prepared = try prepareGemmaDpoPreparedExamplesFromSamples(allocator, base_model_dir, samples.samples, max_examples, max_seq_len, .rejected);
    defer gemma4.freePreparedInputsSummary(allocator, &rejected_prepared);
    if (chosen_prepared.examples.len != rejected_prepared.examples.len or chosen_prepared.examples.len != samples.samples.len) {
        return error.DpoBatchAlignmentMismatch;
    }

    const graph_config = try gemma4_real_autodiff.loadGraphConfig(allocator, base_model_dir);
    var backend = try gemma4_real_autodiff.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var adapter_inspect = try gemma4.inspectCheckpoint(allocator, bootstrap_dir);
    defer gemma4.freeInspectionSummary(allocator, &adapter_inspect);
    const lora_rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const lora_alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse (adapter.target_modules orelse gemma4.default_lora_target_modules[0..]);
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = @intCast(lora_rank),
        .alpha = lora_alpha,
        .target_patterns = target_modules,
    };

    var trainer = try @import("real_autodiff_trainer.zig").RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = @max((recipe.optimizer.gradient_accumulation_steps orelse 1) * 2, 1),
        .hidden_size_hint = graph_config.hidden_size,
        .num_layers_hint = graph_config.num_hidden_layers,
    });
    defer trainer.deinit();

    var ctx = gemma4_real_autodiff.GemmaAutodiffCtx.init(graph_config);
    const bootstrap_example = gemma4_real_autodiff.findFirstSupervisedExample(chosen_prepared.examples) orelse return error.NoTrainingData;
    try gemma4_real_autodiff.initializeTrainerFromAdapterDir(allocator, &trainer, &ctx, bootstrap_dir, bootstrap_example, @intCast(max_seq_len));

    var ref_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = reference_model,
        .max_seq_len = max_seq_len,
    };

    const epochs = recipe.optimizer.epochs orelse 1;
    var total_loss: f64 = 0.0;
    var total_margin: f64 = 0.0;
    var total_accuracy: f64 = 0.0;
    var examples_seen: usize = 0;
    var single_pc = [_]f32{0};
    var single_pr = [_]f32{0};
    var single_rc = [_]f32{0};
    var single_rr = [_]f32{0};
    var single_cl = [_]u32{0};
    var single_rl = [_]u32{0};
    var single_sft = [_]f32{0};

    var epoch_idx: usize = 0;
    while (epoch_idx < epochs) : (epoch_idx += 1) {
        for (chosen_prepared.examples, rejected_prepared.examples, samples.samples) |*chosen_ex, *rejected_ex, sample| {
            const policy_chosen = try gemma4_real_autodiff.sequenceLogprobForExample(allocator, &trainer, &ctx, chosen_ex, @intCast(max_seq_len));
            const policy_rejected = try gemma4_real_autodiff.sequenceLogprobForExample(allocator, &trainer, &ctx, rejected_ex, @intCast(max_seq_len));

            try DecoderLogprobScorer.modelForward(
                @ptrCast(&ref_scorer),
                &.{sample.prompt_tokens},
                &.{sample.chosen_tokens},
                single_rc[0..1],
            );
            try DecoderLogprobScorer.modelForward(
                @ptrCast(&ref_scorer),
                &.{sample.prompt_tokens},
                &.{sample.rejected_tokens},
                single_rr[0..1],
            );

            single_pc[0] = policy_chosen;
            single_pr[0] = policy_rejected;
            single_cl[0] = @intCast(sample.chosen_tokens.len);
            single_rl[0] = @intCast(sample.rejected_tokens.len);
            single_sft[0] = sample.sft_chosen_loss orelse 0;

            var step_result = try preference_loss.pairedPreferenceLoss(allocator, .{
                .policy_chosen_logps = single_pc[0..1],
                .policy_rejected_logps = single_pr[0..1],
                .ref_chosen_logps = single_rc[0..1],
                .ref_rejected_logps = single_rr[0..1],
                .chosen_lengths = single_cl[0..1],
                .rejected_lengths = single_rl[0..1],
                .sft_chosen_loss = single_sft[0..1],
            }, .{
                .kind = .dpo,
                .beta = recipe.preference.beta orelse 0.1,
                .simpo_gamma = recipe.preference.simpo_gamma orelse 0.5,
                .sft_lambda = recipe.preference.sft_lambda orelse 1.0,
                .ipo_tau = recipe.preference.ipo_tau orelse 0.1,
            });
            defer step_result.deinit();

            total_loss += step_result.loss;
            total_margin += step_result.mean_reward_margin;
            total_accuracy += step_result.accuracy;
            examples_seen += 1;

            var chosen_input = try gemma4_real_autodiff.makeTrainerInputForLogprobCoeff(
                allocator,
                &ctx,
                chosen_ex,
                @intCast(max_seq_len),
                step_result.grad_chosen[0],
            );
            defer chosen_input.deinit(allocator);
            _ = try trainer.step(chosen_input.trainer_input);

            var rejected_input = try gemma4_real_autodiff.makeTrainerInputForLogprobCoeff(
                allocator,
                &ctx,
                rejected_ex,
                @intCast(max_seq_len),
                step_result.grad_rejected[0],
            );
            defer rejected_input.deinit(allocator);
            _ = try trainer.step(rejected_input.trainer_input);
        }
    }

    try gemma4_real_autodiff.saveTrainerAsGemmaBundle(allocator, &trainer, base_model_dir, bootstrap_dir, trained_dir);

    const denom = @as(f64, @floatFromInt(@max(examples_seen, 1)));
    try writeJsonFile(allocator, io, report_path, DpoReport{
        .examples = examples_seen,
        .loss = @floatCast(total_loss / denom),
        .mean_reward_margin = @floatCast(total_margin / denom),
        .accuracy = @floatCast(total_accuracy / denom),
        .beta = recipe.preference.beta orelse 0.1,
    });
    print("dpo report: {s}\ntrained adapter: {s}\n", .{ report_path, trained_dir });
}

fn runOptimizerBackedQwen2Dpo(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    dataset_path: []const u8,
    report_path: []const u8,
) !void {
    const base_model_dir = recipe.model.path orelse return error.MissingModelPath;
    const adapter = recipe.adapter orelse AdapterConfig{};
    const bootstrap_dir_config = adapter.path orelse adapterBootstrapDir(recipe);
    const bootstrap_dir = bootstrap_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    defer if (bootstrap_dir_config == null) allocator.free(bootstrap_dir);
    const trained_dir_config = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir;
    const trained_dir = trained_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    defer if (trained_dir_config == null) allocator.free(trained_dir);
    const reference_path = recipe.model.reference_path orelse base_model_dir;
    const backend_kind: qwen2_real_autodiff.BackendKind = if (std.mem.eql(u8, recipe.backend orelse "native", "mlx")) .mlx else .native;
    const max_examples = recipe.dataset.max_examples orelse 32;
    const max_seq_len = recipe.dataset.max_seq_len orelse 512;
    const family = recipe.model.family orelse try inferFamily(recipe);
    const default_target_modules = qwenLoraTargetModulesForFamily(family);
    try validateNonGemmaAdapterOptions(adapter);
    const bootstrap_target_modules = try adapterTargetModulesForQwen(adapter, default_target_modules);

    compat.cwd().access(compat.io(), bootstrap_dir, .{}) catch {
        var bootstrap = try colqwen2.bootstrapLoRABundle(allocator, base_model_dir, bootstrap_dir, .{
            .rank = adapterRank(adapter, .dpo),
            .alpha = adapterAlpha(adapter),
            .base_model_name_or_path = adapter.base_model_name_or_path,
            .target_modules = bootstrap_target_modules,
        });
        defer colqwen2.freeBootstrapSummary(allocator, &bootstrap);
    };

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, try parseRecipeBackendChoice(recipe.backend));
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const reference_model = try model_manager.loadFromDir(reference_path);

    var samples = try loadDpoTextPreferenceSamples(allocator, io, dataset_path, recipe, reference_model);
    defer samples.deinit();

    var chosen_prepared = try prepareGemmaDpoPreparedExamplesFromSamples(allocator, base_model_dir, samples.samples, max_examples, max_seq_len, .chosen);
    defer gemma4.freePreparedInputsSummary(allocator, &chosen_prepared);
    var rejected_prepared = try prepareGemmaDpoPreparedExamplesFromSamples(allocator, base_model_dir, samples.samples, max_examples, max_seq_len, .rejected);
    defer gemma4.freePreparedInputsSummary(allocator, &rejected_prepared);
    if (chosen_prepared.examples.len != rejected_prepared.examples.len or chosen_prepared.examples.len != samples.samples.len) {
        return error.DpoBatchAlignmentMismatch;
    }

    const graph_config = try qwen2_real_autodiff.loadGraphConfig(allocator, base_model_dir);
    var backend = try qwen2_real_autodiff.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var adapter_inspect = try colqwen2.inspectCheckpoint(allocator, bootstrap_dir);
    defer colqwen2.freeInspectionSummary(allocator, &adapter_inspect);
    const lora_rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const lora_alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse bootstrap_target_modules;
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = @intCast(lora_rank),
        .alpha = lora_alpha,
        .target_patterns = target_modules,
    };

    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = @max((recipe.optimizer.gradient_accumulation_steps orelse 1) * 2, 1),
        .hidden_size_hint = graph_config.arch.hidden_size,
        .num_layers_hint = graph_config.arch.num_hidden_layers,
    });
    defer trainer.deinit();

    var ctx = qwen2_real_autodiff.Qwen2AutodiffCtx.init(graph_config);
    const bootstrap_example = qwen2_real_autodiff.findFirstSupervisedExample(chosen_prepared.examples) orelse return error.NoTrainingData;
    try qwen2_real_autodiff.initializeTrainerFromAdapterDir(allocator, &trainer, &ctx, bootstrap_dir, bootstrap_example, @intCast(max_seq_len));

    var ref_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = reference_model,
        .max_seq_len = max_seq_len,
    };

    const epochs = recipe.optimizer.epochs orelse 1;
    var total_loss: f64 = 0.0;
    var total_margin: f64 = 0.0;
    var total_accuracy: f64 = 0.0;
    var examples_seen: usize = 0;
    var single_pc = [_]f32{0};
    var single_pr = [_]f32{0};
    var single_rc = [_]f32{0};
    var single_rr = [_]f32{0};
    var single_cl = [_]u32{0};
    var single_rl = [_]u32{0};
    var single_sft = [_]f32{0};

    var epoch_idx: usize = 0;
    while (epoch_idx < epochs) : (epoch_idx += 1) {
        for (chosen_prepared.examples, rejected_prepared.examples, samples.samples) |*chosen_ex, *rejected_ex, sample| {
            const policy_chosen = try qwen2_real_autodiff.sequenceLogprobForExample(allocator, &trainer, &ctx, chosen_ex, @intCast(max_seq_len));
            const policy_rejected = try qwen2_real_autodiff.sequenceLogprobForExample(allocator, &trainer, &ctx, rejected_ex, @intCast(max_seq_len));

            try DecoderLogprobScorer.modelForward(@ptrCast(&ref_scorer), &.{sample.prompt_tokens}, &.{sample.chosen_tokens}, single_rc[0..1]);
            try DecoderLogprobScorer.modelForward(@ptrCast(&ref_scorer), &.{sample.prompt_tokens}, &.{sample.rejected_tokens}, single_rr[0..1]);

            single_pc[0] = policy_chosen;
            single_pr[0] = policy_rejected;
            single_cl[0] = @intCast(sample.chosen_tokens.len);
            single_rl[0] = @intCast(sample.rejected_tokens.len);
            single_sft[0] = sample.sft_chosen_loss orelse 0;

            var step_result = try preference_loss.pairedPreferenceLoss(allocator, .{
                .policy_chosen_logps = single_pc[0..1],
                .policy_rejected_logps = single_pr[0..1],
                .ref_chosen_logps = single_rc[0..1],
                .ref_rejected_logps = single_rr[0..1],
                .chosen_lengths = single_cl[0..1],
                .rejected_lengths = single_rl[0..1],
                .sft_chosen_loss = single_sft[0..1],
            }, .{
                .kind = .dpo,
                .beta = recipe.preference.beta orelse 0.1,
                .simpo_gamma = recipe.preference.simpo_gamma orelse 0.5,
                .sft_lambda = recipe.preference.sft_lambda orelse 1.0,
                .ipo_tau = recipe.preference.ipo_tau orelse 0.1,
            });
            defer step_result.deinit();

            total_loss += step_result.loss;
            total_margin += step_result.mean_reward_margin;
            total_accuracy += step_result.accuracy;
            examples_seen += 1;

            {
                var chosen_input = try qwen2_real_autodiff.makeTrainerInputForLogprobCoeff(allocator, &ctx, chosen_ex, @intCast(max_seq_len), step_result.grad_chosen[0]);
                defer chosen_input.deinit(allocator);
                _ = try trainer.step(chosen_input.trainer_input);
            }

            {
                var rejected_input = try qwen2_real_autodiff.makeTrainerInputForLogprobCoeff(allocator, &ctx, rejected_ex, @intCast(max_seq_len), step_result.grad_rejected[0]);
                defer rejected_input.deinit(allocator);
                _ = try trainer.step(rejected_input.trainer_input);
            }
        }
    }

    try qwen2_real_autodiff.saveTrainerAsQwenAdapterDir(allocator, &trainer, base_model_dir, bootstrap_dir, trained_dir);

    const denom = @as(f64, @floatFromInt(@max(examples_seen, 1)));
    try writeJsonFile(allocator, io, report_path, DpoReport{
        .examples = examples_seen,
        .loss = @floatCast(total_loss / denom),
        .mean_reward_margin = @floatCast(total_margin / denom),
        .accuracy = @floatCast(total_accuracy / denom),
        .beta = recipe.preference.beta orelse 0.1,
    });
    print("dpo report: {s}\ntrained adapter: {s}\n", .{ report_path, trained_dir });
}

const DpoPreparedSide = enum { chosen, rejected };

fn prepareGemmaDpoPreparedExamplesFromSamples(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    samples: []const preference_harness.PreferenceSample,
    max_examples: usize,
    max_seq_len: usize,
    side: DpoPreparedSide,
) !gemma4.PreparedInputsSummary {
    const limit = @min(samples.len, max_examples);
    const examples = try allocator.alloc(gemma4.PreparedExampleInput, limit);
    var built_count: usize = 0;
    errdefer {
        for (examples[0..built_count]) |*example| freeGemmaPreparedExample(allocator, example);
        allocator.free(examples);
    }

    for (samples[0..limit], 0..) |sample, idx| {
        const completion = switch (side) {
            .chosen => sample.chosen_tokens,
            .rejected => sample.rejected_tokens,
        };
        examples[idx] = try buildGemmaPreparedExampleFromTokens(allocator, sample.prompt_tokens, completion, max_seq_len);
        built_count += 1;
    }

    var max_prompt_tokens: usize = 0;
    var max_response_tokens: usize = 0;
    var max_input_tokens: usize = 0;
    var max_supervised_tokens: usize = 0;
    for (examples[0..limit]) |example| {
        max_prompt_tokens = @max(max_prompt_tokens, example.num_prompt_tokens);
        max_response_tokens = @max(max_response_tokens, example.num_response_tokens);
        max_input_tokens = @max(max_input_tokens, example.num_input_tokens);
        max_supervised_tokens = @max(max_supervised_tokens, example.num_supervised_tokens);
    }

    return .{
        .artifact_family_version = try allocator.dupe(u8, gemma4.artifact_family_version),
        .model_dir = try allocator.dupe(u8, model_dir),
        .max_examples = max_examples,
        .examples_seen = limit,
        .max_seq_len = max_seq_len,
        .max_prompt_tokens = max_prompt_tokens,
        .max_response_tokens = max_response_tokens,
        .max_input_tokens = max_input_tokens,
        .max_supervised_tokens = max_supervised_tokens,
        .examples = examples,
    };
}

fn shouldRunOptimizerBackedGemmaGrpo(recipe: Recipe, format: []const u8) !bool {
    if (!std.mem.eql(u8, format, "text-grpo") and !std.mem.eql(u8, format, "rendered-text-grpo")) return false;
    const family = recipe.model.family orelse try inferFamily(recipe);
    if (!eqlAny(family, &.{ "gemma4", "gemma" })) return false;
    return recipe.artifacts.trained_adapter_dir != null or recipe.artifacts.adapter_dir != null or recipe.adapter != null;
}

fn shouldRunOptimizerBackedQwen2Grpo(recipe: Recipe, format: []const u8) !bool {
    if (!std.mem.eql(u8, format, "text-grpo") and !std.mem.eql(u8, format, "rendered-text-grpo")) return false;
    const family = recipe.model.family orelse try inferFamily(recipe);
    if (!isQwen35Family(family) and !eqlAny(family, &.{ "qwen2", "qwen", "colqwen2", "colqwen", "qwen2vl" })) return false;
    return recipe.artifacts.trained_adapter_dir != null or recipe.artifacts.adapter_dir != null or recipe.adapter != null;
}

fn parseTextRewardMode(value: []const u8) !TextRewardMode {
    if (std.mem.eql(u8, value, "exact-match")) return .exact_match;
    if (std.mem.eql(u8, value, "exact-match-ci")) return .exact_match_ci;
    if (std.mem.eql(u8, value, "prefix-match")) return .prefix_match;
    return error.UnsupportedRewardMode;
}

fn runOptimizerBackedGemmaGrpo(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    dataset_path: []const u8,
    report_path: []const u8,
) !void {
    const base_model_dir = recipe.model.path orelse return error.MissingModelPath;
    const adapter = recipe.adapter orelse AdapterConfig{};
    const bootstrap_dir_config = adapter.path orelse adapterBootstrapDir(recipe);
    const bootstrap_dir = bootstrap_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    defer if (bootstrap_dir_config == null) allocator.free(bootstrap_dir);
    const trained_dir_config = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir;
    const trained_dir = trained_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    defer if (trained_dir_config == null) allocator.free(trained_dir);
    const reference_path = recipe.model.reference_path orelse base_model_dir;
    const backend_kind: gemma4_real_autodiff.BackendKind = if (std.mem.eql(u8, recipe.backend orelse "native", "mlx")) .mlx else .native;
    const max_seq_len = recipe.dataset.max_seq_len orelse 128;
    const group_size = recipe.grpo.group_size orelse 2;
    const max_completion_tokens = recipe.grpo.max_completion_tokens orelse 4;
    const reward_mode = try parseTextRewardMode(recipe.grpo.reward_mode orelse "exact-match");
    try validateGemmaAdapterOptions(adapter);

    compat.cwd().access(compat.io(), bootstrap_dir, .{}) catch {
        var bootstrap = try gemma4.bootstrapLoRABundle(allocator, base_model_dir, bootstrap_dir, .{
            .rank = adapterRank(adapter, .grpo),
            .alpha = adapterAlpha(adapter),
            .base_model_name_or_path = adapter.base_model_name_or_path,
            .target_modules = adapter.target_modules,
            .target_preset = try gemmaTargetPreset(adapter),
            .use_dora = adapter.use_dora orelse false,
            .init_lora_weights = adapter.init_lora_weights,
        });
        defer gemma4.freeBootstrapSummary(allocator, &bootstrap);
    };

    if (recipe.model.projector_path != null) {
        try runOptimizerBackedGemmaMultimodalGrpo(
            allocator,
            io,
            recipe,
            dataset_path,
            report_path,
            base_model_dir,
            bootstrap_dir,
            trained_dir,
            reference_path,
            backend_kind,
            max_seq_len,
            group_size,
            max_completion_tokens,
            reward_mode,
        );
        return;
    }

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, try parseRecipeBackendChoice(recipe.backend));
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const tokenizer_model = try model_manager.loadFromDir(base_model_dir);
    const reference_model = if (std.mem.eql(u8, reference_path, base_model_dir))
        tokenizer_model
    else
        try model_manager.loadFromDir(reference_path);

    var prompt_batch = try loadGrpoTextPrompts(allocator, io, dataset_path, recipe, tokenizer_model);
    defer prompt_batch.deinit();

    const graph_config = try gemma4_real_autodiff.loadGraphConfig(allocator, base_model_dir);
    var backend = try gemma4_real_autodiff.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var adapter_inspect = try gemma4.inspectCheckpoint(allocator, bootstrap_dir);
    defer gemma4.freeInspectionSummary(allocator, &adapter_inspect);
    const lora_rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const lora_alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse (adapter.target_modules orelse gemma4.default_lora_target_modules[0..]);
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = @intCast(lora_rank),
        .alpha = lora_alpha,
        .target_patterns = target_modules,
    };

    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = recipe.optimizer.gradient_accumulation_steps orelse 1,
        .hidden_size_hint = graph_config.hidden_size,
        .num_layers_hint = graph_config.num_hidden_layers,
    });
    defer trainer.deinit();

    var ctx = gemma4_real_autodiff.GemmaAutodiffCtx.init(graph_config);
    const bootstrap_example = try buildGemmaPreparedExampleFromTokens(allocator, prompt_batch.prompts[0], &.{}, max_seq_len);
    defer freeGemmaPreparedExample(allocator, &bootstrap_example);
    try gemma4_real_autodiff.initializeTrainerFromAdapterDir(allocator, &trainer, &ctx, bootstrap_dir, &bootstrap_example, @intCast(max_seq_len));

    var ref_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = reference_model,
        .max_seq_len = max_seq_len,
    };
    var rewarder_ctx = TextRewardCtx{
        .allocator = allocator,
        .tokenizer = tokenizer_model.getTokenizer(),
        .targets = prompt_batch.targets,
        .mode = reward_mode,
    };

    const rewarder = grpo.Rewarder{
        .ctx = &rewarder_ctx,
        .call = TextRewardCtx.score,
    };
    const cfg = grpo.GRPOConfig{
        .group_size = group_size,
        .clip_epsilon = recipe.grpo.clip_epsilon orelse 0.2,
        .kl_coef = recipe.grpo.kl_coef orelse 0.04,
        .advantage_eps = recipe.grpo.advantage_eps orelse 1e-8,
        .normalize_advantage = recipe.grpo.normalize_advantage orelse true,
    };

    var total_loss: f64 = 0.0;
    var total_pg_loss: f64 = 0.0;
    var total_kl_loss: f64 = 0.0;
    var total_clip_fraction: f64 = 0.0;
    var total_groups: usize = 0;
    var total_completions: usize = 0;
    var total_tokens: usize = 0;

    const top_rank_cap: usize = @min(group_size, 8);
    const eos_id = tokenizer_model.getTokenizer().specialTokens().sep_id;
    const epochs = recipe.optimizer.epochs orelse 1;
    var epoch_idx: usize = 0;
    while (epoch_idx < epochs) : (epoch_idx += 1) {
        for (prompt_batch.prompts, 0..) |prompt, prompt_idx| {
            var completions = std.ArrayList(grpo.Completion).empty;
            defer {
                for (completions.items) |completion| {
                    allocator.free(completion.tokens);
                    allocator.free(completion.old_logps);
                    allocator.free(completion.ref_logps);
                }
                completions.deinit(allocator);
            }
            var flat_new_logps = std.ArrayList(f32).empty;
            defer flat_new_logps.deinit(allocator);

            var completion_idx: usize = 0;
            while (completion_idx < group_size) : (completion_idx += 1) {
                var sampled_tokens = std.ArrayList(i32).empty;
                defer sampled_tokens.deinit(allocator);
                var sampled_old_logps = std.ArrayList(f32).empty;
                defer sampled_old_logps.deinit(allocator);
                try gemma4_real_autodiff.sampleCompletionRanked(
                    allocator,
                    &trainer,
                    &ctx,
                    prompt,
                    @intCast(max_seq_len),
                    max_completion_tokens,
                    completion_idx % top_rank_cap,
                    if (eos_id >= 0) eos_id else null,
                    &sampled_tokens,
                    &sampled_old_logps,
                );
                const tokens_owned = try sampled_tokens.toOwnedSlice(allocator);
                errdefer allocator.free(tokens_owned);
                const old_logps_owned = try sampled_old_logps.toOwnedSlice(allocator);
                errdefer allocator.free(old_logps_owned);
                const new_logps_owned = try allocator.alloc(f32, tokens_owned.len);
                defer allocator.free(new_logps_owned);
                try gemma4_real_autodiff.tokenLogprobsForPromptCompletion(
                    allocator,
                    &trainer,
                    &ctx,
                    prompt,
                    tokens_owned,
                    @intCast(max_seq_len),
                    new_logps_owned,
                );
                const ref_logps_owned = try allocator.alloc(f32, tokens_owned.len);
                errdefer allocator.free(ref_logps_owned);
                try DecoderLogprobScorer.tokenLogprobs(
                    @ptrCast(&ref_scorer),
                    prompt,
                    tokens_owned,
                    ref_logps_owned,
                );
                try flat_new_logps.appendSlice(allocator, new_logps_owned);
                try completions.append(allocator, .{
                    .prompt_idx = prompt_idx,
                    .tokens = tokens_owned,
                    .old_logps = old_logps_owned,
                    .ref_logps = ref_logps_owned,
                });
                total_tokens += tokens_owned.len;
            }

            var ga = try grpo.scoreGroup(allocator, rewarder, completions.items);
            defer ga.deinit();
            grpo.computeAdvantages(&ga, completions.items, cfg);

            var loss_result = try grpo.grpoLoss(allocator, completions.items, flat_new_logps.items, ga.advantages, cfg);
            defer loss_result.deinit();

            total_loss += loss_result.loss;
            total_pg_loss += loss_result.pg_loss;
            total_kl_loss += loss_result.kl_loss;
            total_clip_fraction += loss_result.clip_fraction;
            total_groups += 1;
            total_completions += completions.items.len;

            var token_offset: usize = 0;
            for (completions.items) |completion| {
                var prepared = try buildGemmaPreparedExampleFromTokens(allocator, prompt, completion.tokens, max_seq_len);
                defer freeGemmaPreparedExample(allocator, &prepared);
                const grads = loss_result.grad_new_logps[token_offset .. token_offset + completion.tokens.len];
                var input = try gemma4_real_autodiff.makeTrainerInputForTokenLogprobGrads(
                    allocator,
                    &ctx,
                    &prepared,
                    @intCast(max_seq_len),
                    grads,
                );
                defer input.deinit(allocator);
                _ = try trainer.step(input.trainer_input);
                token_offset += completion.tokens.len;
            }
        }
    }

    try gemma4_real_autodiff.saveTrainerAsGemmaBundle(allocator, &trainer, base_model_dir, bootstrap_dir, trained_dir);

    const denom = @as(f64, @floatFromInt(@max(total_groups, 1)));
    try writeJsonFile(allocator, io, report_path, GrpoReport{
        .completions = total_completions,
        .tokens = total_tokens,
        .groups = total_groups,
        .loss = @floatCast(total_loss / denom),
        .pg_loss = @floatCast(total_pg_loss / denom),
        .kl_loss = @floatCast(total_kl_loss / denom),
        .clip_fraction = @floatCast(total_clip_fraction / denom),
    });
    print("grpo report: {s}\ntrained adapter: {s}\n", .{ report_path, trained_dir });
}

fn runOptimizerBackedQwen2Grpo(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    dataset_path: []const u8,
    report_path: []const u8,
) !void {
    const base_model_dir = recipe.model.path orelse return error.MissingModelPath;
    const adapter = recipe.adapter orelse AdapterConfig{};
    const bootstrap_dir_config = adapter.path orelse adapterBootstrapDir(recipe);
    const bootstrap_dir = bootstrap_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-bootstrap");
    defer if (bootstrap_dir_config == null) allocator.free(bootstrap_dir);
    const trained_dir_config = recipe.artifacts.trained_adapter_dir orelse recipe.artifacts.adapter_dir;
    const trained_dir = trained_dir_config orelse try defaultArtifactPath(allocator, recipe, "adapter-trained");
    defer if (trained_dir_config == null) allocator.free(trained_dir);
    const reference_path = recipe.model.reference_path orelse base_model_dir;
    const backend_kind: qwen2_real_autodiff.BackendKind = if (std.mem.eql(u8, recipe.backend orelse "native", "mlx")) .mlx else .native;
    const max_seq_len = recipe.dataset.max_seq_len orelse 128;
    const group_size = recipe.grpo.group_size orelse 2;
    const max_completion_tokens = recipe.grpo.max_completion_tokens orelse 4;
    const reward_mode = try parseTextRewardMode(recipe.grpo.reward_mode orelse "exact-match");
    const family = recipe.model.family orelse try inferFamily(recipe);
    const default_target_modules = qwenLoraTargetModulesForFamily(family);
    try validateNonGemmaAdapterOptions(adapter);
    const bootstrap_target_modules = try adapterTargetModulesForQwen(adapter, default_target_modules);

    compat.cwd().access(compat.io(), bootstrap_dir, .{}) catch {
        var bootstrap = try colqwen2.bootstrapLoRABundle(allocator, base_model_dir, bootstrap_dir, .{
            .rank = adapterRank(adapter, .grpo),
            .alpha = adapterAlpha(adapter),
            .base_model_name_or_path = adapter.base_model_name_or_path,
            .target_modules = bootstrap_target_modules,
        });
        defer colqwen2.freeBootstrapSummary(allocator, &bootstrap);
    };

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, try parseRecipeBackendChoice(recipe.backend));
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const tokenizer_model = try model_manager.loadFromDir(base_model_dir);
    const reference_model = if (std.mem.eql(u8, reference_path, base_model_dir))
        tokenizer_model
    else
        try model_manager.loadFromDir(reference_path);

    var prompt_batch = try loadGrpoTextPrompts(allocator, io, dataset_path, recipe, tokenizer_model);
    defer prompt_batch.deinit();

    const graph_config = try qwen2_real_autodiff.loadGraphConfig(allocator, base_model_dir);
    var backend = try qwen2_real_autodiff.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var adapter_inspect = try colqwen2.inspectCheckpoint(allocator, bootstrap_dir);
    defer colqwen2.freeInspectionSummary(allocator, &adapter_inspect);
    const lora_rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const lora_alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse bootstrap_target_modules;
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = @intCast(lora_rank),
        .alpha = lora_alpha,
        .target_patterns = target_modules,
    };

    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = recipe.optimizer.gradient_accumulation_steps orelse 1,
        .hidden_size_hint = graph_config.arch.hidden_size,
        .num_layers_hint = graph_config.arch.num_hidden_layers,
    });
    defer trainer.deinit();

    var ctx = qwen2_real_autodiff.Qwen2AutodiffCtx.init(graph_config);
    const bootstrap_example = try buildGemmaPreparedExampleFromTokens(allocator, prompt_batch.prompts[0], &.{}, max_seq_len);
    defer freeGemmaPreparedExample(allocator, &bootstrap_example);
    try qwen2_real_autodiff.initializeTrainerFromAdapterDir(allocator, &trainer, &ctx, bootstrap_dir, &bootstrap_example, @intCast(max_seq_len));

    var ref_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = reference_model,
        .max_seq_len = max_seq_len,
    };
    var rewarder_ctx = TextRewardCtx{
        .allocator = allocator,
        .tokenizer = tokenizer_model.getTokenizer(),
        .targets = prompt_batch.targets,
        .mode = reward_mode,
    };

    const rewarder = grpo.Rewarder{
        .ctx = &rewarder_ctx,
        .call = TextRewardCtx.score,
    };
    const cfg = grpo.GRPOConfig{
        .group_size = group_size,
        .clip_epsilon = recipe.grpo.clip_epsilon orelse 0.2,
        .kl_coef = recipe.grpo.kl_coef orelse 0.04,
        .advantage_eps = recipe.grpo.advantage_eps orelse 1e-8,
        .normalize_advantage = recipe.grpo.normalize_advantage orelse true,
    };

    var total_loss: f64 = 0.0;
    var total_pg_loss: f64 = 0.0;
    var total_kl_loss: f64 = 0.0;
    var total_clip_fraction: f64 = 0.0;
    var total_groups: usize = 0;
    var total_completions: usize = 0;
    var total_tokens: usize = 0;

    const top_rank_cap: usize = @min(group_size, 8);
    const eos_id = tokenizer_model.getTokenizer().specialTokens().sep_id;
    const epochs = recipe.optimizer.epochs orelse 1;
    var epoch_idx: usize = 0;
    while (epoch_idx < epochs) : (epoch_idx += 1) {
        for (prompt_batch.prompts, 0..) |prompt, prompt_idx| {
            var completions = std.ArrayList(grpo.Completion).empty;
            defer {
                for (completions.items) |completion| {
                    allocator.free(completion.tokens);
                    allocator.free(completion.old_logps);
                    allocator.free(completion.ref_logps);
                }
                completions.deinit(allocator);
            }
            var flat_new_logps = std.ArrayList(f32).empty;
            defer flat_new_logps.deinit(allocator);

            var completion_idx: usize = 0;
            while (completion_idx < group_size) : (completion_idx += 1) {
                var sampled_tokens = std.ArrayList(i32).empty;
                defer sampled_tokens.deinit(allocator);
                var sampled_old_logps = std.ArrayList(f32).empty;
                defer sampled_old_logps.deinit(allocator);
                try qwen2_real_autodiff.sampleCompletionRanked(
                    allocator,
                    &trainer,
                    &ctx,
                    prompt,
                    @intCast(max_seq_len),
                    max_completion_tokens,
                    completion_idx % top_rank_cap,
                    if (eos_id >= 0) eos_id else null,
                    &sampled_tokens,
                    &sampled_old_logps,
                );
                const tokens_owned = try sampled_tokens.toOwnedSlice(allocator);
                errdefer allocator.free(tokens_owned);
                const old_logps_owned = try sampled_old_logps.toOwnedSlice(allocator);
                errdefer allocator.free(old_logps_owned);
                const new_logps_owned = try allocator.alloc(f32, tokens_owned.len);
                defer allocator.free(new_logps_owned);
                try qwen2_real_autodiff.tokenLogprobsForPromptCompletion(
                    allocator,
                    &trainer,
                    &ctx,
                    prompt,
                    tokens_owned,
                    @intCast(max_seq_len),
                    new_logps_owned,
                );
                const ref_logps_owned = try allocator.alloc(f32, tokens_owned.len);
                errdefer allocator.free(ref_logps_owned);
                try DecoderLogprobScorer.tokenLogprobs(@ptrCast(&ref_scorer), prompt, tokens_owned, ref_logps_owned);
                try flat_new_logps.appendSlice(allocator, new_logps_owned);
                try completions.append(allocator, .{
                    .prompt_idx = prompt_idx,
                    .tokens = tokens_owned,
                    .old_logps = old_logps_owned,
                    .ref_logps = ref_logps_owned,
                });
                total_tokens += tokens_owned.len;
            }

            var ga = try grpo.scoreGroup(allocator, rewarder, completions.items);
            defer ga.deinit();
            grpo.computeAdvantages(&ga, completions.items, cfg);

            var loss_result = try grpo.grpoLoss(allocator, completions.items, flat_new_logps.items, ga.advantages, cfg);
            defer loss_result.deinit();

            total_loss += loss_result.loss;
            total_pg_loss += loss_result.pg_loss;
            total_kl_loss += loss_result.kl_loss;
            total_clip_fraction += loss_result.clip_fraction;
            total_groups += 1;
            total_completions += completions.items.len;

            var token_offset: usize = 0;
            for (completions.items) |completion| {
                var prepared = try buildGemmaPreparedExampleFromTokens(allocator, prompt, completion.tokens, max_seq_len);
                defer freeGemmaPreparedExample(allocator, &prepared);
                const grads = loss_result.grad_new_logps[token_offset .. token_offset + completion.tokens.len];
                var input = try qwen2_real_autodiff.makeTrainerInputForTokenLogprobGrads(allocator, &ctx, &prepared, @intCast(max_seq_len), grads);
                defer input.deinit(allocator);
                _ = try trainer.step(input.trainer_input);
                token_offset += completion.tokens.len;
            }
        }
    }

    try qwen2_real_autodiff.saveTrainerAsQwenAdapterDir(allocator, &trainer, base_model_dir, bootstrap_dir, trained_dir);

    const denom = @as(f64, @floatFromInt(@max(total_groups, 1)));
    try writeJsonFile(allocator, io, report_path, GrpoReport{
        .completions = total_completions,
        .tokens = total_tokens,
        .groups = total_groups,
        .loss = @floatCast(total_loss / denom),
        .pg_loss = @floatCast(total_pg_loss / denom),
        .kl_loss = @floatCast(total_kl_loss / denom),
        .clip_fraction = @floatCast(total_clip_fraction / denom),
    });
    print("grpo report: {s}\ntrained adapter: {s}\n", .{ report_path, trained_dir });
}

fn runOptimizerBackedGemmaMultimodalGrpo(
    allocator: std.mem.Allocator,
    io: std.Io,
    recipe: Recipe,
    dataset_path: []const u8,
    report_path: []const u8,
    base_model_dir: []const u8,
    bootstrap_dir: []const u8,
    trained_dir: []const u8,
    reference_path: []const u8,
    backend_kind: gemma4_real_autodiff.BackendKind,
    max_seq_len: usize,
    group_size: usize,
    max_completion_tokens: usize,
    reward_mode: TextRewardMode,
) !void {
    const projector_path = recipe.model.projector_path orelse return error.MissingGgufProjector;
    if (!std.mem.eql(u8, reference_path, base_model_dir)) return error.UnsupportedReferencePath;

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, try parseRecipeBackendChoice(recipe.backend));
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    const tokenizer_model = try model_manager.loadFromDir(base_model_dir);

    var prompt_batch = try loadGemmaGrpoPreparedPrompts(allocator, io, dataset_path, recipe, base_model_dir, projector_path);
    defer prompt_batch.deinit();

    const graph_config = try gemma4_real_autodiff.loadGraphConfig(allocator, base_model_dir);
    var backend = try gemma4_real_autodiff.loadBackendForModelDir(allocator, base_model_dir, backend_kind);
    defer backend.deinit();

    var adapter_inspect = try gemma4.inspectCheckpoint(allocator, bootstrap_dir);
    defer gemma4.freeInspectionSummary(allocator, &adapter_inspect);
    const lora_rank = adapter_inspect.lora_rank orelse return error.MissingAdapterConfig;
    const lora_alpha = @as(f32, @floatCast(adapter_inspect.lora_alpha orelse return error.MissingAdapterConfig));
    const target_modules = adapter_inspect.target_modules orelse gemma4.default_lora_target_modules[0..];
    const lora_config = ml.graph.lora.LoRAConfig{
        .rank = @intCast(lora_rank),
        .alpha = lora_alpha,
        .target_patterns = target_modules,
    };

    var trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = recipe.optimizer.gradient_accumulation_steps orelse 1,
        .hidden_size_hint = graph_config.hidden_size,
        .num_layers_hint = graph_config.num_hidden_layers,
    });
    defer trainer.deinit();
    const tokenizer = try gemma4_mm_real_autodiff.loadTokenizerForModelDir(allocator, base_model_dir);
    var ctx = gemma4_mm_real_autodiff.MultimodalCtx.init(allocator, backend.backendPtr(), graph_config, projector_path, prompt_batch.summaries[0].gguf_projector_sha256.?, tokenizer);
    defer ctx.deinit();
    try gemma4_mm_real_autodiff.initializeTrainerFromAdapterDir(allocator, &trainer, &ctx, bootstrap_dir, prompt_batch.prompts[0], @intCast(max_seq_len));

    var ref_trainer = try real_autodiff.RealAutodiffTrainer.init(allocator, backend.backendPtr(), .{
        .lora = lora_config,
        .optimizer = .{},
        .lr_schedule = .{ .constant = recipe.optimizer.learning_rate orelse 0.0001 },
        .max_grad_norm = recipe.optimizer.max_grad_norm orelse 1.0,
        .grad_accum_steps = 1,
        .hidden_size_hint = graph_config.hidden_size,
        .num_layers_hint = graph_config.num_hidden_layers,
    });
    defer ref_trainer.deinit();
    const ref_tokenizer = try gemma4_mm_real_autodiff.loadTokenizerForModelDir(allocator, base_model_dir);
    var ref_ctx = gemma4_mm_real_autodiff.MultimodalCtx.init(allocator, backend.backendPtr(), graph_config, projector_path, prompt_batch.summaries[0].gguf_projector_sha256.?, ref_tokenizer);
    defer ref_ctx.deinit();
    try gemma4_mm_real_autodiff.initializeTrainerFromAdapterDir(allocator, &ref_trainer, &ref_ctx, bootstrap_dir, prompt_batch.prompts[0], @intCast(max_seq_len));

    var rewarder_ctx = TextRewardCtx{
        .allocator = allocator,
        .tokenizer = tokenizer_model.getTokenizer(),
        .targets = prompt_batch.targets,
        .mode = reward_mode,
    };
    const rewarder = grpo.Rewarder{
        .ctx = &rewarder_ctx,
        .call = TextRewardCtx.score,
    };
    const cfg = grpo.GRPOConfig{
        .group_size = group_size,
        .clip_epsilon = recipe.grpo.clip_epsilon orelse 0.2,
        .kl_coef = recipe.grpo.kl_coef orelse 0.04,
        .advantage_eps = recipe.grpo.advantage_eps orelse 1e-8,
        .normalize_advantage = recipe.grpo.normalize_advantage orelse true,
    };

    var total_loss: f64 = 0.0;
    var total_pg_loss: f64 = 0.0;
    var total_kl_loss: f64 = 0.0;
    var total_clip_fraction: f64 = 0.0;
    var total_groups: usize = 0;
    var total_completions: usize = 0;
    var total_tokens: usize = 0;

    const top_rank_cap: usize = @min(group_size, 8);
    const eos_id = tokenizer_model.getTokenizer().specialTokens().sep_id;
    const epochs = recipe.optimizer.epochs orelse 1;
    var epoch_idx: usize = 0;
    while (epoch_idx < epochs) : (epoch_idx += 1) {
        for (prompt_batch.prompts, 0..) |prompt, prompt_idx| {
            var completions = std.ArrayList(grpo.Completion).empty;
            defer {
                for (completions.items) |completion| {
                    allocator.free(completion.tokens);
                    allocator.free(completion.old_logps);
                    allocator.free(completion.ref_logps);
                }
                completions.deinit(allocator);
            }
            var flat_new_logps = std.ArrayList(f32).empty;
            defer flat_new_logps.deinit(allocator);

            var completion_idx: usize = 0;
            while (completion_idx < group_size) : (completion_idx += 1) {
                var sampled_tokens = std.ArrayList(i32).empty;
                defer sampled_tokens.deinit(allocator);
                var sampled_old_logps = std.ArrayList(f32).empty;
                defer sampled_old_logps.deinit(allocator);
                try sampleGemmaMultimodalCompletionRanked(
                    allocator,
                    &trainer,
                    &ctx,
                    prompt,
                    max_seq_len,
                    max_completion_tokens,
                    completion_idx % top_rank_cap,
                    if (eos_id >= 0) eos_id else null,
                    &sampled_tokens,
                    &sampled_old_logps,
                );
                const tokens_owned = try sampled_tokens.toOwnedSlice(allocator);
                errdefer allocator.free(tokens_owned);
                const old_logps_owned = try sampled_old_logps.toOwnedSlice(allocator);
                errdefer allocator.free(old_logps_owned);
                const new_logps_owned = try allocator.alloc(f32, tokens_owned.len);
                defer allocator.free(new_logps_owned);
                try scoreGemmaMultimodalCompletionLogprobs(allocator, &trainer, &ctx, prompt, tokens_owned, max_seq_len, new_logps_owned);
                const ref_logps_owned = try allocator.alloc(f32, tokens_owned.len);
                errdefer allocator.free(ref_logps_owned);
                try scoreGemmaMultimodalCompletionLogprobs(allocator, &ref_trainer, &ref_ctx, prompt, tokens_owned, max_seq_len, ref_logps_owned);
                try flat_new_logps.appendSlice(allocator, new_logps_owned);
                try completions.append(allocator, .{
                    .prompt_idx = prompt_idx,
                    .tokens = tokens_owned,
                    .old_logps = old_logps_owned,
                    .ref_logps = ref_logps_owned,
                });
                total_tokens += tokens_owned.len;
            }

            var ga = try grpo.scoreGroup(allocator, rewarder, completions.items);
            defer ga.deinit();
            grpo.computeAdvantages(&ga, completions.items, cfg);

            var loss_result = try grpo.grpoLoss(allocator, completions.items, flat_new_logps.items, ga.advantages, cfg);
            defer loss_result.deinit();

            total_loss += loss_result.loss;
            total_pg_loss += loss_result.pg_loss;
            total_kl_loss += loss_result.kl_loss;
            total_clip_fraction += loss_result.clip_fraction;
            total_groups += 1;
            total_completions += completions.items.len;

            var token_offset: usize = 0;
            for (completions.items) |completion| {
                var prepared = try buildGemmaPreparedExampleFromPromptExample(allocator, prompt, completion.tokens, max_seq_len);
                defer freeGemmaPreparedExample(allocator, &prepared);
                const grads = loss_result.grad_new_logps[token_offset .. token_offset + completion.tokens.len];
                var input = try gemma4_mm_real_autodiff.makeTrainerInputForTokenLogprobGrads(
                    allocator,
                    &ctx,
                    &prepared,
                    @intCast(max_seq_len),
                    grads,
                );
                defer input.deinit(allocator);
                _ = try trainer.step(input.trainer_input);
                token_offset += completion.tokens.len;
            }
        }
    }

    try gemma4_real_autodiff.saveTrainerAsGemmaBundle(allocator, &trainer, base_model_dir, bootstrap_dir, trained_dir);

    const denom = @as(f64, @floatFromInt(@max(total_groups, 1)));
    try writeJsonFile(allocator, io, report_path, GrpoReport{
        .completions = total_completions,
        .tokens = total_tokens,
        .groups = total_groups,
        .loss = @floatCast(total_loss / denom),
        .pg_loss = @floatCast(total_pg_loss / denom),
        .kl_loss = @floatCast(total_kl_loss / denom),
        .clip_fraction = @floatCast(total_clip_fraction / denom),
    });
    print("grpo report: {s}\ntrained adapter: {s}\n", .{ report_path, trained_dir });
}

fn sampleGemmaMultimodalCompletionRanked(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *gemma4_mm_real_autodiff.MultimodalCtx,
    prompt: *const gemma4.PreparedExampleInput,
    max_seq_len: usize,
    max_completion_tokens: usize,
    rank: usize,
    eos_token_id: ?i32,
    out_tokens: *std.ArrayList(i32),
    out_logps: *std.ArrayList(f32),
) !void {
    var seq = std.ArrayList(i32).empty;
    defer seq.deinit(allocator);

    var step: usize = 0;
    while (step < max_completion_tokens and prompt.prompt_input_ids.len + seq.items.len < max_seq_len) : (step += 1) {
        var prepared = try buildGemmaPreparedExampleFromPromptExample(allocator, prompt, seq.items, max_seq_len);
        defer freeGemmaPreparedExample(allocator, &prepared);
        const logits = try gemma4_mm_real_autodiff.logitsForExample(allocator, trainer, ctx, &prepared, @intCast(max_seq_len));
        defer allocator.free(logits);
        const vocab_size: usize = @intCast(ctx.graph_config.vocab_size);
        const row = logits[(prompt.prompt_input_ids.len + seq.items.len - 1) * vocab_size ..][0..vocab_size];
        const token_id = try selectRankedTokenFromLogits(allocator, row, rank);
        try out_tokens.append(allocator, token_id);
        try out_logps.append(allocator, logProbAtToken(row, token_id));
        try seq.append(allocator, token_id);
        if (eos_token_id) |eos_id| if (token_id == eos_id) break;
    }
    if (out_tokens.items.len == 0) return error.EmptyCompletion;
}

fn scoreGemmaMultimodalCompletionLogprobs(
    allocator: std.mem.Allocator,
    trainer: *real_autodiff.RealAutodiffTrainer,
    ctx: *gemma4_mm_real_autodiff.MultimodalCtx,
    prompt: *const gemma4.PreparedExampleInput,
    completion: []const i32,
    max_seq_len: usize,
    out_logps: []f32,
) !void {
    if (completion.len != out_logps.len) return error.LogpLenMismatch;
    var prepared = try buildGemmaPreparedExampleFromPromptExample(allocator, prompt, completion, max_seq_len);
    defer freeGemmaPreparedExample(allocator, &prepared);
    const logits = try gemma4_mm_real_autodiff.logitsForExample(allocator, trainer, ctx, &prepared, @intCast(max_seq_len));
    defer allocator.free(logits);
    const vocab_size: usize = @intCast(ctx.graph_config.vocab_size);
    for (completion, 0..) |token_id, idx| {
        const row = logits[(prompt.prompt_input_ids.len + idx - 1) * vocab_size ..][0..vocab_size];
        out_logps[idx] = logProbAtToken(row, token_id);
    }
}

fn buildGemmaPreparedExampleFromTokens(
    allocator: std.mem.Allocator,
    prompt_tokens: []const i32,
    completion_tokens: []const i32,
    max_seq_len: usize,
) !gemma4.PreparedExampleInput {
    if (prompt_tokens.len == 0) return error.EmptyPrompt;
    if (prompt_tokens.len + completion_tokens.len > max_seq_len) return error.SequenceTooLong;

    const prompt_copy = try allocator.dupe(i32, prompt_tokens);
    errdefer allocator.free(prompt_copy);
    const completion_copy = try allocator.dupe(i32, completion_tokens);
    errdefer allocator.free(completion_copy);
    const input_ids = try allocator.alloc(i32, prompt_tokens.len + completion_tokens.len);
    errdefer allocator.free(input_ids);
    @memcpy(input_ids[0..prompt_tokens.len], prompt_tokens);
    @memcpy(input_ids[prompt_tokens.len..], completion_tokens);
    const labels = try allocator.alloc(i32, input_ids.len);
    errdefer allocator.free(labels);
    for (0..prompt_tokens.len) |idx| labels[idx] = -100;
    for (completion_tokens, 0..) |token_id, idx| labels[prompt_tokens.len + idx] = token_id;

    return .{
        .mode = .instruction,
        .prompt_input_ids = prompt_copy,
        .response_input_ids = completion_copy,
        .num_prompt_tokens = prompt_copy.len,
        .num_response_tokens = completion_copy.len,
        .input_ids = input_ids,
        .labels = labels,
        .num_input_tokens = input_ids.len,
        .num_supervised_tokens = completion_copy.len,
    };
}

fn buildGemmaPreparedExampleFromPromptExample(
    allocator: std.mem.Allocator,
    prompt_example: *const gemma4.PreparedExampleInput,
    completion_tokens: []const i32,
    max_seq_len: usize,
) !gemma4.PreparedExampleInput {
    if (prompt_example.prompt_input_ids.len == 0) return error.EmptyPrompt;
    if (prompt_example.prompt_input_ids.len + completion_tokens.len > max_seq_len) return error.SequenceTooLong;

    const prompt_copy = try allocator.dupe(i32, prompt_example.prompt_input_ids);
    errdefer allocator.free(prompt_copy);
    const completion_copy = try allocator.dupe(i32, completion_tokens);
    errdefer allocator.free(completion_copy);
    const input_ids = try allocator.alloc(i32, prompt_copy.len + completion_copy.len);
    errdefer allocator.free(input_ids);
    @memcpy(input_ids[0..prompt_copy.len], prompt_copy);
    @memcpy(input_ids[prompt_copy.len..], completion_copy);
    const labels = try allocator.alloc(i32, input_ids.len);
    errdefer allocator.free(labels);
    for (0..prompt_copy.len) |idx| labels[idx] = -100;
    for (completion_copy, 0..) |token_id, idx| labels[prompt_copy.len + idx] = token_id;

    return .{
        .mode = .instruction,
        .prompt_input_ids = prompt_copy,
        .response_input_ids = completion_copy,
        .num_prompt_tokens = prompt_copy.len,
        .num_response_tokens = completion_copy.len,
        .input_ids = input_ids,
        .labels = labels,
        .num_input_tokens = input_ids.len,
        .num_supervised_tokens = completion_copy.len,
        .image_paths = try dupeStringSlice(allocator, prompt_example.image_paths),
        .audio_paths = try dupeStringSlice(allocator, prompt_example.audio_paths),
        .image_token_counts = try allocator.dupe(usize, prompt_example.image_token_counts),
        .audio_token_counts = try allocator.dupe(usize, prompt_example.audio_token_counts),
    };
}

fn freeGemmaPreparedExample(allocator: std.mem.Allocator, example: *const gemma4.PreparedExampleInput) void {
    allocator.free(example.prompt_input_ids);
    allocator.free(example.response_input_ids);
    allocator.free(example.input_ids);
    allocator.free(example.labels);
    for (example.image_paths) |path| allocator.free(path);
    if (example.image_paths.len > 0) allocator.free(example.image_paths);
    for (example.audio_paths) |path| allocator.free(path);
    if (example.audio_paths.len > 0) allocator.free(example.audio_paths);
    if (example.image_token_counts.len > 0) allocator.free(example.image_token_counts);
    if (example.audio_token_counts.len > 0) allocator.free(example.audio_token_counts);
}

fn dupeStringSlice(allocator: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try allocator.alloc([]const u8, values.len);
    var copied: usize = 0;
    errdefer {
        for (out[0..copied]) |value| allocator.free(value);
        allocator.free(out);
    }
    for (values, 0..) |value, idx| {
        out[idx] = try allocator.dupe(u8, value);
        copied += 1;
    }
    return out;
}

fn loadDpoScalarJsonl(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !DpoBatchOwned {
    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var pc: std.ArrayList(f32) = .empty;
    var pr: std.ArrayList(f32) = .empty;
    var rc: std.ArrayList(f32) = .empty;
    var rr: std.ArrayList(f32) = .empty;
    var cl: std.ArrayList(u32) = .empty;
    var rl: std.ArrayList(u32) = .empty;
    var sft: std.ArrayList(f32) = .empty;
    errdefer {
        pc.deinit(allocator);
        pr.deinit(allocator);
        rc.deinit(allocator);
        rr.deinit(allocator);
        cl.deinit(allocator);
        rl.deinit(allocator);
        sft.deinit(allocator);
    }

    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        const parsed = try std.json.parseFromSliceLeaky(DpoScalarRow, allocator, line, .{ .ignore_unknown_fields = true });
        try pc.append(allocator, parsed.policy_chosen_logp);
        try pr.append(allocator, parsed.policy_rejected_logp);
        try rc.append(allocator, parsed.ref_chosen_logp);
        try rr.append(allocator, parsed.ref_rejected_logp);
        try cl.append(allocator, parsed.chosen_length orelse 0);
        try rl.append(allocator, parsed.rejected_length orelse 0);
        try sft.append(allocator, parsed.sft_chosen_loss orelse 0);
    }
    if (pc.items.len == 0) return error.EmptyBatch;
    return .{
        .policy_chosen_logps = try pc.toOwnedSlice(allocator),
        .policy_rejected_logps = try pr.toOwnedSlice(allocator),
        .ref_chosen_logps = try rc.toOwnedSlice(allocator),
        .ref_rejected_logps = try rr.toOwnedSlice(allocator),
        .chosen_lengths = try cl.toOwnedSlice(allocator),
        .rejected_lengths = try rl.toOwnedSlice(allocator),
        .sft_chosen_loss = try sft.toOwnedSlice(allocator),
    };
}

fn loadDpoTextPreferenceSamples(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    recipe: Recipe,
    policy_model: *model_manager_mod.LoadedModel,
) !DpoPreferenceSamplesOwned {
    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var rows: std.ArrayListUnmanaged(DpoTextRow) = .empty;
    errdefer rows.deinit(arena_alloc);
    var lines = std.mem.splitScalar(u8, raw, '\n');
    const max_examples = recipe.dataset.max_examples orelse std.math.maxInt(usize);
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        if (rows.items.len >= max_examples) break;
        const parsed = try std.json.parseFromSliceLeaky(DpoTextRow, arena_alloc, line, .{ .ignore_unknown_fields = true });
        try rows.append(arena_alloc, parsed);
    }
    if (rows.items.len == 0) return error.EmptyBatch;

    const samples = try arena_alloc.alloc(preference_harness.PreferenceSample, rows.items.len);
    for (rows.items, 0..) |row, idx| {
        const tokenized = try tokenizeDpoTextRow(arena_alloc, policy_model, recipe, row);
        samples[idx] = .{
            .prompt_tokens = tokenized.prompt_tokens,
            .chosen_tokens = tokenized.chosen_tokens,
            .rejected_tokens = tokenized.rejected_tokens,
            .sft_chosen_loss = row.sft_chosen_loss,
        };
    }
    return .{
        .arena = arena,
        .samples = samples,
    };
}

const TokenizedPreferenceRow = struct {
    prompt_tokens: []const i32,
    chosen_tokens: []const i32,
    rejected_tokens: []const i32,
};

fn tokenizeDpoTextRow(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    recipe: Recipe,
    row: DpoTextRow,
) !TokenizedPreferenceRow {
    const tokenizer = model.getTokenizer();
    const max_seq_len = recipe.dataset.max_seq_len orelse 2048;
    const render_prompt = !std.mem.eql(u8, recipe.dataset.format orelse "text-preference", "rendered-text-preference");
    const prompt_text = if (render_prompt)
        try renderDpoPrompt(allocator, model, row.prompt)
    else
        try allocator.dupe(u8, row.prompt);
    defer allocator.free(prompt_text);

    var prompt_encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        prompt_text,
        max_seq_len,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer prompt_encoded.deinit();

    const prompt_len = countAttentionMask(prompt_encoded.attention_mask);
    if (prompt_len == 0) return error.EmptyPrompt;
    const remaining_budget = max_seq_len - prompt_len;
    if (remaining_budget == 0) return error.NoCompletionBudget;

    const chosen_tokens = try tokenizeCompletion(allocator, tokenizer, row.chosen, remaining_budget);
    const rejected_tokens = try tokenizeCompletion(allocator, tokenizer, row.rejected, remaining_budget);
    if (chosen_tokens.len == 0 or rejected_tokens.len == 0) return error.EmptyCompletion;

    const prompt_tokens = try allocator.alloc(i32, prompt_len);
    for (0..prompt_len) |idx| prompt_tokens[idx] = prompt_encoded.ids[idx];
    return .{
        .prompt_tokens = prompt_tokens,
        .chosen_tokens = chosen_tokens,
        .rejected_tokens = rejected_tokens,
    };
}

fn renderDpoPrompt(allocator: std.mem.Allocator, model: *model_manager_mod.LoadedModel, prompt: []const u8) ![]u8 {
    const messages = [_]generation.Message{
        .{ .role = "user", .content = prompt },
    };
    if (model.chat_tmpl) |tmpl| return tmpl.apply(allocator, &messages, true);
    return generation.formatMessages(allocator, &messages);
}

fn tokenizeCompletion(
    allocator: std.mem.Allocator,
    tokenizer: tokenizer_mod.Tokenizer,
    text: []const u8,
    max_tokens: usize,
) ![]const i32 {
    const raw_tokens = try tokenizer.encode(allocator, text);
    defer allocator.free(raw_tokens);
    const token_count = @min(raw_tokens.len, max_tokens);
    if (token_count == 0) return allocator.alloc(i32, 0);
    const out = try allocator.alloc(i32, token_count);
    @memcpy(out, raw_tokens[0..token_count]);
    return out;
}

fn countAttentionMask(mask: []const i32) usize {
    var count: usize = 0;
    for (mask) |value| {
        if (value == 0) break;
        count += 1;
    }
    return count;
}

fn logProbAtToken(logits: []const f32, token_id: i32) f32 {
    if (token_id < 0 or @as(usize, @intCast(token_id)) >= logits.len) return -std.math.inf(f32);
    var max_logit = logits[0];
    for (logits[1..]) |value| {
        if (value > max_logit) max_logit = value;
    }
    var sum_exp: f64 = 0.0;
    for (logits) |value| {
        sum_exp += @exp(@as(f64, value - max_logit));
    }
    const log_z = @as(f64, max_logit) + @log(sum_exp);
    return @as(f32, @floatCast(@as(f64, logits[@intCast(token_id)]) - log_z));
}

fn parseRecipeBackendChoice(value: ?[]const u8) !native_backend_choice.Choice {
    const raw = value orelse return .auto;
    return native_backend_choice.parse(raw) orelse error.InvalidBackend;
}

const GrpoScalarRow = struct {
    prompt_idx: usize,
    tokens: []const i32,
    old_logps: []const f32,
    ref_logps: []const f32,
    new_logps: []const f32,
    reward: f32,
};

const GrpoBatchOwned = struct {
    allocator: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    completions: []grpo.Completion,
    new_logps: []f32,
    rewards: []f32,

    fn deinit(self: *GrpoBatchOwned) void {
        self.allocator.free(self.completions);
        self.allocator.free(self.new_logps);
        self.allocator.free(self.rewards);
        self.arena.deinit();
    }
};

fn runDirectGrpo(allocator: std.mem.Allocator, io: std.Io, recipe: Recipe, report_path: []const u8) !void {
    const path = trainDatasetPath(recipe) orelse return error.MissingDatasetPath;
    const format = recipe.dataset.format orelse "token-logprobs";
    if (std.mem.eql(u8, format, "token-logprobs")) {
        var batch = try loadGrpoScalarJsonl(allocator, io, path);
        defer batch.deinit();
        const cfg = grpo.GRPOConfig{
            .group_size = recipe.grpo.group_size orelse 8,
            .clip_epsilon = recipe.grpo.clip_epsilon orelse 0.2,
            .kl_coef = recipe.grpo.kl_coef orelse 0.04,
            .advantage_eps = recipe.grpo.advantage_eps orelse 1e-8,
            .normalize_advantage = recipe.grpo.normalize_advantage orelse true,
        };
        var ga = grpo.GroupAdvantages{
            .allocator = allocator,
            .rewards = try allocator.dupe(f32, batch.rewards),
            .advantages = try allocator.alloc(f32, batch.completions.len),
            .num_groups = countGrpoGroups(batch.completions),
        };
        defer ga.deinit();
        @memset(ga.advantages, 0);
        grpo.computeAdvantages(&ga, batch.completions, cfg);
        var result = try grpo.grpoLoss(allocator, batch.completions, batch.new_logps, ga.advantages, cfg);
        defer result.deinit();
        try writeJsonFile(allocator, io, report_path, GrpoReport{
            .completions = batch.completions.len,
            .tokens = batch.new_logps.len,
            .groups = ga.num_groups,
            .loss = result.loss,
            .pg_loss = result.pg_loss,
            .kl_loss = result.kl_loss,
            .clip_fraction = result.clip_fraction,
        });
        print("grpo report: {s}\n", .{report_path});
        return;
    }
    if (!std.mem.eql(u8, format, "text-grpo") and !std.mem.eql(u8, format, "rendered-text-grpo")) {
        return error.UnsupportedGrpoFormat;
    }
    if (try shouldRunOptimizerBackedQwen2Grpo(recipe, format)) {
        try runOptimizerBackedQwen2Grpo(allocator, io, recipe, path, report_path);
        return;
    }
    if (try shouldRunOptimizerBackedGemmaGrpo(recipe, format)) {
        try runOptimizerBackedGemmaGrpo(allocator, io, recipe, path, report_path);
        return;
    }

    const policy_path = recipe.model.path orelse return error.MissingModelPath;
    const reference_path = recipe.model.reference_path orelse policy_path;
    const backend_choice = try parseRecipeBackendChoice(recipe.backend);

    var session_manager = backends.SessionManager.init(allocator);
    native_backend_choice.configureSessionPreference(&session_manager, backend_choice);
    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const policy_model = try model_manager.loadFromDir(policy_path);
    const reference_model = if (std.mem.eql(u8, reference_path, policy_path))
        policy_model
    else
        try model_manager.loadFromDir(reference_path);

    var prompt_batch = try loadGrpoTextPrompts(allocator, io, path, recipe, policy_model);
    defer prompt_batch.deinit();

    const reward_mode = try parseTextRewardMode(recipe.grpo.reward_mode orelse "exact-match");

    var sampler = DecoderGrpoSampler{
        .allocator = allocator,
        .model = policy_model,
        .max_seq_len = recipe.dataset.max_seq_len orelse 128,
        .max_completion_tokens = recipe.grpo.max_completion_tokens orelse 4,
    };
    var policy_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = policy_model,
        .max_seq_len = recipe.dataset.max_seq_len orelse 128,
    };
    var ref_scorer = DecoderLogprobScorer{
        .allocator = allocator,
        .model = reference_model,
        .max_seq_len = recipe.dataset.max_seq_len orelse 128,
    };
    var rewarder_ctx = TextRewardCtx{
        .allocator = allocator,
        .tokenizer = policy_model.getTokenizer(),
        .targets = prompt_batch.targets,
        .mode = reward_mode,
    };

    var result = try preference_harness.grpoStep(allocator, prompt_batch.prompts, .{
        .ctx = &sampler,
        .call = DecoderGrpoSampler.sample,
    }, .{
        .ctx = &policy_scorer,
        .call = DecoderLogprobScorer.tokenLogprobs,
    }, .{
        .ctx = &ref_scorer,
        .call = DecoderLogprobScorer.tokenLogprobs,
    }, .{
        .ctx = &rewarder_ctx,
        .call = TextRewardCtx.score,
    }, .{
        .grpo = .{
            .group_size = recipe.grpo.group_size orelse 2,
            .clip_epsilon = recipe.grpo.clip_epsilon orelse 0.2,
            .kl_coef = recipe.grpo.kl_coef orelse 0.04,
            .advantage_eps = recipe.grpo.advantage_eps orelse 1e-8,
            .normalize_advantage = recipe.grpo.normalize_advantage orelse true,
        },
        .num_prompts = prompt_batch.prompts.len,
    });
    defer result.deinit();
    try writeJsonFile(allocator, io, report_path, GrpoReport{
        .completions = prompt_batch.prompts.len * (recipe.grpo.group_size orelse 2),
        .tokens = result.grad_new_logps.len,
        .groups = prompt_batch.prompts.len,
        .loss = result.loss,
        .pg_loss = result.pg_loss,
        .kl_loss = result.kl_loss,
        .clip_fraction = result.clip_fraction,
    });
    print("grpo report: {s}\n", .{report_path});
}

fn loadGrpoScalarJsonl(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !GrpoBatchOwned {
    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();
    var completions: std.ArrayList(grpo.Completion) = .empty;
    var new_logps: std.ArrayList(f32) = .empty;
    var rewards: std.ArrayList(f32) = .empty;
    errdefer {
        completions.deinit(allocator);
        new_logps.deinit(allocator);
        rewards.deinit(allocator);
    }

    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        const row = try std.json.parseFromSliceLeaky(GrpoScalarRow, aa, line, .{ .ignore_unknown_fields = true });
        if (row.tokens.len != row.old_logps.len or row.tokens.len != row.ref_logps.len or row.tokens.len != row.new_logps.len) return error.LogpLenMismatch;
        try completions.append(allocator, .{
            .prompt_idx = row.prompt_idx,
            .tokens = row.tokens,
            .old_logps = row.old_logps,
            .ref_logps = row.ref_logps,
        });
        try new_logps.appendSlice(allocator, row.new_logps);
        try rewards.append(allocator, row.reward);
    }
    if (completions.items.len == 0) return error.EmptyBatch;
    return .{
        .allocator = allocator,
        .arena = arena,
        .completions = try completions.toOwnedSlice(allocator),
        .new_logps = try new_logps.toOwnedSlice(allocator),
        .rewards = try rewards.toOwnedSlice(allocator),
    };
}

fn loadGrpoTextPrompts(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    recipe: Recipe,
    policy_model: *model_manager_mod.LoadedModel,
) !GrpoPromptBatchOwned {
    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    var rows: std.ArrayListUnmanaged(GrpoTextRow) = .empty;
    errdefer rows.deinit(aa);
    var lines = std.mem.splitScalar(u8, raw, '\n');
    const max_examples = recipe.dataset.max_examples orelse std.math.maxInt(usize);
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        if (rows.items.len >= max_examples) break;
        const parsed = try std.json.parseFromSliceLeaky(GrpoTextRow, aa, line, .{ .ignore_unknown_fields = true });
        try rows.append(aa, parsed);
    }
    if (rows.items.len == 0) return error.EmptyBatch;

    const prompts = try aa.alloc([]const i32, rows.items.len);
    const targets = try aa.alloc([]const u8, rows.items.len);
    for (rows.items, 0..) |row, idx| {
        const tokenized_prompt = try tokenizeGrpoPrompt(aa, policy_model, recipe, row.prompt);
        prompts[idx] = tokenized_prompt;
        targets[idx] = row.target;
    }

    return .{
        .arena = arena,
        .prompts = prompts,
        .targets = targets,
    };
}

fn loadGemmaGrpoPreparedPrompts(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    recipe: Recipe,
    base_model_dir: []const u8,
    projector_path: []const u8,
) !GemmaPreparedPromptBatchOwned {
    if (std.mem.eql(u8, recipe.dataset.format orelse "text-grpo", "rendered-text-grpo")) {
        return error.UnsupportedRenderedMultimodalGrpo;
    }

    const raw = try readFileMax(allocator, io, path, 256 * 1024 * 1024);
    defer allocator.free(raw);

    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const aa = arena.allocator();

    var rows: std.ArrayListUnmanaged(GrpoTextRow) = .empty;
    errdefer rows.deinit(aa);
    var lines = std.mem.splitScalar(u8, raw, '\n');
    const max_examples = recipe.dataset.max_examples orelse std.math.maxInt(usize);
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r");
        if (line.len == 0) continue;
        if (rows.items.len >= max_examples) break;
        try rows.append(aa, try std.json.parseFromSliceLeaky(GrpoTextRow, aa, line, .{ .ignore_unknown_fields = true }));
    }
    if (rows.items.len == 0) return error.EmptyBatch;

    const prompts = try allocator.alloc(*const gemma4.PreparedExampleInput, rows.items.len);
    errdefer allocator.free(prompts);
    const summaries = try allocator.alloc(gemma4.PreparedInputsSummary, rows.items.len);
    errdefer allocator.free(summaries);
    const targets = try allocator.alloc([]const u8, rows.items.len);
    errdefer allocator.free(targets);

    var built: usize = 0;
    errdefer {
        for (summaries[0..built]) |*summary| gemma4.freePreparedInputsSummary(allocator, summary);
        for (targets[0..built]) |target| allocator.free(target);
    }

    const max_seq_len = recipe.dataset.max_seq_len orelse 128;
    for (rows.items, 0..) |row, idx| {
        const messages = try allocator.alloc(gemma_chat_data.Message, 1);
        errdefer allocator.free(messages);
        messages[0] = .{ .role = .user, .content = row.prompt };
        const example = gemma_chat_data.Example{
            .messages = messages,
            .image_paths = row.image_paths orelse &.{},
            .audio_paths = row.audio_paths orelse &.{},
        };
        const source = [_]gemma_chat_data.Example{example};
        summaries[idx] = try gemma4.prepareMultimodalInputsFromChatData(allocator, base_model_dir, projector_path, source[0..], 1, max_seq_len);
        allocator.free(messages);
        if (summaries[idx].examples.len == 0) return error.EmptyPrompt;
        prompts[idx] = &summaries[idx].examples[0];
        targets[idx] = try allocator.dupe(u8, row.target);
        built += 1;
    }
    arena.deinit();

    return .{
        .allocator = allocator,
        .prompts = prompts,
        .summaries = summaries,
        .targets = targets,
    };
}

fn tokenizeGrpoPrompt(
    allocator: std.mem.Allocator,
    model: *model_manager_mod.LoadedModel,
    recipe: Recipe,
    prompt: []const u8,
) ![]const i32 {
    const tokenizer = model.getTokenizer();
    const max_seq_len = recipe.dataset.max_seq_len orelse 128;
    const render_prompt = !std.mem.eql(u8, recipe.dataset.format orelse "text-grpo", "rendered-text-grpo");
    const prompt_text = if (render_prompt)
        try renderDpoPrompt(allocator, model, prompt)
    else
        try allocator.dupe(u8, prompt);
    defer allocator.free(prompt_text);

    var encoded = try generation.encodePromptForGeneration(
        tokenizer,
        allocator,
        prompt_text,
        max_seq_len,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer encoded.deinit();
    const prompt_len = countAttentionMask(encoded.attention_mask);
    if (prompt_len == 0) return error.EmptyPrompt;
    const out = try allocator.alloc(i32, prompt_len);
    for (0..prompt_len) |idx| out[idx] = encoded.ids[idx];
    return out;
}

fn countGrpoGroups(completions: []const grpo.Completion) usize {
    var max_prompt: usize = 0;
    var any = false;
    for (completions) |completion| {
        if (!any or completion.prompt_idx > max_prompt) max_prompt = completion.prompt_idx;
        any = true;
    }
    return if (any) max_prompt + 1 else 0;
}

fn selectRankedTokenFromLogits(allocator: std.mem.Allocator, logits: []const f32, rank: usize) !i32 {
    const Entry = struct {
        idx: usize,
        value: f32,
    };
    var entries = try allocator.alloc(Entry, logits.len);
    defer allocator.free(entries);
    for (logits, 0..) |value, idx| {
        entries[idx] = .{ .idx = idx, .value = value };
    }
    std.sort.heap(Entry, entries, {}, struct {
        fn lessThan(_: void, lhs: Entry, rhs: Entry) bool {
            return lhs.value > rhs.value;
        }
    }.lessThan);
    return @intCast(entries[@min(rank, entries.len - 1)].idx);
}

fn argv(allocator: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, values);
    return out.toOwnedSlice(allocator);
}

fn appendMany(allocator: std.mem.Allocator, list: *std.ArrayList([]const u8), values: []const []const u8) !void {
    try list.appendSlice(allocator, values);
}

fn fmtInt(allocator: std.mem.Allocator, value: anytype) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{d}", .{value});
}

fn fmtFloat(allocator: std.mem.Allocator, value: anytype) ![]const u8 {
    return std.fmt.allocPrint(allocator, "{d}", .{value});
}

fn joinCsv(allocator: std.mem.Allocator, values: []const []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    for (values, 0..) |value, idx| {
        if (idx != 0) try out.append(allocator, ',');
        try out.appendSlice(allocator, value);
    }
    return out.toOwnedSlice(allocator);
}

fn eqlName(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn eqlAny(value: []const u8, names: []const []const u8) bool {
    for (names) |name| if (eqlName(value, name)) return true;
    return false;
}

fn isQwen35Family(family: []const u8) bool {
    return eqlAny(family, &.{
        "qwen3_5",
        "qwen3.5",
        "qwen3-5",
        "qwen35",
        "qwen3_5_text",
        "qwen3.5-text",
        "qwen3-5-text",
        "qwen35_text",
        "chandra",
        "chandra-ocr",
    });
}

fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    return std.ascii.indexOfIgnoreCase(haystack, needle) != null;
}

fn containsQwen35Signal(path: []const u8) bool {
    return containsIgnoreCase(path, "qwen3_5") or
        containsIgnoreCase(path, "qwen3.5") or
        containsIgnoreCase(path, "qwen3-5") or
        containsIgnoreCase(path, "qwen35") or
        containsIgnoreCase(path, "chandra");
}

fn freePlan(allocator: std.mem.Allocator, plan: Plan) void {
    freeSteps(allocator, plan.steps);
    allocator.free(plan.steps);
}

fn freeSteps(allocator: std.mem.Allocator, steps: []Step) void {
    for (steps) |step| allocator.free(step.argv);
}

fn installedPackageRoot(allocator: std.mem.Allocator, exe_dir: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, exe_dir, "/zig-out/bin")) {
        return allocator.dupe(u8, std.fs.path.dirname(std.fs.path.dirname(exe_dir).?).?);
    }
    return allocator.dupe(u8, ".");
}

fn readFileMax(allocator: std.mem.Allocator, io: std.Io, path: []const u8, max_bytes: usize) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(max_bytes));
}

fn usage() void {
    print(
        \\usage: antfly inference finetune run <recipe.json> [--dry-run]
        \\       antfly inference finetune smoke-fast [--out-root <path>]
        \\
        \\recipe kinds: sft, lora-sft, qlora-sft, dpo, grpo, reranker, vlm-retrieval
        \\common fields: model, dataset, adapter, optimizer, eval, artifacts
        \\
    , .{});
}

fn usageError() error{InvalidArguments} {
    usage();
    return error.InvalidArguments;
}

test "recipe kind accepts taxonomy spellings" {
    try std.testing.expectEqual(RecipeKind.lora_sft, try parseKind("lora-sft"));
    try std.testing.expectEqual(RecipeKind.qlora_sft, try parseKind("qlora_sft"));
    try std.testing.expectEqual(RecipeKind.vlm_retrieval, try parseKind("vlm-retrieval"));
}

test "family inference keeps qwen3_5 and colqwen distinct from qwen2" {
    try std.testing.expectEqualStrings("qwen3_5", inferFamilyFromModelPath("/models/datalab-to/chandra-ocr-2").?);
    try std.testing.expectEqualStrings("qwen3_5", inferFamilyFromModelPath("/models/Qwen3.5-VL").?);
    try std.testing.expectEqualStrings("colqwen2", inferFamilyFromModelPath("/models/vidore/colqwen2-v1.0-hf").?);
    try std.testing.expectEqualStrings("qwen2", inferFamilyFromModelPath("/models/Qwen2-0.5B").?);
}

test "qwen3_5 text preference recipes route to qwen autodiff planner" {
    const sft = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/chandra-ocr-2" },
        .dataset = .{ .path = "/data/train.jsonl" },
        .adapter = .{ .rank = 8, .alpha = 16 },
        .artifacts = .{ .root = "/tmp/qwen35-sft" },
    };
    const sft_plan = try buildPlan(std.heap.page_allocator, sft);
    defer freePlan(std.heap.page_allocator, sft_plan);
    try std.testing.expectEqual(StepKind.direct_sft, sft_plan.steps[0].kind);
    try std.testing.expect(try shouldRunOptimizerBackedQwen35Sft(sft, "text-sft"));

    const dpo = Recipe{
        .recipe = "dpo",
        .model = .{ .path = "/models/Qwen3.5-VL" },
        .dataset = .{ .path = "/data/prefs.jsonl", .format = "text-preference" },
        .adapter = .{ .rank = 8, .alpha = 16 },
        .artifacts = .{ .root = "/tmp/qwen35-dpo" },
    };
    const dpo_plan = try buildPlan(std.heap.page_allocator, dpo);
    defer freePlan(std.heap.page_allocator, dpo_plan);
    try std.testing.expectEqual(StepKind.direct_dpo, dpo_plan.steps[0].kind);
    try std.testing.expect(try shouldRunOptimizerBackedQwen2Dpo(dpo, "text-preference"));

    var report_only = dpo;
    report_only.adapter = null;
    report_only.artifacts = .{ .root = "/tmp/qwen35-report" };
    const plan = try buildPlan(std.heap.page_allocator, report_only);
    defer freePlan(std.heap.page_allocator, plan);
    try std.testing.expectEqual(StepKind.direct_dpo, plan.steps[0].kind);
    try std.testing.expect(!try shouldRunOptimizerBackedQwen2Dpo(report_only, "text-preference"));

    const grpo_recipe = Recipe{
        .recipe = "grpo",
        .model = .{ .path = "/models/Qwen3.5-VL" },
        .dataset = .{ .path = "/data/prompts.jsonl", .format = "text-grpo" },
        .adapter = .{ .rank = 8, .alpha = 16 },
        .artifacts = .{ .root = "/tmp/qwen35-grpo" },
    };
    const grpo_plan = try buildPlan(std.heap.page_allocator, grpo_recipe);
    defer freePlan(std.heap.page_allocator, grpo_plan);
    try std.testing.expectEqual(StepKind.direct_grpo, grpo_plan.steps[0].kind);
    try std.testing.expect(try shouldRunOptimizerBackedQwen2Grpo(grpo_recipe, "text-grpo"));
}

test "gemma4 lora recipe builds prepare bootstrap train plan" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gemma4", .family = "gemma4" },
        .dataset = .{ .path = "/data/train.jsonl", .max_examples = 4 },
        .adapter = .{ .rank = 4, .alpha = 8 },
        .optimizer = .{ .learning_rate = 0.0002, .epochs = 2 },
        .artifacts = .{ .root = "/tmp/out" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try std.testing.expectEqual(@as(usize, 3), plan.steps.len);
    try std.testing.expectEqualStrings("prepare-gemma4-lora-inputs", plan.steps[0].argv[0]);
    try std.testing.expectEqualStrings("bootstrap-gemma4-lora", plan.steps[1].argv[0]);
    try std.testing.expectEqualStrings("train-eval-gemma4-lora-bundle", plan.steps[2].argv[0]);
}

test "gemma4 lora recipe defaults to all-linear rank16 alpha32" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gemma4", .family = "gemma4" },
        .dataset = .{ .path = "/data/train.jsonl" },
        .artifacts = .{ .root = "/tmp/out" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try std.testing.expectEqualStrings("16", plan.steps[1].argv[3]);
    try std.testing.expectEqualStrings("32", plan.steps[1].argv[4]);
    try std.testing.expectEqualStrings("--target-preset", plan.steps[1].argv[5]);
    try std.testing.expectEqualStrings("all-linear", plan.steps[1].argv[6]);
}

test "gemma4 lora recipe passes explicit adapter knobs" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gemma4", .family = "gemma4" },
        .dataset = .{ .path = "/data/train.jsonl" },
        .adapter = .{
            .target_modules = &.{ "q_proj", "v_proj" },
            .init_lora_weights = "default",
            .use_dora = true,
        },
        .artifacts = .{ .root = "/tmp/out" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try std.testing.expectEqualStrings("--target-modules", plan.steps[1].argv[5]);
    try std.testing.expectEqualStrings("q_proj,v_proj", plan.steps[1].argv[6]);
    try std.testing.expectEqualStrings("--use-dora", plan.steps[1].argv[7]);
    try std.testing.expectEqualStrings("--init-lora-weights", plan.steps[1].argv[8]);
    try std.testing.expectEqualStrings("default", plan.steps[1].argv[9]);
}

test "gemma4 lora recipe rejects conflicting target selectors" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gemma4", .family = "gemma4" },
        .dataset = .{ .path = "/data/train.jsonl" },
        .adapter = .{
            .target_preset = "all-linear",
            .target_modules = &.{"q_proj"},
        },
        .artifacts = .{ .root = "/tmp/out" },
    };
    try std.testing.expectError(error.ConflictingLoRATargetSelection, buildPlan(std.heap.page_allocator, recipe));
}

test "qwen adapter target presets map to supported module sets" {
    const all_linear = try adapterTargetModulesForQwen(.{ .target_preset = "all-linear" }, qwen2_real_autodiff.default_lora_target_modules[0..]);
    try std.testing.expectEqual(qwen2_real_autodiff.default_lora_target_modules.len, all_linear.len);
    const attention = try adapterTargetModulesForQwen(.{ .target_preset = "attention-only" }, qwen2_real_autodiff.default_lora_target_modules[0..]);
    try std.testing.expectEqualStrings("q_proj", attention[0]);
    try std.testing.expectEqualStrings("o_proj", attention[3]);
    try std.testing.expectError(error.ConflictingLoRATargetSelection, adapterTargetModulesForQwen(.{ .target_preset = "all-linear", .target_modules = &.{"q_proj"} }, qwen2_real_autodiff.default_lora_target_modules[0..]));
    try std.testing.expectError(error.UnsupportedLoRATargetPreset, adapterTargetModulesForQwen(.{ .target_preset = "moe-experts" }, qwen2_real_autodiff.default_lora_target_modules[0..]));
}

test "generic bootstrap families reject unsupported target preset" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gliner2", .family = "gliner2" },
        .dataset = .{
            .train_path = "/data/gliner-train.jsonl",
            .labels = "person,organization",
        },
        .adapter = .{ .target_preset = "all-linear" },
        .artifacts = .{ .root = "/tmp/gliner-run" },
    };
    try std.testing.expectError(error.UnsupportedLoRATargetPreset, buildPlan(std.heap.page_allocator, recipe));
}

test "gliner2 lora recipe keeps distinct train and eval caches" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/gliner2", .family = "gliner2" },
        .dataset = .{
            .train_path = "/data/gliner-train.jsonl",
            .eval_path = "/data/gliner-eval.jsonl",
            .labels = "person,organization",
            .max_examples = 8,
            .eval_max_examples = 4,
        },
        .artifacts = .{ .root = "/tmp/gliner-run", .materialized_dir = "/tmp/gliner-merged" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try expectStepCommands(plan, &.{
        "bootstrap-gliner2-lora",
        "prepare-gliner2-top-layer-boundary-cache",
        "prepare-gliner2-top-layer-boundary-cache",
        "train-eval-gliner2-lora-bundle",
        "materialize-gliner2-lora",
    });
    try std.testing.expectEqualStrings("/tmp/gliner-run/gliner2_train_boundary_cache.json", plan.steps[3].argv[3]);
    try std.testing.expectEqualStrings("/tmp/gliner-run/gliner2_eval_boundary_cache.json", plan.steps[3].argv[4]);
}

test "layoutlmv3 token recipe emits train eval positional paths" {
    const recipe = Recipe{
        .recipe = "qlora-sft",
        .model = .{ .path = "/models/layoutlmv3", .family = "layoutlmv3" },
        .dataset = .{
            .train_path = "/data/layout-train.jsonl",
            .eval_path = "/data/layout-eval.jsonl",
            .format = "token",
            .max_examples = 6,
            .eval_max_examples = 3,
        },
        .artifacts = .{ .root = "/tmp/layout-run", .materialized_dir = "/tmp/layout-merged" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try expectStepCommands(plan, &.{
        "bootstrap-layoutlmv3-lora",
        "train-eval-layoutlmv3-lora-token",
        "materialize-layoutlmv3-checkpoint",
    });
    try std.testing.expectEqualStrings("/data/layout-train.jsonl", plan.steps[1].argv[3]);
    try std.testing.expectEqualStrings("/data/layout-eval.jsonl", plan.steps[1].argv[4]);
}

test "reranker recipe prepares train and eval pooled caches" {
    const recipe = Recipe{
        .recipe = "reranker",
        .model = .{ .path = "/models/reranker", .family = "reranker" },
        .dataset = .{
            .train_path = "/data/rerank-train.jsonl",
            .eval_path = "/data/rerank-eval.jsonl",
            .max_examples = 10,
            .eval_max_examples = 5,
        },
        .artifacts = .{ .root = "/tmp/rerank-run", .materialized_dir = "/tmp/rerank-materialized" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try expectStepCommands(plan, &.{
        "prepare-reranker-pooled-cache",
        "prepare-reranker-pooled-cache",
        "train-eval-reranker-head-cached",
        "materialize-reranker-head",
    });
    try std.testing.expectEqualStrings("/tmp/rerank-run/reranker_train_pooled_cache.json", plan.steps[2].argv[2]);
    try std.testing.expectEqualStrings("/tmp/rerank-run/reranker_eval_pooled_cache.json", plan.steps[2].argv[3]);
}

test "reranker lora recipe requires and routes head input" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/reranker", .family = "reranker" },
        .dataset = .{
            .train_path = "/data/rerank-train.jsonl",
            .eval_path = "/data/rerank-eval.jsonl",
        },
        .artifacts = .{
            .root = "/tmp/rerank-lora-run",
            .report_path = "/tmp/rerank-head",
            .materialized_dir = "/tmp/rerank-lora-merged",
        },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try expectStepCommands(plan, &.{
        "bootstrap-reranker-lora",
        "prepare-reranker-top-layer-cache",
        "prepare-reranker-top-layer-cache",
        "train-eval-reranker-lora-top-layer-cached-surrogate",
        "materialize-reranker-lora",
    });
    try std.testing.expectEqualStrings("/tmp/rerank-head", plan.steps[3].argv[3]);
}

test "reranker lora recipe routes base model override before module flags" {
    const recipe = Recipe{
        .recipe = "lora-sft",
        .model = .{ .path = "/models/reranker", .family = "reranker" },
        .dataset = .{ .train_path = "/data/rerank-train.jsonl" },
        .adapter = .{
            .base_model_name_or_path = "BAAI/bge-reranker-base",
            .target_modules = &.{"query"},
        },
        .artifacts = .{
            .root = "/tmp/rerank-lora-run",
            .report_path = "/tmp/rerank-head",
        },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try std.testing.expectEqualStrings("bootstrap-reranker-lora", plan.steps[0].argv[0]);
    try std.testing.expectEqualStrings("16", plan.steps[0].argv[3]);
    try std.testing.expectEqualStrings("32", plan.steps[0].argv[4]);
    try std.testing.expectEqualStrings("BAAI/bge-reranker-base", plan.steps[0].argv[5]);
    try std.testing.expectEqualStrings("--target-modules", plan.steps[0].argv[6]);
    try std.testing.expectEqualStrings("query", plan.steps[0].argv[7]);
}

test "vlm retrieval routes colqwen2 prepared inputs" {
    const recipe = Recipe{
        .recipe = "vlm-retrieval",
        .model = .{ .path = "/models/colqwen2", .family = "colqwen2" },
        .dataset = .{
            .path = "/data/colqwen-root",
            .train_path = "/data/colqwen-examples.jsonl",
            .max_examples = 7,
        },
        .artifacts = .{ .root = "/tmp/colqwen-run" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    try expectStepCommands(plan, &.{
        "prepare-colqwen2-inputs",
        "bootstrap-colqwen2-lora",
        "train-eval-colqwen2-lora-bundle",
    });
    try std.testing.expectEqualStrings("/data/colqwen-root", plan.steps[0].argv[2]);
    try std.testing.expectEqualStrings("/data/colqwen-examples.jsonl", plan.steps[0].argv[3]);
}

test "sft dpo grpo recipes build runnable plans" {
    const base = Recipe{
        .model = .{ .path = "/models/gemma4", .family = "gemma4" },
        .dataset = .{ .path = "/data/train.jsonl" },
    };
    var sft = base;
    sft.recipe = "sft";
    const sft_plan = try buildPlan(std.heap.page_allocator, sft);
    defer freePlan(std.heap.page_allocator, sft_plan);
    try std.testing.expectEqualStrings("prepare-gemma4-lora-inputs", sft_plan.steps[0].argv[0]);

    var dpo = base;
    dpo.recipe = "dpo";
    const dpo_plan = try buildPlan(std.heap.page_allocator, dpo);
    defer freePlan(std.heap.page_allocator, dpo_plan);
    try std.testing.expectEqual(StepKind.direct_dpo, dpo_plan.steps[0].kind);

    var grpo_recipe = base;
    grpo_recipe.recipe = "grpo";
    const grpo_plan = try buildPlan(std.heap.page_allocator, grpo_recipe);
    defer freePlan(std.heap.page_allocator, grpo_plan);
    try std.testing.expectEqual(StepKind.direct_grpo, grpo_plan.steps[0].kind);
}

test "run manifest captures recipe plan status" {
    const recipe = Recipe{
        .recipe = "reranker",
        .model = .{ .path = "/models/reranker", .family = "reranker" },
        .dataset = .{ .path = "/data/train.jsonl" },
        .artifacts = .{ .root = "/tmp/manifest-test" },
    };
    const plan = try buildPlan(std.heap.page_allocator, recipe);
    defer freePlan(std.heap.page_allocator, plan);
    const steps = try initStepManifests(std.heap.page_allocator, plan);
    defer std.heap.page_allocator.free(steps);
    steps[0].status = .running;
    const rendered = try std.json.Stringify.valueAlloc(std.heap.page_allocator, RunManifest{
        .status = .running,
        .recipe = recipe,
        .artifact_root = recipe.artifacts.root,
        .steps = steps,
    }, .{ .whitespace = .indent_2 });
    defer std.heap.page_allocator.free(rendered);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "antfly_inference_finetune_recipe_run/v1") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "prepare-reranker-pooled-cache") != null);
    try std.testing.expect(std.mem.indexOf(u8, rendered, "running") != null);
}

test "fast smoke resolves checked-in testdata from current package cwd" {
    const allocator = std.testing.allocator;
    const path = try resolveCwdPath(allocator, std.testing.io, "pkg/inference/testdata/recipe_gemma4_lora.json");
    defer allocator.free(path);
    try std.testing.expect(cwdPathExists(std.testing.io, path));
}

test "direct command adapter registry covers reranker family steps" {
    try std.testing.expect(isDirectCommandAdapter("prepare-gemma4-lora-inputs"));
    try std.testing.expect(isDirectCommandAdapter("bootstrap-gemma4-lora"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-gemma4-lora-bundle"));
    try std.testing.expect(isDirectCommandAdapter("bootstrap-gliner2-lora"));
    try std.testing.expect(isDirectCommandAdapter("prepare-gliner2-top-layer-boundary-cache"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-gliner2-lora-bundle"));
    try std.testing.expect(isDirectCommandAdapter("materialize-gliner2-lora"));
    try std.testing.expect(isDirectCommandAdapter("bootstrap-layoutlmv3-lora"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-layoutlmv3-lora-sequence"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-layoutlmv3-lora-token"));
    try std.testing.expect(isDirectCommandAdapter("materialize-layoutlmv3-checkpoint"));
    try std.testing.expect(isDirectCommandAdapter("bootstrap-reranker-lora"));
    try std.testing.expect(isDirectCommandAdapter("prepare-reranker-top-layer-cache"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-reranker-lora-top-layer-cached-surrogate"));
    try std.testing.expect(isDirectCommandAdapter("materialize-reranker-lora"));
    try std.testing.expect(isDirectCommandAdapter("prepare-colqwen2-inputs"));
    try std.testing.expect(isDirectCommandAdapter("bootstrap-colqwen2-lora"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-colqwen2-lora-bundle"));
    try std.testing.expect(isDirectCommandAdapter("prepare-reranker-pooled-cache"));
    try std.testing.expect(isDirectCommandAdapter("train-eval-reranker-head-cached"));
    try std.testing.expect(isDirectCommandAdapter("materialize-reranker-head"));
}

test "text reward modes score as expected" {
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), scoreTextReward(.exact_match, "yes", "yes"), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), scoreTextReward(.exact_match, "yes indeed", "yes"), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), scoreTextReward(.exact_match_ci, "Yes", "yes"), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), scoreTextReward(.exact_match_ci, "yes indeed", "yes"), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), scoreTextReward(.prefix_match, "yes indeed", "yes"), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), scoreTextReward(.prefix_match, "indeed yes", "yes"), 1e-6);
}

test "synthetic gliner2 smoke assets tokenize within vocab bounds" {
    const allocator = std.testing.allocator;
    const root = "/tmp/antfly_inference_recipe_gliner2_smoke_assets_test";
    compat.cwd().deleteTree(compat.io(), root) catch {};

    const assets = try writeSyntheticGliner2SmokeAssets(allocator, std.testing.io, root);
    defer {
        allocator.free(assets.model_dir);
        allocator.free(assets.train_path);
        allocator.free(assets.eval_path);
        compat.cwd().deleteTree(compat.io(), root) catch {};
    }

    var loaded = try gliner2_data.loadExamples(allocator, assets.train_path, null);
    defer loaded.deinit();
    var tokenizer = try gliner2_data.Tokenizer.initGLiNER2HF(allocator, assets.model_dir);
    defer tokenizer.deinit(allocator);
    var workspace = try gliner2_data.ReusableBatch.init(allocator, 1, 16, 8, 3);
    defer workspace.deinit();

    const labels = [_][]const u8{ "person", "organization", "location" };
    var batch = try gliner2_data.buildSimpleBatchInto(&workspace, &tokenizer, loaded.examples[0..1], labels[0..], 8);
    defer batch.deinit();
    for (batch.input_ids, batch.attention_mask) |token_id, mask| {
        if (mask == 0) continue;
        try std.testing.expect(token_id >= 0);
        try std.testing.expect(@as(usize, @intCast(token_id)) < 8192);
    }
}

fn expectStepCommands(plan: Plan, expected: []const []const u8) !void {
    try std.testing.expectEqual(expected.len, plan.steps.len);
    for (expected, 0..) |command, i| {
        try std.testing.expectEqualStrings(command, plan.steps[i].argv[0]);
    }
}
