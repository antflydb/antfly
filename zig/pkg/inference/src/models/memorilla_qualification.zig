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

//! Versioned, opt-in production identities. These pins freeze the exact pair
//! measured by the F32 conformance gates; names alone never grant admission.
const std = @import("std");
const file = @import("../util/c_file.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const catalog = @import("../registry/memorilla_catalog.zig");
const A = std.mem.Allocator;
const memory = @import("../runtime/root.zig").tier.memory;
/// All unpinned alternate routes and optional executable metadata are fenced,
/// including their absence. Entries also pinned by a role are permitted there.
pub const optional_paths = [_][]const u8{
    "added_tokens.json",                   "special_tokens_map.json",        "adapter_config.json",                  "adapter_model.safetensors",         "adapter_model.bin",
    "model.safetensors",                   "pytorch_model.safetensors",      "pytorch_model.safetensors.index.json", "pytorch_model.bin",                 "model.gguf",
    "antfly_inference_bundle.json",        "antfly_inference_variants.json", "encoder_config/config.json",           "gliner_config.json",                "clip_config.json",
    "1_SpladePooling/config.json",         "modules.json",                   "1_Pooling/config.json",                "config_sentence_transformers.json", "processor_config.json",
    "preprocessor_config.json",            "vocab.txt",                      "tokenizer.model",                      "sentencepiece.bpe.model",           "text_model.onnx",
    "text_model_f16.onnx",                 "text_model_i8.onnx",             "model.onnx",                           "model_f16.onnx",                    "model_i8.onnx",
    "model_i8-st.onnx",                    "model_i4.onnx",                  "model_quantized.onnx",                 "decoder_model_merged.onnx",         "decoder_model_merged_fp16.onnx",
    "decoder_model_merged_quantized.onnx", "decoder_model_merged_q4.onnx",   "decoder_model_merged_q4f16.onnx",      "encoder.onnx",
};
/// Python `str.isspace` code points (the `str.strip` default set).
pub fn pythonWhitespace(cp: u21) bool {
    return switch (cp) {
        0x09...0x0d, 0x1c...0x20, 0x85, 0xa0, 0x1680, 0x2000...0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000 => true,
        else => false,
    };
}

/// Exact default Python str.strip whitespace policy, without copying. The
/// decoder must consume this same canonical question verbatim.
pub fn canonicalQuestion(text: []const u8) ![]const u8 {
    const utf8 = try std.unicode.Utf8View.init(text);
    var it = utf8.iterator();
    var offset: usize = 0;
    var first: ?usize = null;
    var last: usize = 0;
    while (it.nextCodepoint()) |cp| {
        const n = try std.unicode.utf8CodepointSequenceLength(cp);
        if (!pythonWhitespace(cp)) {
            if (first == null) first = offset;
            last = offset + n;
        }
        offset += n;
    }
    const canonical = if (first) |start| text[start..last] else text[text.len..];
    if (canonical.len == 0) return error.EmptyMemorillaQuestion;
    if (std.mem.indexOf(u8, canonical, "<|memory|>") != null) return error.InvalidMemorillaQuestion;
    return canonical;
}

pub const question_recipe = "python-strip-unicode-v1;no-empty;no-memory-token";

/// Apply only to the versioned Memorilla profiles. Ordinary CUDA models keep
/// their operator-selected diagnostic and reduced-precision routes.
pub const forbidden_cuda_flags = [_][*:0]const u8{
    "ANTFLY_CUDA_ALLOW_HOST_ATTENTION_FALLBACK",
    "ANTFLY_CUDA_FORCE_HOST_GQA_ATTENTION",
    "ANTFLY_CUDA_CROSS_BACKEND_COPY_SYNC",
    "ANTFLY_CUDA_ALLOW_PLANNED_FALLBACK",
    "ANTFLY_CUDA_FORCE_EVAL_SYNC",
    "ANTFLY_GEMMA4_MTP_PROFILE_SYNC",
    "ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR",
    "ANTFLY_INFERENCE_CUDA_FUSED_GATE_UP_BF16",
    "ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC",
    "ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS",
};

/// Research-only opt-in, read once per check: with
/// `ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES=1` the rank-2 decoder-layer
/// matrices (`model.layers.*.weight`) of both Memorilla models and the
/// decoder's LM head stay resident in their BF16 source precision and run as
/// BF16 cuBLASLt GEMMs with F32 accumulation. Multi-row gated SiLU FFNs keep
/// BF16 activations between their projections (as the PyTorch reference
/// does); embeddings, norms, the KV cache and the residual stream stay F32. The execution identity changes with
/// it, so no vector or pin of the qualified F32 identity can match a
/// BF16-matrix producer. This is a latency experiment, not a qualification.
pub const research_bf16_matrices_env = "ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES";

pub fn researchBf16Matrices() bool {
    if (@import("builtin").is_test) return false;
    return @import("antfly_platform").env.getenvBool(research_bf16_matrices_env);
}

/// Research-only opt-in (`ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL=1`): dense
/// multi-row F32 attention (the encoder passes and the decoder prefill) runs
/// the cooperative per-(row, head) kernel with an online softmax instead of
/// the scalar kernel that recomputes Q.K once per output dimension. Same F32
/// precision, different summation order; it also changes the identity.
pub const research_fast_prefill_env = "ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL";

pub fn researchFastPrefill() bool {
    if (@import("builtin").is_test) return false;
    return @import("antfly_platform").env.getenvBool(research_fast_prefill_env);
}

/// Weights the BF16-matrix research mode keeps in BF16: rank-2 decoder-layer
/// matrices (`model.layers.*` in the decoder, `layers.*` in the encoder) and
/// the decoder's untied LM head.
pub fn isResearchBf16Matrix(name: []const u8, rank: usize) bool {
    if (rank == 2 and std.mem.eql(u8, name, "lm_head.weight")) return true;
    const layer = std.mem.startsWith(u8, name, "model.layers.") or std.mem.startsWith(u8, name, "layers.");
    return rank == 2 and layer and std.mem.endsWith(u8, name, ".weight");
}

test "Memorilla research BF16 matrices are the rank-2 layer weights of both models" {
    try std.testing.expect(isResearchBf16Matrix("model.layers.3.mlp.down_proj.weight", 2));
    try std.testing.expect(isResearchBf16Matrix("layers.0.self_attn.q_proj.weight", 2));
    try std.testing.expect(!isResearchBf16Matrix("layers.0.input_layernorm.weight", 1));
    try std.testing.expect(!isResearchBf16Matrix("model.embed_tokens.weight", 2));
    try std.testing.expect(!isResearchBf16Matrix("embed_tokens.weight", 2));
    try std.testing.expect(isResearchBf16Matrix("lm_head.weight", 2));
    try std.testing.expect(!isResearchBf16Matrix("lm_head.weight", 1));
    try std.testing.expect(!researchBf16Matrices() and !researchFastPrefill());
}

pub fn requireCudaExecutionStats(stats: anytype) !void {
    @setEvalBranchQuota(100_000);
    const bf16_matrices = researchBf16Matrices();
    inline for (@typeInfo(@TypeOf(stats)).@"struct".field_names) |name| {
        if (comptime std.mem.startsWith(u8, name, "bf16_") or std.mem.startsWith(u8, name, "f16_") or
            std.mem.eql(u8, name, "rms_norm_bf16_mirror_hits") or std.mem.eql(u8, name, "fused_gate_up_bf16_hits"))
        {
            // The research mode's BF16 routes, including the RMS-norm BF16
            // mirror that feeds its BF16 projections.
            const research_route = comptime (std.mem.startsWith(u8, name, "bf16_") and !std.mem.startsWith(u8, name, "bf16_mirror")) or
                std.mem.eql(u8, name, "rms_norm_bf16_mirror_hits");
            if (@field(stats, name) != 0 and !(bf16_matrices and research_route)) return error.UnsupportedMemorillaPrecision;
        }
    }
    if (stats.host_attention_fallbacks != 0 or stats.paged_attention_host_fallbacks != 0 or
        stats.rope_host_fallbacks != 0 or stats.rope_per_item_host_fallbacks != 0 or
        stats.cross_backend_sync_fallbacks != 0) return error.UnsupportedMemorillaCudaPolicy;
}

pub fn requireCudaEnvironment() !void {
    const env = @import("antfly_platform").env;
    for (forbidden_cuda_flags) |name| if (env.getenvBool(name)) return error.UnsupportedMemorillaCudaPolicy;
    if (env.getenv("ANTFLY_INFERENCE_CUDA_GQA_DECODE_PROFILE")) |value| {
        if (!std.mem.eql(u8, value, "off")) return error.UnsupportedMemorillaCudaPolicy;
    }
    if (env.getenv("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE")) |value| {
        if (!std.mem.eql(u8, value, "automatic")) return error.UnsupportedMemorillaCudaPolicy;
    }
    if (env.getenvBool("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_FAST") or
        env.getenvBool("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED") or
        env.getenvBool("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA")) return error.UnsupportedMemorillaCudaPolicy;
    if (env.getenv("ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY") != null or
        env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") != null or
        env.getenvBool("ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY")) return error.UnsupportedMemorillaCudaPolicy;
}

test "Memorilla execution proof rejects reduced precision and every host fallback family" {
    const Stats = struct {
        kernel_launches: usize = 42,
        d2h_bytes: usize = 1024,
        bf16_cublaslt_linear_calls: usize = 0,
        bf16_future_kernel_calls: usize = 0,
        f16_cublaslt_activation_staging_calls: usize = 0,
        rms_norm_bf16_mirror_hits: usize = 0,
        fused_gate_up_bf16_hits: usize = 0,
        host_attention_fallbacks: usize = 0,
        paged_attention_host_fallbacks: usize = 0,
        rope_host_fallbacks: usize = 0,
        rope_per_item_host_fallbacks: usize = 0,
        cross_backend_sync_fallbacks: usize = 0,
    };
    try requireCudaExecutionStats(Stats{});
    inline for (@typeInfo(Stats).@"struct".field_names) |name| {
        if (comptime !std.mem.eql(u8, name, "kernel_launches") and !std.mem.eql(u8, name, "d2h_bytes")) {
            var bad = Stats{};
            @field(bad, name) = 1;
            const precision = comptime std.mem.startsWith(u8, name, "bf16_") or std.mem.startsWith(u8, name, "f16_") or
                std.mem.eql(u8, name, "rms_norm_bf16_mirror_hits") or std.mem.eql(u8, name, "fused_gate_up_bf16_hits");
            try std.testing.expectError(if (precision) error.UnsupportedMemorillaPrecision else error.UnsupportedMemorillaCudaPolicy, requireCudaExecutionStats(bad));
        }
    }
}

/// `qwen3_encoder_lora_research_f32_v1` is the research encoder: the released
/// F32 shards with a LoRA merged offline. It is never promoted to qualified; it
/// is a second, explicitly configured encoder kind failing closed on its own
/// in-directory lock (`research_lock_file`).
pub const Profile = enum { qwen3_encoder_f32_v1, qwen3_encoder_lora_research_f32_v1, qwen3_decoder_f32_kv_f32_v1 };
pub const Role = enum { encoder, decoder };
pub const lock_bytes = @embedFile("memorilla_v1_base_models.json");
pub const research_lock_file = "memorilla_lock.json";
pub const research_lock_schema = "antfly.memorilla.research-encoder-lock.v1";
pub const research_lock_max_bytes: usize = 1024 * 1024;
pub const research_receipt_file = ".antfly-download-complete.json";
pub const research_merge_receipt_file = "merge.json";
pub const max_text_tokens: usize = 256;
pub const max_documents: usize = 64;
pub const width: usize = 2560;
pub const cuda_kernel_workspace_bytes = @import("memorilla_constants.zig").cuda_kernel_workspace_bytes;
pub const encoder_weight_bytes: usize = 16_087_097_344;
// Covers widening the largest BF16 tensor, temporary upload storage and one
// bounded GEMM workspace while all mapped source pages remain admitted.
pub const decoder_weight_bytes: usize = 32_762_941_440;

pub const ResidencyPlan = struct { peak: memory.AdmissionAmounts, resident: memory.AdmissionAmounts };

/// `directory` is read only for the research tag, whose lock lives next to
/// its artifacts; the released tags keep their embedded lock.
pub fn residencyPlan(a: A, profile: Profile, directory: []const u8) !ResidencyPlan {
    const source = try sourceBytes(a, profile, directory);
    const backend = switch (profile) {
        .qwen3_encoder_f32_v1, .qwen3_encoder_lora_research_f32_v1 => encoder_weight_bytes,
        .qwen3_decoder_f32_kv_f32_v1 => decoder_weight_bytes,
    };
    // Retain a conservative charge for the complete clean source cache. The
    // loader never globally evicts pages potentially used by another owner.
    const resident = memory.AdmissionAmounts{ .host_weight_bytes = source, .backend_weight_bytes = backend, .backend_scratch_bytes = cuda_kernel_workspace_bytes };
    var peak = resident;
    peak.host_scratch_bytes = switch (profile) {
        .qwen3_encoder_f32_v1, .qwen3_encoder_lora_research_f32_v1 => 2 * 1024 * 1024 * 1024,
        .qwen3_decoder_f32_kv_f32_v1 => 3 * 1024 * 1024 * 1024,
    };
    return .{ .peak = peak, .resident = resident };
}

pub fn role(profile: Profile) Role {
    return switch (profile) {
        .qwen3_encoder_f32_v1, .qwen3_encoder_lora_research_f32_v1 => .encoder,
        .qwen3_decoder_f32_kv_f32_v1 => .decoder,
    };
}

pub fn isResearch(profile: Profile) bool {
    return profile == .qwen3_encoder_lora_research_f32_v1;
}

/// Portable across install paths, distinct from a publication/lifetime pin.
/// Changing source, recipe, arithmetic, role or measured device domain changes
/// the producer identity and therefore cannot reuse another profile's vectors.
///
/// The research identity is fixed per tag. Its per-candidate pin (the merged
/// shards, `candidate_sha256`) lives in the directory lock and is surfaced in
/// responses as `candidate_sha256`, not folded into the identity: clients
/// never produce vectors for this path (the encoder consumes texts), so the
/// identity only needs to separate the research route from the released
/// producer, and a fixed value lets the request contract pin it statically.
pub fn executionIdentity(profile: Profile) [64]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.memorilla.execution.v1\x00");
    hash.update(@tagName(profile));
    hash.update("\x00");
    hash.update(canonicalManifest(profile));
    hash.update("\x00");
    if (isResearch(profile)) hash.update("research-lock-in-directory") else hash.update(lock_bytes);
    hash.update(if (researchBf16Matrices()) "\x00bf16-source/bf16-layer-matrices/f32-activations\x00sm_121\x00" else "\x00bf16-source/f32-resident/f32-activations\x00sm_121\x00");
    if (researchFastPrefill()) hash.update("research-cooperative-f32-prefill-attention\x00");
    hash.update(question_recipe);
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}

/// Exact `model_manifest.json` bytes a directory must carry for `profile`.
pub fn canonicalManifest(profile: Profile) []const u8 {
    return switch (profile) {
        .qwen3_encoder_f32_v1 => catalog.encoder_manifest,
        .qwen3_encoder_lora_research_f32_v1 => catalog.research_encoder_manifest,
        .qwen3_decoder_f32_kv_f32_v1 => catalog.decoder_manifest,
    };
}

pub fn requireRecipe(profile: Profile, manifest: anytype) !void {
    try requireRoleRecipe(profile, manifest);
    if (manifest.nativeWeightArtifactKind() != .sharded_safetensors)
        return error.UnsupportedMemorillaArtifact;
}

pub fn requireRoleRecipe(profile: Profile, manifest: anytype) !void {
    if (manifest.memorilla_profile != profile) return error.UnsupportedMemorillaArtifact;
    switch (profile) {
        .qwen3_encoder_f32_v1, .qwen3_encoder_lora_research_f32_v1 => {
            if (manifest.model_type != .embedder or manifest.embedding_style != .qwen3_embedding or
                manifest.pooling != .last or !manifest.normalize or manifest.embedding_trailing_eos_id != 151643 or
                manifest.embedding_profile.task_contract != .profiled or
                manifest.embedding_profile.query.prefix.len != 0 or manifest.embedding_profile.document.prefix.len != 0 or
                manifest.embedding_profile.instruction_template.len != 0)
                return error.UnsupportedMemorillaArtifact;
        },
        .qwen3_decoder_f32_kv_f32_v1 => if (manifest.model_type != .generator or manifest.embedding_style != .none)
            return error.UnsupportedMemorillaArtifact,
    }
}

fn member(value: std.json.Value, key: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidMemorillaSourceLock;
    return value.object.get(key) orelse error.InvalidMemorillaSourceLock;
}

fn researchMember(value: std.json.Value, key: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidMemorillaResearchLock;
    return value.object.get(key) orelse error.InvalidMemorillaResearchLock;
}

fn pinnedPath(entries: std.json.Value, name: []const u8) bool {
    for (entries.array.items) |entry| {
        const value = entry.object.get("path") orelse continue;
        if (value == .string and std.mem.eql(u8, value.string, name)) return true;
    }
    return false;
}

fn isLowerHex64(text: []const u8) bool {
    if (text.len != 64) return false;
    for (text) |c| if (!((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'))) return false;
    return true;
}

/// Paths every research lock must pin: the executor inputs selected by the
/// manifest, and the merge receipt binding the shards to the adapter.
pub const research_lock_required_paths = [_][]const u8{
    "model.safetensors.index.json", "config.json", "tokenizer.json", "tokenizer_config.json", research_merge_receipt_file,
};

/// Validated `<directory>/memorilla_lock.json`. `files` is the pinned array
/// (`{path, size_bytes, sha256}`), owned by `parsed`.
pub const ResearchLock = struct {
    parsed: std.json.Parsed(std.json.Value),
    files: std.json.Value,
    candidate_sha256: [64]u8,

    pub fn deinit(self: *ResearchLock) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

/// Reads and validates the research lock. Every shape error is
/// `InvalidMemorillaResearchLock`; file IO errors pass through. The lock does
/// not authenticate itself: `verifyArtifacts` proves the files it pins, and the
/// generation fence and managed receipt pin the lock bytes themselves.
pub fn loadResearchLock(a: A, directory: []const u8) !ResearchLock {
    const path = try std.fs.path.join(a, &.{ directory, research_lock_file });
    defer a.free(path);
    const bytes = file.readFileMax(a, path, research_lock_max_bytes) catch |err| switch (err) {
        error.FileTooLarge => return error.InvalidMemorillaResearchLock,
        else => return err,
    };
    defer a.free(bytes);
    var parsed = std.json.parseFromSlice(std.json.Value, a, bytes, .{}) catch return error.InvalidMemorillaResearchLock;
    errdefer parsed.deinit();
    const root = parsed.value;
    const schema = try researchMember(root, "schema");
    if (schema != .string or !std.mem.eql(u8, schema.string, research_lock_schema)) return error.InvalidMemorillaResearchLock;
    const candidate = try researchMember(root, "candidate_sha256");
    if (candidate != .string or !isLowerHex64(candidate.string)) return error.InvalidMemorillaResearchLock;
    const base = try researchMember(root, "base");
    const model = try researchMember(base, "model");
    const revision = try researchMember(base, "revision");
    if (model != .string or !std.mem.eql(u8, model.string, catalog.encoder_repo)) return error.InvalidMemorillaResearchLock;
    if (revision != .string or !std.mem.eql(u8, revision.string, catalog.encoder_revision)) return error.InvalidMemorillaResearchLock;
    const files = try researchMember(root, "files");
    if (files != .array or files.array.items.len == 0) return error.InvalidMemorillaResearchLock;
    for (files.array.items, 0..) |entry, index| {
        const name = try researchMember(entry, "path");
        const size = try researchMember(entry, "size_bytes");
        const sha = try researchMember(entry, "sha256");
        if (name != .string or size != .integer or size.integer <= 0 or sha != .string or !isLowerHex64(sha.string)) return error.InvalidMemorillaResearchLock;
        const text = name.string;
        if (text.len == 0 or std.fs.path.isAbsolute(text) or std.mem.indexOf(u8, text, "\\") != null or
            std.mem.eql(u8, text, research_lock_file) or std.mem.eql(u8, text, "model_manifest.json") or std.mem.eql(u8, text, research_receipt_file))
            return error.InvalidMemorillaResearchLock;
        var components = std.mem.splitScalar(u8, text, '/');
        while (components.next()) |component| {
            if (component.len == 0 or std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) return error.InvalidMemorillaResearchLock;
        }
        for (files.array.items[0..index]) |previous| {
            const other = previous.object.get("path") orelse continue;
            if (other == .string and std.mem.eql(u8, other.string, text)) return error.InvalidMemorillaResearchLock;
        }
    }
    for (research_lock_required_paths) |required| if (!pinnedPath(files, required)) return error.InvalidMemorillaResearchLock;
    var result = ResearchLock{ .parsed = parsed, .files = files, .candidate_sha256 = undefined };
    @memcpy(&result.candidate_sha256, candidate.string);
    return result;
}

/// The pinned file list of `profile`: the embedded v1 lock for the released
/// tags, the directory lock for the research tag.
const Lock = struct {
    parsed: std.json.Parsed(std.json.Value),
    entries: std.json.Value,

    fn deinit(self: *Lock) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

fn loadLock(a: A, profile: Profile, directory: []const u8) !Lock {
    if (isResearch(profile)) {
        const research = try loadResearchLock(a, directory);
        return .{ .parsed = research.parsed, .entries = research.files };
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    errdefer parsed.deinit();
    const model = try member(try member(parsed.value, "models"), @tagName(role(profile)));
    const entries = try member(model, "files");
    if (entries != .array) return error.InvalidMemorillaSourceLock;
    return .{ .parsed = parsed, .entries = entries };
}

fn generationEntry(hash: *std.crypto.hash.sha2.Sha256, a: A, io: std.Io, directory: []const u8, name: []const u8, required: bool, directory_entry: bool, control: Control) !void {
    try control.check();
    hash.update(name);
    hash.update("\x00");
    const path = try std.fs.path.join(a, &.{ directory, name });
    defer a.free(path);
    const stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => {
            if (required) return err;
            hash.update("absent\x00");
            return;
        },
        else => return err,
    };
    if (stat.kind != (if (directory_entry) std.Io.File.Kind.directory else .file)) return error.UnsupportedMemorillaArtifact;
    hash.update("present\x00");
    hash.update(std.mem.asBytes(&stat.inode));
    hash.update(std.mem.asBytes(&stat.size));
    const mtime: i128 = stat.mtime.toNanoseconds();
    const ctime: i128 = stat.ctime.toNanoseconds();
    hash.update(std.mem.asBytes(&mtime));
    hash.update(std.mem.asBytes(&ctime));
    const final = try std.Io.Dir.cwd().statFile(io, path, .{});
    if (final.kind != stat.kind or final.inode != stat.inode or final.size != stat.size or
        final.mtime.toNanoseconds() != mtime or final.ctime.toNanoseconds() != ctime) return error.ModelArtifactsChanging;
    try control.check();
}

/// Cheap cache-generation fence for complete required and forbidden inventory.
/// Full cryptographic validation remains mandatory on every cold construction.
pub fn artifactGeneration(a: A, io: std.Io, directory: []const u8, profile: Profile, control: Control) ![32]u8 {
    var lock = try loadLock(a, profile, directory);
    defer lock.deinit();
    const entries = lock.entries;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.memorilla.artifact-generation.v1\x00");
    hash.update(&executionIdentity(profile));
    for (entries.array.items) |entry| {
        const path = try member(entry, "path");
        if (path != .string) return error.InvalidMemorillaSourceLock;
        try generationEntry(&hash, a, io, directory, path.string, true, false, control);
    }
    try generationEntry(&hash, a, io, directory, "model_manifest.json", true, false, control);
    if (isResearch(profile)) {
        // The lock is the source inventory itself; the merge receipt and the
        // managed receipt bind it to the candidate and the publication.
        try generationEntry(&hash, a, io, directory, research_lock_file, true, false, control);
        try generationEntry(&hash, a, io, directory, research_merge_receipt_file, true, false, control);
        try generationEntry(&hash, a, io, directory, research_receipt_file, false, false, control);
    }
    for (optional_paths) |path| {
        if (!pinnedPath(entries, path)) try generationEntry(&hash, a, io, directory, path, false, false, control);
    }
    // Directory ctime fences an optional file added and removed between two
    // snapshots, even when its final state is absent again.
    for ([_][]const u8{ ".", "1_Pooling", "encoder_config", "onnx" }) |name| {
        try generationEntry(&hash, a, io, directory, name, std.mem.eql(u8, name, "."), true, control);
    }
    return hash.finalResult();
}

pub fn sourceBytes(a: A, profile: Profile, directory: []const u8) !usize {
    var lock = try loadLock(a, profile, directory);
    defer lock.deinit();
    var total: usize = 0;
    for (lock.entries.array.items) |entry| {
        const n = try member(entry, "size_bytes");
        if (n != .integer or n.integer <= 0) return error.InvalidMemorillaSourceLock;
        total = try std.math.add(usize, total, @intCast(n.integer));
    }
    return total;
}

/// Hash the same open mapping in bounded chunks, checking cancellation between
/// them. Managed publication verification fences substitution before execution.
pub fn verifyArtifacts(a: A, directory: []const u8, profile: Profile, control: Control) !void {
    try control.check();
    var lock = try loadLock(a, profile, directory);
    defer lock.deinit();
    for (lock.entries.array.items) |entry| {
        try control.check();
        const name = try member(entry, "path");
        const size = try member(entry, "size_bytes");
        if (name != .string or size != .integer or size.integer <= 0) return error.InvalidMemorillaSourceLock;
        const path = try std.fs.path.join(a, &.{ directory, name.string });
        defer a.free(path);
        var mapped = try file.MmapRegion.initLimited(a, path, @intCast(size.integer));
        defer mapped.deinit();
        if (mapped.data.len != size.integer) return error.UnsupportedMemorillaArtifact;
        // Do not evict checkpoint pages that another workload may be using.
        mapped.discard_on_deinit = false;
        if (entry.object.get("sha256")) |expected| {
            if (expected != .string) return error.InvalidMemorillaSourceLock;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            var offset: usize = 0;
            while (offset < mapped.data.len) {
                try control.check();
                const end = @min(mapped.data.len, offset + 1024 * 1024);
                hash.update(mapped.data[offset..end]);
                offset = end;
            }
            if (!std.mem.eql(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower), expected.string))
                return error.UnsupportedMemorillaArtifact;
        } else {
            // Only the embedded v1 lock carries git blob pins; the research
            // lock loader already required a sha256 on every entry.
            const expected = try member(entry, "git_blob_sha1");
            if (expected != .string) return error.InvalidMemorillaSourceLock;
            var hash = std.crypto.hash.Sha1.init(.{});
            var buf: [64]u8 = undefined;
            hash.update(try std.fmt.bufPrint(&buf, "blob {d}\x00", .{mapped.data.len}));
            hash.update(mapped.data);
            if (!std.mem.eql(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower), expected.string))
                return error.UnsupportedMemorillaArtifact;
        }
    }
    const manifest_path = try std.fs.path.join(a, &.{ directory, "model_manifest.json" });
    defer a.free(manifest_path);
    const bytes = try file.readFileMax(a, manifest_path, 4096);
    defer a.free(bytes);
    if (!std.mem.eql(u8, bytes, canonicalManifest(profile))) return error.UnsupportedMemorillaArtifact;
    try control.check();
}

pub fn verifySelectedPaths(a: A, io: std.Io, directory: []const u8, manifest: anytype) !void {
    inline for (.{
        .{ "safetensors_index_path", "model.safetensors.index.json" },
        .{ "config_path", "config.json" },
        .{ "tokenizer_json_path", "tokenizer.json" },
        .{ "tokenizer_config_path", "tokenizer_config.json" },
        .{ "model_manifest_path", "model_manifest.json" },
    }) |pair| {
        const selected = @field(manifest, pair[0]) orelse return error.UnsupportedMemorillaArtifact;
        const expected = try std.fs.path.join(a, &.{ directory, pair[1] });
        defer a.free(expected);
        const actual_real = try std.Io.Dir.cwd().realPathFileAlloc(io, selected, a);
        defer a.free(actual_real);
        const expected_real = try std.Io.Dir.cwd().realPathFileAlloc(io, expected, a);
        defer a.free(expected_real);
        if (!std.mem.eql(u8, actual_real, expected_real)) return error.UnsupportedMemorillaArtifact;
    }
    inline for (.{ "onnx_path", "safetensors_path", "gguf_path", "gguf_projector_path", "special_tokens_map_path", "preprocessor_config_path", "processor_config_path", "vocab_txt_path", "text_projection_path", "visual_projection_path", "visual_model_path", "audio_model_path", "audio_projection_path" }) |field| {
        if (@field(manifest, field) != null) return error.UnsupportedMemorillaArtifact;
    }
    const profile = manifest.memorilla_profile orelse return error.UnsupportedMemorillaArtifact;
    var lock = try loadLock(a, profile, directory);
    defer lock.deinit();
    for (optional_paths) |name| {
        if (pinnedPath(lock.entries, name)) continue;
        const path = try std.fs.path.join(a, &.{ directory, name });
        defer a.free(path);
        _ = std.Io.Dir.cwd().statFile(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => continue,
            else => return err,
        };
        return error.UnsupportedMemorillaArtifact;
    }
}

test "Memorilla qualified profiles retain distinct source roles and checked source bytes" {
    try std.testing.expectEqual(Role.encoder, role(.qwen3_encoder_f32_v1));
    try std.testing.expectEqual(Role.encoder, role(.qwen3_encoder_lora_research_f32_v1));
    try std.testing.expectEqual(Role.decoder, role(.qwen3_decoder_f32_kv_f32_v1));
    try std.testing.expect(try sourceBytes(std.testing.allocator, .qwen3_encoder_f32_v1, "/unused") > 8_000_000_000);
    try std.testing.expect(try sourceBytes(std.testing.allocator, .qwen3_decoder_f32_kv_f32_v1, "/unused") > 16_000_000_000);
    const encoder = try residencyPlan(std.testing.allocator, .qwen3_encoder_f32_v1, "/unused");
    const decoder = try residencyPlan(std.testing.allocator, .qwen3_decoder_f32_kv_f32_v1, "/unused");
    try std.testing.expect(encoder.peak.host_scratch_bytes > 151665 * @as(usize, 2560) * 4);
    try std.testing.expect(decoder.peak.host_scratch_bytes > 151936 * @as(usize, 4096) * 4);
    try std.testing.expectEqual(try sourceBytes(std.testing.allocator, .qwen3_encoder_f32_v1, "/unused"), encoder.resident.host_weight_bytes);
    // The research tag reads its lock from the directory and fails closed without one.
    try std.testing.expectError(error.FileNotFound, sourceBytes(std.testing.allocator, .qwen3_encoder_lora_research_f32_v1, "/nonexistent-memorilla-research-encoder"));
    try std.testing.expectError(error.FileNotFound, residencyPlan(std.testing.allocator, .qwen3_encoder_lora_research_f32_v1, "/nonexistent-memorilla-research-encoder"));
}

test "Memorilla execution identities are fixed per tag and separate the research route" {
    const released = executionIdentity(.qwen3_encoder_f32_v1);
    const research = executionIdentity(.qwen3_encoder_lora_research_f32_v1);
    const decoder = executionIdentity(.qwen3_decoder_f32_kv_f32_v1);
    try std.testing.expect(!std.mem.eql(u8, &released, &research));
    try std.testing.expect(!std.mem.eql(u8, &research, &decoder));
    try std.testing.expectEqualSlices(u8, &research, &executionIdentity(.qwen3_encoder_lora_research_f32_v1));
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.memorilla.execution.v1\x00qwen3_encoder_lora_research_f32_v1\x00");
    hash.update(catalog.research_encoder_manifest);
    hash.update("\x00research-lock-in-directory\x00bf16-source/f32-resident/f32-activations\x00sm_121\x00");
    hash.update(question_recipe);
    try std.testing.expectEqualSlices(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower), &research);
    try std.testing.expectEqualStrings(catalog.research_encoder_manifest, canonicalManifest(.qwen3_encoder_lora_research_f32_v1));
}

test "Memorilla artifact generation tracks optional addition removal and cancellation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try temp.dir.createDirPath(io, "model/1_Pooling");
    const parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    defer parsed.deinit();
    const entries = try member(try member(try member(parsed.value, "models"), "encoder"), "files");
    for (entries.array.items) |entry| {
        const path = try std.fs.path.join(a, &.{ "model", (try member(entry, "path")).string });
        defer a.free(path);
        try temp.dir.writeFile(io, .{ .sub_path = path, .data = "not-a-qualified-artifact" });
    }
    try temp.dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = catalog.encoder_manifest });
    const directory = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "model" });
    defer a.free(directory);
    // Metadata snapshots are not cold cryptographic admission proofs.
    const before = try artifactGeneration(a, io, directory, .qwen3_encoder_f32_v1, .{});
    try temp.dir.writeFile(io, .{ .sub_path = "model/added_tokens.json", .data = "{}" });
    const added = try artifactGeneration(a, io, directory, .qwen3_encoder_f32_v1, .{});
    try std.testing.expect(!std.mem.eql(u8, &before, &added));
    try temp.dir.deleteFile(io, "model/added_tokens.json");
    const removed = try artifactGeneration(a, io, directory, .qwen3_encoder_f32_v1, .{});
    try std.testing.expect(!std.mem.eql(u8, &before, &removed));
    const Cancel = struct {
        fn check(_: ?*anyopaque) anyerror!void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, artifactGeneration(a, io, directory, .qwen3_encoder_f32_v1, .{ .check_fn = Cancel.check }));
    try std.testing.expectError(error.Cancelled, verifyArtifacts(a, directory, .qwen3_encoder_f32_v1, .{ .check_fn = Cancel.check }));
}

const ResearchFixture = struct {
    const File = struct { path: []const u8, data: []const u8 };
    // Required executor inputs plus one shard and the three sidecars the v1
    // lock also pins (permitted optional paths).
    const files = [_]File{
        .{ .path = "model.safetensors.index.json", .data = "{\"weight_map\":{}}" },
        .{ .path = "config.json", .data = "{\"model_type\":\"qwen3\"}" },
        .{ .path = "tokenizer.json", .data = "{\"version\":\"1.0\"}" },
        .{ .path = "tokenizer_config.json", .data = "{}" },
        .{ .path = "merge.json", .data = "{\"candidate_sha256\":\"ab\"}" },
        .{ .path = "model-00001-of-00001.safetensors", .data = "merged-f32-shard-bytes" },
        .{ .path = "1_Pooling/config.json", .data = "{\"pooling_mode_lasttoken\":true}" },
        .{ .path = "modules.json", .data = "[]" },
        .{ .path = "config_sentence_transformers.json", .data = "{}" },
    };
    const candidate = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn lockJson(a: A, size_delta: i64, schema: []const u8, revision: []const u8) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(a);
        defer out.deinit();
        const w = &out.writer;
        try w.print("{{\"schema\":\"{s}\",\"candidate_sha256\":\"{s}\",\"base\":{{\"model\":\"{s}\",\"revision\":\"{s}\"}},\"files\":[", .{ schema, candidate, catalog.encoder_repo, revision });
        for (files, 0..) |entry, index| {
            var raw: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(entry.data, &raw, .{});
            const size: i64 = @as(i64, @intCast(entry.data.len)) + (if (index == 0) size_delta else 0);
            try w.print("{s}{{\"path\":\"{s}\",\"size_bytes\":{d},\"sha256\":\"{s}\"}}", .{ if (index == 0) "" else ",", entry.path, size, &std.fmt.bytesToHex(raw, .lower) });
        }
        try w.writeAll("]}\n");
        return a.dupe(u8, out.written());
    }

    fn write(a: A, io: std.Io, dir: std.Io.Dir, lock: []const u8) !void {
        try dir.createDirPath(io, "model/1_Pooling");
        for (files) |entry| {
            const path = try std.fs.path.join(a, &.{ "model", entry.path });
            defer a.free(path);
            try dir.writeFile(io, .{ .sub_path = path, .data = entry.data });
        }
        try dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = catalog.research_encoder_manifest });
        try dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = lock });
    }
};

test "Memorilla research lock pins the merged directory and fails closed on every mismatch" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const good = try ResearchFixture.lockJson(a, 0, research_lock_schema, catalog.encoder_revision);
    defer a.free(good);
    try ResearchFixture.write(a, io, temp.dir, good);
    const directory = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "model" });
    defer a.free(directory);
    const profile: Profile = .qwen3_encoder_lora_research_f32_v1;

    var lock = try loadResearchLock(a, directory);
    try std.testing.expectEqualStrings(ResearchFixture.candidate, &lock.candidate_sha256);
    try std.testing.expectEqual(ResearchFixture.files.len, lock.files.array.items.len);
    lock.deinit();
    var expected_bytes: usize = 0;
    for (ResearchFixture.files) |entry| expected_bytes += entry.data.len;
    try std.testing.expectEqual(expected_bytes, try sourceBytes(a, profile, directory));
    const plan = try residencyPlan(a, profile, directory);
    try std.testing.expectEqual(expected_bytes, plan.resident.host_weight_bytes);
    try std.testing.expectEqual(encoder_weight_bytes, plan.resident.backend_weight_bytes);
    try verifyArtifacts(a, directory, profile, .{});
    const generation = try artifactGeneration(a, io, directory, profile, .{});
    try std.testing.expectEqualSlices(u8, &generation, &try artifactGeneration(a, io, directory, profile, .{}));

    // The research manifest is required byte-for-byte; the v1 manifest in a
    // research directory is a different profile.
    try temp.dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = catalog.encoder_manifest });
    try std.testing.expectError(error.UnsupportedMemorillaArtifact, verifyArtifacts(a, directory, profile, .{}));
    try temp.dir.writeFile(io, .{ .sub_path = "model/model_manifest.json", .data = catalog.research_encoder_manifest });
    // A replaced shard fails the hash; the lock, the merge receipt and the
    // managed receipt all move the generation fence.
    try temp.dir.writeFile(io, .{ .sub_path = "model/model-00001-of-00001.safetensors", .data = "merged-f32-shard-BYTES" });
    try std.testing.expectError(error.UnsupportedMemorillaArtifact, verifyArtifacts(a, directory, profile, .{}));
    try std.testing.expect(!std.mem.eql(u8, &generation, &try artifactGeneration(a, io, directory, profile, .{})));
    try temp.dir.writeFile(io, .{ .sub_path = "model/model-00001-of-00001.safetensors", .data = "merged-f32-shard-bytes" });
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_receipt_file, .data = "{\"version\":1}" });
    const with_receipt = try artifactGeneration(a, io, directory, profile, .{});
    try std.testing.expect(!std.mem.eql(u8, &generation, &with_receipt));
    try temp.dir.deleteFile(io, "model/" ++ research_receipt_file);
    // Unpinned optional routes stay forbidden next to a research encoder.
    try temp.dir.writeFile(io, .{ .sub_path = "model/adapter_config.json", .data = "{}" });
    const Manifest = struct {
        safetensors_index_path: ?[]const u8,
        config_path: ?[]const u8,
        tokenizer_json_path: ?[]const u8,
        tokenizer_config_path: ?[]const u8,
        model_manifest_path: ?[]const u8,
        onnx_path: ?[]const u8 = null,
        safetensors_path: ?[]const u8 = null,
        gguf_path: ?[]const u8 = null,
        gguf_projector_path: ?[]const u8 = null,
        special_tokens_map_path: ?[]const u8 = null,
        preprocessor_config_path: ?[]const u8 = null,
        processor_config_path: ?[]const u8 = null,
        vocab_txt_path: ?[]const u8 = null,
        text_projection_path: ?[]const u8 = null,
        visual_projection_path: ?[]const u8 = null,
        visual_model_path: ?[]const u8 = null,
        audio_model_path: ?[]const u8 = null,
        audio_projection_path: ?[]const u8 = null,
        memorilla_profile: ?Profile = .qwen3_encoder_lora_research_f32_v1,
    };
    var selected: [5][]const u8 = undefined;
    inline for (.{ "model.safetensors.index.json", "config.json", "tokenizer.json", "tokenizer_config.json", "model_manifest.json" }, 0..) |name, i| selected[i] = try std.fs.path.join(a, &.{ directory, name });
    defer for (selected) |path| a.free(path);
    const manifest = Manifest{ .safetensors_index_path = selected[0], .config_path = selected[1], .tokenizer_json_path = selected[2], .tokenizer_config_path = selected[3], .model_manifest_path = selected[4] };
    try std.testing.expectError(error.UnsupportedMemorillaArtifact, verifySelectedPaths(a, io, directory, manifest));
    try temp.dir.deleteFile(io, "model/adapter_config.json");
    try verifySelectedPaths(a, io, directory, manifest);

    // Lock shape failures: wrong size, bad schema, wrong base revision,
    // missing required path, oversized lock, absent lock.
    const wrong_size = try ResearchFixture.lockJson(a, 1, research_lock_schema, catalog.encoder_revision);
    defer a.free(wrong_size);
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = wrong_size });
    try std.testing.expectError(error.UnsupportedMemorillaArtifact, verifyArtifacts(a, directory, profile, .{}));
    const bad_schema = try ResearchFixture.lockJson(a, 0, "antfly.memorilla.research-encoder-lock.v0", catalog.encoder_revision);
    defer a.free(bad_schema);
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = bad_schema });
    try std.testing.expectError(error.InvalidMemorillaResearchLock, verifyArtifacts(a, directory, profile, .{}));
    try std.testing.expectError(error.InvalidMemorillaResearchLock, artifactGeneration(a, io, directory, profile, .{}));
    try std.testing.expectError(error.InvalidMemorillaResearchLock, sourceBytes(a, profile, directory));
    try std.testing.expectError(error.InvalidMemorillaResearchLock, verifySelectedPaths(a, io, directory, manifest));
    const wrong_base = try ResearchFixture.lockJson(a, 0, research_lock_schema, "0000000000000000000000000000000000000000");
    defer a.free(wrong_base);
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = wrong_base });
    try std.testing.expectError(error.InvalidMemorillaResearchLock, loadResearchLock(a, directory));
    const missing_merge = try std.mem.replaceOwned(u8, a, good, "\"path\":\"merge.json\"", "\"path\":\"merge-receipt.json\"");
    defer a.free(missing_merge);
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = missing_merge });
    try std.testing.expectError(error.InvalidMemorillaResearchLock, loadResearchLock(a, directory));
    const self_pin = try std.mem.replaceOwned(u8, a, good, "\"path\":\"modules.json\"", "\"path\":\"" ++ research_lock_file ++ "\"");
    defer a.free(self_pin);
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = self_pin });
    try std.testing.expectError(error.InvalidMemorillaResearchLock, loadResearchLock(a, directory));
    const traversal = try std.mem.replaceOwned(u8, a, good, "\"path\":\"modules.json\"", "\"path\":\"../modules.json\"");
    defer a.free(traversal);
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = traversal });
    try std.testing.expectError(error.InvalidMemorillaResearchLock, loadResearchLock(a, directory));
    const huge = try a.alloc(u8, research_lock_max_bytes + 1);
    defer a.free(huge);
    @memset(huge, ' ');
    try temp.dir.writeFile(io, .{ .sub_path = "model/" ++ research_lock_file, .data = huge });
    try std.testing.expectError(error.InvalidMemorillaResearchLock, loadResearchLock(a, directory));
    try temp.dir.deleteFile(io, "model/" ++ research_lock_file);
    try std.testing.expectError(error.FileNotFound, loadResearchLock(a, directory));
    try std.testing.expectError(error.FileNotFound, artifactGeneration(a, io, directory, profile, .{}));
}
