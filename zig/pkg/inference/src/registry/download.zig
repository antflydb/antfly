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

// HuggingFace Hub model download.
//
// Downloads model files from huggingface.co using the Hub API.
// Supports token authentication and variant selection.
// Uses pure Zig std.Io for cross-platform file I/O (matching antfly-zig patterns).

const std = @import("std");
const httpx = @import("httpx");
const builtin = @import("builtin");
const managed_receipt = @import("managed_receipt.zig");

pub const default_max_artifact_bytes: u64 = 64 * 1024 * 1024 * 1024;
pub const default_max_model_bytes: u64 = 128 * 1024 * 1024 * 1024;

pub const HubConfig = struct {
    /// HuggingFace Hub API token (optional, for private/gated models).
    token: ?[]const u8 = null,
    /// Base URL for the Hub API.
    base_url: []const u8 = "https://huggingface.co",
    /// Maximum bytes accepted for one downloaded model artifact.
    ///
    /// Downloads stream directly to disk, so this is a disk-safety boundary
    /// rather than an in-memory response limit. Keep it high enough for large
    /// unsharded checkpoints while remaining finite for untrusted responses.
    max_artifact_bytes: u64 = default_max_artifact_bytes,
    /// Maximum aggregate bytes accepted for the selected model artifact set.
    ///
    /// This prevents a repository with many individually valid shards from
    /// exhausting the model volume.
    max_model_bytes: u64 = default_max_model_bytes,
};

pub const managed_download_in_progress_filename = managed_receipt.in_progress_filename;
pub const managed_download_plan_filename = managed_receipt.plan_filename;
pub const managed_download_complete_filename = managed_receipt.complete_filename;
pub const managed_download_lock_filename = ".antfly-download.lock";
const managed_download_staging_suffix = ".antfly-download-staging";
const managed_download_backup_suffix = ".antfly-download-backup";

const ManagedArtifactReceipt = managed_receipt.ArtifactReceipt;
const ManagedDownloadReceipt = managed_receipt.DownloadReceipt;

pub const ManagedDownloadState = enum {
    unmanaged,
    incomplete,
    complete,
};

const ResolvedArtifact = struct {
    file: HubFile,
    total_bytes: ?u64,
};

fn addKnownModelBytes(current: u64, artifact_bytes: u64, limit: u64) !u64 {
    const next = std.math.add(u64, current, artifact_bytes) catch
        return error.ModelSizeLimitExceeded;
    if (next > limit) return error.ModelSizeLimitExceeded;
    return next;
}

fn consumeModelBudget(remaining: u64, artifact_bytes: u64) !u64 {
    if (artifact_bytes > remaining) return error.ModelSizeLimitExceeded;
    return remaining - artifact_bytes;
}

pub const ProjectorSelection = union(enum) {
    auto,
    none,
    match: []const u8,
};

pub fn parseProjectorSelection(value: []const u8) ?ProjectorSelection {
    if (std.mem.eql(u8, value, "auto") or std.mem.eql(u8, value, "default")) return .auto;
    if (std.mem.eql(u8, value, "none") or
        std.mem.eql(u8, value, "off") or
        std.mem.eql(u8, value, "false"))
    {
        return .none;
    }
    if (value.len == 0) return null;
    return .{ .match = value };
}

pub const HubFile = struct {
    name: []const u8,
    size: ?u64 = null,
    sha256: ?[]const u8 = null,
};

pub const PayloadSupportSummary = struct {
    has_gguf: bool = false,
    has_onnx: bool = false,
    has_safetensors: bool = false,
    has_framework_weights: bool = false,

    pub fn hasCompatiblePayload(self: PayloadSupportSummary) bool {
        return self.has_gguf or self.has_onnx or self.has_safetensors;
    }
};

const SyntheticMetadataPlan = union(enum) {
    none,
    paddleocr: struct {
        detection_model: []const u8,
        recognition_model: []const u8,
        char_dict_file: []const u8,
    },
};

/// Files needed per model type, in priority order.
/// We always download config + tokenizer; the model file depends on variant.
const always_files = [_][]const u8{
    "config.json",
    "clip_config.json",
    "generation_config.json",
    "processor_config.json",
    "preprocessor_config.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "special_tokens_map.json",
    "tokenizer.model", // SentencePiece fallback
    "vocab.txt",
    "vocab.json",
    "merges.txt",
    "modules.json",
    "config_sentence_transformers.json",
    "1_SpladePooling/config.json",
    "added_tokens.json",
    "gliner_config.json",
    "termite_bundle.json",
    "spm.model",
    "model_manifest.json",
    "antfly_metadata.json",
    "antfly_inference_variants.json",
};

/// ONNX model file candidates, tried in order.
const onnx_candidates = [_][]const u8{
    "model.onnx",
    "onnx/model.onnx",
    "model_quantized.onnx",
    "onnx/model_quantized.onnx",
    "model_i8.onnx",
    "model_f16.onnx",
};

/// Multimodal ONNX payloads (CLIP, CLAP, CLIPCLAP).
const multimodal_onnx_candidates = [_][]const u8{
    "text_model.onnx",
    "onnx/text_model.onnx",
    "visual_model.onnx",
    "onnx/visual_model.onnx",
    "vision_model.onnx",
    "onnx/vision_model.onnx",
    "audio_model.onnx",
    "onnx/audio_model.onnx",
    "audio_projection.onnx",
    "onnx/audio_projection.onnx",
    "text_projection.onnx",
    "onnx/text_projection.onnx",
    "visual_projection.onnx",
    "onnx/visual_projection.onnx",
};

/// Split encoder/decoder ONNX payloads used by seq2seq readers such as TrOCR and Donut.
const seq2seq_onnx_candidates = [_][]const u8{
    "encoder_model.onnx",
    "onnx/encoder_model.onnx",
    "decoder_model.onnx",
    "onnx/decoder_model.onnx",
    "decoder_model_merged.onnx",
    "onnx/decoder_model_merged.onnx",
    "decoder_with_past_model.onnx",
    "onnx/decoder_with_past_model.onnx",
};

/// Decoder-only VLM ONNX payloads used by Moondream-style readers.
const decoder_only_vlm_decoder_candidates = [_][]const u8{
    "decoder_model_merged_q4f16.onnx",
    "onnx/decoder_model_merged_q4f16.onnx",
    "decoder_model_merged_q4.onnx",
    "onnx/decoder_model_merged_q4.onnx",
    "decoder_model_merged_quantized.onnx",
    "onnx/decoder_model_merged_quantized.onnx",
    "decoder_model_merged_fp16.onnx",
    "onnx/decoder_model_merged_fp16.onnx",
    "decoder_model_merged.onnx",
    "onnx/decoder_model_merged.onnx",
};

const decoder_only_vlm_embed_candidates = [_][]const u8{
    "embed_tokens_q4f16.onnx",
    "onnx/embed_tokens_q4f16.onnx",
    "embed_tokens_q4.onnx",
    "onnx/embed_tokens_q4.onnx",
    "embed_tokens_quantized.onnx",
    "onnx/embed_tokens_quantized.onnx",
    "embed_tokens_fp16.onnx",
    "onnx/embed_tokens_fp16.onnx",
    "embed_tokens.onnx",
    "onnx/embed_tokens.onnx",
};

const decoder_only_vlm_vision_candidates = [_][]const u8{
    "vision_encoder_q4f16.onnx",
    "onnx/vision_encoder_q4f16.onnx",
    "vision_encoder_q4.onnx",
    "onnx/vision_encoder_q4.onnx",
    "vision_encoder_quantized.onnx",
    "onnx/vision_encoder_quantized.onnx",
    "vision_encoder_fp16.onnx",
    "onnx/vision_encoder_fp16.onnx",
    "vision_encoder.onnx",
    "onnx/vision_encoder.onnx",
};

/// SafeTensors candidates for native backends.
const safetensors_index_candidates = [_][]const u8{
    "model.safetensors.index.json",
    "pytorch_model.safetensors.index.json",
};

const safetensors_candidates = [_][]const u8{
    "model.safetensors",
    "pytorch_model.safetensors",
};

/// Preferred GGUF quant suffixes for default pulls.
/// Bias toward smaller deployable artifacts first; larger quants should be
/// requested explicitly via `:gguf:Q...`.
const gguf_quant_preference = [_][]const u8{
    "Q4_K_S",
    "UD-Q4_K_S",
    "Q4_K",
    "Q4_K_M",
    "UD-Q4_K_M",
    "UD-Q4_K_XL",
    "Q4_0",
    "Q5_K_S",
    "Q5_K_M",
    "Q6_K",
    "Q8_0",
    "Q3_K_M",
    "Q2_K",
};

/// Preferred external multimodal projector GGUF payloads for automatic pulls.
/// Q8 is effectively lossless for projector inference while avoiding the
/// roughly 2x residency and download cost of F16/BF16. Users who need a dense
/// projector can still select it explicitly with `--projector BF16`.
const gguf_projector_preference = [_][]const u8{
    "Q8_0", "q8_0", "Q6_K", "q6_k", "Q5_K_M", "q5_k_m", "Q4_K_M", "q4_k_m", "f16", "bf16", "F16", "BF16",
};

fn basename(path: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, path, '/')) |slash| return path[slash + 1 ..];
    return path;
}

fn isGgufFile(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".gguf");
}

fn isOnnxFile(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".onnx");
}

fn isSafetensorsFile(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".safetensors") or std.mem.endsWith(u8, path, ".safetensors.index.json");
}

fn isFrameworkWeightFile(path: []const u8) bool {
    const base = basename(path);
    return std.mem.eql(u8, base, "pytorch_model.bin") or
        (std.mem.startsWith(u8, base, "pytorch_model-") and std.mem.endsWith(u8, base, ".bin")) or
        std.mem.eql(u8, base, "tf_model.h5") or
        std.mem.eql(u8, base, "flax_model.msgpack") or
        std.mem.endsWith(u8, base, ".ckpt");
}

pub fn summarizePayloadSupport(files: []const HubFile) PayloadSupportSummary {
    var summary: PayloadSupportSummary = .{};
    for (files) |file| {
        summary.has_gguf = summary.has_gguf or isGgufFile(file.name);
        summary.has_onnx = summary.has_onnx or isOnnxFile(file.name);
        summary.has_safetensors = summary.has_safetensors or isSafetensorsFile(file.name);
        summary.has_framework_weights = summary.has_framework_weights or isFrameworkWeightFile(file.name);
    }
    return summary;
}

fn appendModelRef(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    owner: []const u8,
    name: []const u8,
    variant: []const u8,
) !void {
    try out.appendSlice(alloc, owner);
    try out.append(alloc, '/');
    try out.appendSlice(alloc, name);
    if (variant.len > 0 and !std.mem.eql(u8, variant, "auto")) {
        try out.append(alloc, ':');
        try out.appendSlice(alloc, variant);
    }
}

fn isOpenAiClipRef(owner: []const u8, name: []const u8) bool {
    return std.mem.eql(u8, owner, "openai") and std.mem.startsWith(u8, name, "clip-");
}

pub fn noModelFilesAdviceAlloc(
    alloc: std.mem.Allocator,
    owner: []const u8,
    name: []const u8,
    variant: []const u8,
    files: []const HubFile,
) ![]u8 {
    const summary = summarizePayloadSupport(files);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "No compatible model files found for ");
    try appendModelRef(alloc, &out, owner, name, variant);
    try out.appendSlice(alloc, ".\n");
    try out.appendSlice(alloc, "Antfly inference pull expects GGUF, ONNX, or safetensors payloads.");
    if (summary.has_framework_weights and !summary.hasCompatiblePayload()) {
        try out.appendSlice(alloc, " This repo appears to publish framework weights only, such as PyTorch, TensorFlow, or Flax files.");
    }
    try out.append(alloc, '\n');

    if (isOpenAiClipRef(owner, name)) {
        try out.appendSlice(alloc, "For v0.2 CLIP/CLAP embeddings, use:\n");
        try out.appendSlice(alloc, "  antfly inference pull antflydb/clipclap:gguf:Q4_K\n");
        try out.appendSlice(alloc, "For an ONNX CLIP export, try:\n");
        try out.appendSlice(alloc, "  antfly inference pull Xenova/clip-vit-base-patch32:hybrid\n");
    } else {
        try out.appendSlice(alloc, "Use a repo or variant that includes GGUF, ONNX, or safetensors model files.\n");
    }

    return try out.toOwnedSlice(alloc);
}

fn isGgufProjectorFile(path: []const u8) bool {
    if (!isGgufFile(path)) return false;
    const base = basename(path);
    const ext = ".gguf";
    const stem = base[0 .. base.len - ext.len];
    return std.mem.eql(u8, stem, "mmproj") or
        std.mem.startsWith(u8, stem, "mmproj-") or
        std.mem.startsWith(u8, stem, "mmproj_") or
        std.mem.endsWith(u8, stem, "-mmproj") or
        std.mem.endsWith(u8, stem, "_mmproj");
}

fn isGgufQuantBoundary(ch: u8) bool {
    return switch (ch) {
        '-', '_', '.' => true,
        else => false,
    };
}

fn ggufQuantSuffixMatches(path: []const u8, quant: []const u8) bool {
    if (!isGgufFile(path) or quant.len == 0) return false;
    const base = basename(path);
    const ext = ".gguf";
    if (base.len <= ext.len or !std.mem.endsWith(u8, base, ext)) return false;
    const stem = base[0 .. base.len - ext.len];
    if (stem.len < quant.len) return false;

    var start: usize = 0;
    while (start + quant.len <= stem.len) : (start += 1) {
        if (!std.ascii.eqlIgnoreCase(stem[start .. start + quant.len], quant)) continue;
        const has_left_boundary = start == 0 or isGgufQuantBoundary(stem[start - 1]);
        const end = start + quant.len;
        const has_right_boundary = end == stem.len or isGgufQuantBoundary(stem[end]);
        if (has_left_boundary and has_right_boundary) return true;
    }
    return false;
}

fn isClipclapClipGgufFile(path: []const u8) bool {
    return clipclapGgufSuffix(path, "clip") != null;
}

fn isClipclapClapGgufFile(path: []const u8) bool {
    return clipclapGgufSuffix(path, "clap") != null;
}

fn clipclapGgufSuffix(path: []const u8, component: []const u8) ?[]const u8 {
    if (!isGgufFile(path)) return null;
    const base = basename(path);
    var prefix_buf: [32]u8 = undefined;
    const prefix = std.fmt.bufPrint(&prefix_buf, "clipclap-{s}", .{component}) catch return null;
    if (!std.mem.startsWith(u8, base, prefix)) return null;
    const ext = ".gguf";
    const rest = base[prefix.len..];
    if (std.mem.eql(u8, rest, ext)) return "";
    if (rest.len <= 1 + ext.len or rest[0] != '.') return null;
    if (!std.mem.endsWith(u8, rest, ext)) return null;
    return rest[1 .. rest.len - ext.len];
}

fn clipclapGgufMatchesSuffix(path: []const u8, component: []const u8, suffix: []const u8) bool {
    const actual = clipclapGgufSuffix(path, component) orelse return false;
    return std.ascii.eqlIgnoreCase(actual, suffix);
}

fn appendClipclapGgufPairForQuant(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    quant_suffix: []const u8,
) !bool {
    var clip: ?HubFile = null;
    var clap: ?HubFile = null;

    for (files) |f| {
        if (clip == null and clipclapGgufMatchesSuffix(f.name, "clip", quant_suffix)) {
            clip = f;
        } else if (clap == null and clipclapGgufMatchesSuffix(f.name, "clap", quant_suffix)) {
            clap = f;
        }
    }

    if (clip == null or clap == null) return false;
    try to_download.append(allocator, clip.?);
    try to_download.append(allocator, clap.?);
    return true;
}

fn appendFirstCompleteClipclapGgufPair(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !bool {
    for (files) |f| {
        const suffix = clipclapGgufSuffix(f.name, "clip") orelse continue;
        if (try appendClipclapGgufPairForQuant(allocator, to_download, files, suffix)) {
            return true;
        }
    }

    return false;
}

fn appendUnsuffixedClipclapGgufPair(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !bool {
    var clip: ?HubFile = null;
    var clap: ?HubFile = null;
    for (files) |f| {
        if (clip == null and isClipclapClipGgufFile(f.name)) {
            if (clipclapGgufSuffix(f.name, "clip")) |suffix| {
                if (suffix.len == 0) clip = f;
            }
        } else if (clap == null and isClipclapClapGgufFile(f.name)) {
            if (clipclapGgufSuffix(f.name, "clap")) |suffix| {
                if (suffix.len == 0) clap = f;
            }
        }
    }
    if (clip == null or clap == null) return false;
    try to_download.append(allocator, clip.?);
    try to_download.append(allocator, clap.?);
    return true;
}

/// Find the paired CLIP and CLAP GGUF files in a single-repo ClipClap layout.
fn appendBestClipclapGgufPair(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    quant_filter: ?[]const u8,
) !bool {
    if (quant_filter != null) {
        return appendClipclapGgufPairForQuant(allocator, to_download, files, quant_filter.?);
    }
    for (&gguf_quant_preference) |quant| {
        if (try appendClipclapGgufPairForQuant(allocator, to_download, files, quant)) return true;
    }
    if (try appendUnsuffixedClipclapGgufPair(allocator, to_download, files)) return true;
    return appendFirstCompleteClipclapGgufPair(allocator, to_download, files);
}

fn isGlinerEncoderGgufFile(path: []const u8) bool {
    return glinerGgufSuffix(path, "encoder") != null;
}

fn isGlinerHeadGgufFile(path: []const u8) bool {
    return glinerGgufSuffix(path, "head") != null;
}

fn glinerGgufSuffix(path: []const u8, component: []const u8) ?[]const u8 {
    if (!isGgufFile(path)) return null;
    const base = basename(path);
    if (std.mem.eql(u8, component, "encoder") and std.mem.eql(u8, base, "encoder.gguf")) return "";
    if (std.mem.eql(u8, component, "head") and std.mem.eql(u8, base, "gliner_head.gguf")) return "";

    var prefix_buf: [48]u8 = undefined;
    const prefix = std.fmt.bufPrint(&prefix_buf, "gliner2-{s}", .{component}) catch return null;
    if (!std.mem.startsWith(u8, base, prefix)) return null;
    const ext = ".gguf";
    const rest = base[prefix.len..];
    if (std.mem.eql(u8, rest, ext)) return "";
    if (rest.len <= 1 + ext.len or rest[0] != '.') return null;
    if (!std.mem.endsWith(u8, rest, ext)) return null;
    return rest[1 .. rest.len - ext.len];
}

fn glinerGgufMatchesSuffix(path: []const u8, component: []const u8, suffix: []const u8) bool {
    const actual = glinerGgufSuffix(path, component) orelse return false;
    return std.ascii.eqlIgnoreCase(actual, suffix);
}

fn appendGlinerSplitGgufBundleForQuant(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    quant_suffix: []const u8,
) !bool {
    var encoder: ?HubFile = null;
    var head: ?HubFile = null;

    for (files) |f| {
        if (encoder == null and glinerGgufMatchesSuffix(f.name, "encoder", quant_suffix)) {
            encoder = f;
        } else if (head == null and glinerGgufMatchesSuffix(f.name, "head", quant_suffix)) {
            head = f;
        }
    }

    if (encoder == null or head == null) return false;
    try to_download.append(allocator, encoder.?);
    try to_download.append(allocator, head.?);
    return true;
}

fn appendGlinerSplitGgufBundle(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    quant_filter: ?[]const u8,
) !bool {
    if (quant_filter) |quant| {
        return appendGlinerSplitGgufBundleForQuant(allocator, to_download, files, quant);
    }

    for (&gguf_quant_preference) |quant| {
        if (try appendGlinerSplitGgufBundleForQuant(allocator, to_download, files, quant)) return true;
    }
    if (try appendGlinerSplitGgufBundleForQuant(allocator, to_download, files, "")) return true;

    for (files) |f| {
        const suffix = glinerGgufSuffix(f.name, "encoder") orelse continue;
        if (try appendGlinerSplitGgufBundleForQuant(allocator, to_download, files, suffix)) return true;
    }
    return false;
}

fn hasGlinerSplitGgufCandidate(files: []const HubFile) bool {
    for (files) |file| {
        if (isGlinerEncoderGgufFile(file.name) or isGlinerHeadGgufFile(file.name)) return true;
    }
    return false;
}

fn hasClipclapGgufCandidate(files: []const HubFile) bool {
    for (files) |file| {
        if (isClipclapClipGgufFile(file.name) or isClipclapClapGgufFile(file.name)) return true;
    }
    return false;
}

fn appendBestRequestedGgufPayload(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    quant_filter: ?[]const u8,
    projector_selection: ProjectorSelection,
) !bool {
    const has_clipclap_gguf = hasClipclapGgufCandidate(files);
    if (try appendBestClipclapGgufPair(allocator, to_download, files, quant_filter)) {
        return true;
    }
    if (has_clipclap_gguf) return false;
    const has_gliner_split_gguf = hasGlinerSplitGgufCandidate(files);
    if (try appendGlinerSplitGgufBundle(allocator, to_download, files, quant_filter)) {
        return true;
    }
    if (has_gliner_split_gguf) return false;
    if (try appendBestGgufFile(allocator, to_download, files, quant_filter)) {
        _ = try appendSelectedGgufProjectorFile(allocator, to_download, files, projector_selection);
        return true;
    }
    return false;
}

/// Find the best .gguf file in the repo. If quant_filter is set (e.g. "Q4_K_M"),
/// pick the file whose name contains that substring. Otherwise pick by preference order.
fn appendBestGgufFile(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    quant_filter: ?[]const u8,
) !bool {
    // If a specific quant was requested, find the first .gguf matching it.
    if (quant_filter) |filter| {
        for (files) |f| {
            if (isGgufFile(f.name) and
                !isGgufProjectorFile(f.name) and
                ggufQuantSuffixMatches(f.name, filter))
            {
                try to_download.append(allocator, f);
                return true;
            }
        }
        return false;
    }
    // Auto-select: try preferred quants in order.
    for (&gguf_quant_preference) |quant| {
        for (files) |f| {
            if (isGgufFile(f.name) and
                !isGgufProjectorFile(f.name) and
                ggufQuantSuffixMatches(f.name, quant))
            {
                try to_download.append(allocator, f);
                return true;
            }
        }
    }
    // Fallback: any .gguf file.
    for (files) |f| {
        if (isGgufFile(f.name) and !isGgufProjectorFile(f.name)) {
            try to_download.append(allocator, f);
            return true;
        }
    }
    return false;
}

/// Find the best external multimodal projector GGUF, if the repo provides one.
fn appendBestGgufProjectorFile(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !bool {
    for (&gguf_projector_preference) |preferred| {
        for (files) |f| {
            if (isGgufProjectorFile(f.name) and std.mem.indexOf(u8, f.name, preferred) != null) {
                try to_download.append(allocator, f);
                return true;
            }
        }
    }

    for (files) |f| {
        if (isGgufProjectorFile(f.name)) {
            try to_download.append(allocator, f);
            return true;
        }
    }

    return false;
}

fn appendSelectedGgufProjectorFile(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    selection: ProjectorSelection,
) !bool {
    return switch (selection) {
        .auto => appendBestGgufProjectorFile(allocator, to_download, files),
        .none => false,
        .match => |value| appendMatchingGgufProjectorFile(allocator, to_download, files, value),
    };
}

fn appendMatchingGgufProjectorFile(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    selector: []const u8,
) !bool {
    for (files) |f| {
        if (!isGgufProjectorFile(f.name)) continue;
        const base = basename(f.name);
        if (std.mem.eql(u8, f.name, selector) or
            std.mem.eql(u8, base, selector) or
            ggufQuantSuffixMatches(f.name, selector))
        {
            try to_download.append(allocator, f);
            return true;
        }
    }
    return false;
}

fn appendMatchingFile(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    candidate: []const u8,
) !bool {
    for (files) |f| {
        if (std.mem.eql(u8, f.name, candidate)) {
            try to_download.append(allocator, f);
            return true;
        }
    }
    return false;
}

fn appendFirstMatchingFile(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    candidates: []const []const u8,
) !bool {
    for (candidates) |candidate| {
        if (try appendMatchingFile(allocator, to_download, files, candidate)) {
            return true;
        }
    }
    return false;
}

fn appendMatchingFileIfMissing(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    candidate: []const u8,
) !bool {
    for (to_download.items) |existing| {
        if (std.mem.eql(u8, existing.name, candidate)) return false;
    }
    return appendMatchingFile(allocator, to_download, files, candidate);
}

fn appendSidecarIfPresent(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    filename: []const u8,
) !void {
    const dot_data = try std.fmt.allocPrint(allocator, "{s}.data", .{filename});
    defer allocator.free(dot_data);
    _ = try appendMatchingFileIfMissing(allocator, to_download, files, dot_data);

    if (std.mem.endsWith(u8, filename, ".onnx")) {
        const underscore_data = try std.fmt.allocPrint(allocator, "{s}_data", .{filename});
        defer allocator.free(underscore_data);
        _ = try appendMatchingFileIfMissing(allocator, to_download, files, underscore_data);
    }
}

fn appendAllMatchingFiles(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    candidates: []const []const u8,
) !bool {
    var found = false;
    for (candidates) |candidate| {
        if (try appendMatchingFileIfMissing(allocator, to_download, files, candidate)) {
            found = true;
            try appendSidecarIfPresent(allocator, to_download, files, candidate);
        }
    }
    return found;
}

fn appendPreferredOnnxPayload(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    candidates: []const []const u8,
) !bool {
    const found = try appendFirstMatchingFile(allocator, to_download, files, candidates);
    if (!found) return false;

    const payload = to_download.items[to_download.items.len - 1].name;
    try appendSidecarIfPresent(allocator, to_download, files, payload);
    return true;
}

fn splitDirAndBase(path: []const u8) struct { dir: []const u8, base: []const u8 } {
    if (std.mem.lastIndexOfScalar(u8, path, '/')) |slash| {
        return .{ .dir = path[0 .. slash + 1], .base = path[slash + 1 ..] };
    }
    return .{ .dir = "", .base = path };
}

fn allDigits(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |ch| {
        if (!std.ascii.isDigit(ch)) return false;
    }
    return true;
}

fn isSafetensorsShardForIndex(index_name: []const u8, candidate_name: []const u8) bool {
    const index_parts = splitDirAndBase(index_name);
    const candidate_parts = splitDirAndBase(candidate_name);
    if (!std.mem.eql(u8, index_parts.dir, candidate_parts.dir)) return false;

    const index_suffix = ".safetensors.index.json";
    if (!std.mem.endsWith(u8, index_parts.base, index_suffix)) return false;
    const stem = index_parts.base[0 .. index_parts.base.len - index_suffix.len];

    const shard_suffix = ".safetensors";
    if (!std.mem.endsWith(u8, candidate_parts.base, shard_suffix)) return false;
    const shard_stem = candidate_parts.base[0 .. candidate_parts.base.len - shard_suffix.len];

    const prefix_len = stem.len + 1;
    if (shard_stem.len <= prefix_len) return false;
    if (!std.mem.startsWith(u8, shard_stem, stem) or shard_stem[stem.len] != '-') return false;

    const shard_numbers = shard_stem[prefix_len..];
    const of_marker = "-of-";
    const of_index = std.mem.indexOf(u8, shard_numbers, of_marker) orelse return false;
    if (std.mem.indexOfPos(u8, shard_numbers, of_index + of_marker.len, of_marker) != null) return false;

    const shard_number = shard_numbers[0..of_index];
    const shard_total = shard_numbers[of_index + of_marker.len ..];
    return allDigits(shard_number) and allDigits(shard_total);
}

fn appendSafetensorsIndexAndShards(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
    index_name: []const u8,
) !bool {
    var index_file: ?HubFile = null;
    var shard_count: usize = 0;

    for (files) |file| {
        if (std.mem.eql(u8, file.name, index_name)) {
            index_file = file;
        } else if (isSafetensorsShardForIndex(index_name, file.name)) {
            shard_count += 1;
        }
    }

    if (index_file == null or shard_count == 0) return false;

    try to_download.append(allocator, index_file.?);
    for (files) |file| {
        if (isSafetensorsShardForIndex(index_name, file.name)) {
            try to_download.append(allocator, file);
        }
    }
    return true;
}

fn appendPreferredSafetensorsPayload(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !bool {
    for (&safetensors_index_candidates) |candidate| {
        if (try appendSafetensorsIndexAndShards(allocator, to_download, files, candidate)) {
            return true;
        }
    }

    return appendFirstMatchingFile(allocator, to_download, files, &safetensors_candidates);
}

fn isAdapterArtifact(path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, "adapters/")) return false;
    return std.mem.endsWith(u8, path, "/adapter_config.json") or
        std.mem.endsWith(u8, path, "/adapter_model.safetensors");
}

fn appendAdapterArtifacts(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !usize {
    var appended: usize = 0;
    for (files) |file| {
        if (!isAdapterArtifact(file.name)) continue;
        if (try appendMatchingFileIfMissing(allocator, to_download, files, file.name)) {
            appended += 1;
        }
    }
    return appended;
}

fn hasFile(files: []const HubFile, candidate: []const u8) bool {
    for (files) |file| {
        if (std.mem.eql(u8, file.name, candidate)) return true;
    }
    return false;
}

fn findPreferredFile(
    files: []const HubFile,
    suffixes: []const []const u8,
    preferred_substrings: []const []const u8,
) ?HubFile {
    for (preferred_substrings) |preferred| {
        for (files) |file| {
            for (suffixes) |suffix| {
                if (std.mem.endsWith(u8, file.name, suffix) and std.mem.indexOf(u8, file.name, preferred) != null) {
                    return file;
                }
            }
        }
    }

    for (files) |file| {
        for (suffixes) |suffix| {
            if (std.mem.endsWith(u8, file.name, suffix)) return file;
        }
    }

    return null;
}

fn appendMultiStageArtifacts(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !bool {
    if (!hasFile(files, "antfly_metadata.json")) return false;

    _ = try appendMatchingFileIfMissing(allocator, to_download, files, "antfly_metadata.json");

    var found_stage_payload = false;
    for (files) |file| {
        if (std.mem.endsWith(u8, file.name, ".onnx")) {
            if (try appendMatchingFileIfMissing(allocator, to_download, files, file.name)) {
                found_stage_payload = true;
                try appendSidecarIfPresent(allocator, to_download, files, file.name);
            }
            continue;
        }

        if (std.mem.endsWith(u8, file.name, ".json") or
            std.mem.endsWith(u8, file.name, ".txt") or
            std.mem.endsWith(u8, file.name, ".model"))
        {
            _ = try appendMatchingFileIfMissing(allocator, to_download, files, file.name);
        }
    }

    return found_stage_payload;
}

fn appendPreexportedPaddleOCRArtifacts(
    allocator: std.mem.Allocator,
    to_download: *std.ArrayListUnmanaged(HubFile),
    files: []const HubFile,
) !SyntheticMetadataPlan {
    if (hasFile(files, "antfly_metadata.json")) return .none;

    const det = findPreferredFile(files, &.{ "/det.onnx", "det.onnx" }, &.{ "/v3/", "/v5/" }) orelse return .none;
    const rec = findPreferredFile(files, &.{ "/rec.onnx", "rec.onnx" }, &.{ "/english/", "/latin/" }) orelse return .none;
    const dict = findPreferredFile(files, &.{ "/dict.txt", "ppocr_keys_v1.txt", "dict.txt" }, &.{ "/english/", "/latin/" }) orelse return .none;

    _ = try appendMatchingFileIfMissing(allocator, to_download, files, det.name);
    try appendSidecarIfPresent(allocator, to_download, files, det.name);
    _ = try appendMatchingFileIfMissing(allocator, to_download, files, rec.name);
    try appendSidecarIfPresent(allocator, to_download, files, rec.name);
    _ = try appendMatchingFileIfMissing(allocator, to_download, files, dict.name);

    const det_dir = std.fs.path.dirname(det.name);
    if (det_dir) |path| {
        const config = try std.fmt.allocPrint(allocator, "{s}/config.json", .{path});
        defer allocator.free(config);
        _ = try appendMatchingFileIfMissing(allocator, to_download, files, config);
    }
    const rec_dir = std.fs.path.dirname(rec.name);
    if (rec_dir) |path| {
        const config = try std.fmt.allocPrint(allocator, "{s}/config.json", .{path});
        defer allocator.free(config);
        _ = try appendMatchingFileIfMissing(allocator, to_download, files, config);
    }

    return .{ .paddleocr = .{
        .detection_model = det.name,
        .recognition_model = rec.name,
        .char_dict_file = dict.name,
    } };
}

fn writeSyntheticMetadata(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
    plan: SyntheticMetadataPlan,
) !?ManagedArtifactReceipt {
    switch (plan) {
        .none => return null,
        .paddleocr => |payload| {
            const metadata =
                try std.fmt.allocPrint(allocator,
                    \\{{
                    \\  "model_type": "paddleocr",
                    \\  "pipeline_type": "multistage_ocr",
                    \\  "stages": {{
                    \\    "detection": {{
                    \\      "model_file": "{s}",
                    \\      "post_processor": "db"
                    \\    }},
                    \\    "recognition": {{
                    \\      "type": "ctc",
                    \\      "model_file": "{s}",
                    \\      "char_dict_file": "{s}"
                    \\    }}
                    \\  }}
                    \\}}
                , .{ payload.detection_model, payload.recognition_model, payload.char_dict_file });
            defer allocator.free(metadata);

            const metadata_path = try std.fmt.allocPrint(allocator, "{s}/antfly_metadata.json", .{dest_dir});
            defer allocator.free(metadata_path);
            try writeFileAtomically(allocator, io, metadata_path, metadata);
            return .{
                .path = "antfly_metadata.json",
                .size = metadata.len,
            };
        },
    }
}

pub const DownloadProgress = struct {
    file: []const u8,
    bytes_downloaded: u64,
    total_bytes: ?u64,
    files_done: usize,
    files_total: usize,
};

pub const ProgressCallback = *const fn (progress: DownloadProgress, ctx: ?*anyopaque) void;

pub const ProgressSink = struct {
    callback: ?ProgressCallback = null,
    context: ?*anyopaque = null,
};

const progress_report_bytes: u64 = 16 * 1024 * 1024;

const OffsetFileWriter = struct {
    file: std.Io.File,
    io: std.Io,
    offset: u64,
    max_offset: u64,

    pub fn writeAll(self: *OffsetFileWriter, data: []const u8) !void {
        const next_offset = std.math.add(u64, self.offset, data.len) catch
            return error.DownloadSizeLimitExceeded;
        if (next_offset > self.max_offset) return error.DownloadSizeLimitExceeded;
        try self.file.writePositionalAll(self.io, data, self.offset);
        self.offset = next_offset;
    }
};

const FileProgressCtx = struct {
    progress: ProgressSink,
    file: []const u8,
    total_bytes: ?u64,
    files_done: usize,
    files_total: usize,
    base_offset: u64 = 0,
    next_report_at: u64 = progress_report_bytes,

    fn onWriterProgress(progress: httpx.WriterProgress, raw_ctx: ?*anyopaque) void {
        const ctx = raw_ctx orelse return;
        var self: *FileProgressCtx = @ptrCast(@alignCast(ctx));
        const cb = self.progress.callback orelse return;
        const current = self.base_offset + progress.bytes_written;
        const done = if (self.total_bytes) |total| current >= total else false;
        if (done) return;
        if (current < self.next_report_at) return;

        cb(.{
            .file = self.file,
            .bytes_downloaded = current,
            .total_bytes = self.total_bytes,
            .files_done = self.files_done,
            .files_total = self.files_total,
        }, self.progress.context);

        self.next_report_at = current + progress_report_bytes;
    }
};

fn managedPath(
    allocator: std.mem.Allocator,
    dest_dir: []const u8,
    filename: []const u8,
) ![]u8 {
    return std.fs.path.join(allocator, &.{ dest_dir, filename });
}

fn managedSiblingPath(
    allocator: std.mem.Allocator,
    destination: []const u8,
    suffix: []const u8,
) ![]u8 {
    const leaf = try std.fmt.allocPrint(allocator, ".{s}{s}", .{ std.fs.path.basename(destination), suffix });
    defer allocator.free(leaf);
    return std.fs.path.join(allocator, &.{ std.fs.path.dirname(destination) orelse ".", leaf });
}

pub fn managedModelLockPathAlloc(
    allocator: std.mem.Allocator,
    destination: []const u8,
) ![]u8 {
    return managedSiblingPath(allocator, destination, managed_download_lock_filename);
}

fn pathExists(dir: std.Io.Dir, io: std.Io, path: []const u8) !bool {
    dir.access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn removeTreeIfExists(dir: std.Io.Dir, io: std.Io, path: []const u8) !void {
    if (!try pathExists(dir, io, path)) return;
    try dir.deleteTree(io, path);
}

fn seedManagedArtifactsWithHardLinks(
    allocator: std.mem.Allocator,
    io: std.Io,
    source_dir: []const u8,
    staging_dir: []const u8,
) !void {
    var receipt = managed_receipt.loadValidated(allocator, io, source_dir) catch |err| switch (err) {
        error.InvalidManagedDownload,
        error.IncompleteManagedDownload,
        error.ModelArtifactOutsideRoot,
        => return,
        else => return err,
    } orelse return;
    defer receipt.deinit();

    const cwd = std.Io.Dir.cwd();
    for (receipt.artifacts) |artifact| {
        const staging_path = try std.fs.path.join(allocator, &.{ staging_dir, artifact.path });
        defer allocator.free(staging_path);
        if (std.fs.path.dirname(staging_path)) |parent| try cwd.createDirPath(io, parent);

        // Hard links let a repair reuse verified multi-gigabyte weights without
        // copying them. Downloads install replacements with atomic rename, so
        // changing the staged file can never mutate the published inode.
        std.Io.Dir.hardLink(cwd, artifact.canonical_path, cwd, staging_path, io, .{ .follow_symlinks = false }) catch continue;
    }
}

/// A pull transaction built beside the published model directory.
///
/// Downloads never mutate the active cache. Verified existing artifacts are
/// hard-linked into the staging tree for zero-copy reuse, and the completed
/// tree is published with same-filesystem renames. An interrupted publication
/// is recovered on the next pull from the retained backup directory.
pub const ManagedModelTransaction = struct {
    allocator: std.mem.Allocator,
    destination: []u8,
    staging: []u8,
    backup: []u8,
    committed: bool = false,

    pub fn begin(
        allocator: std.mem.Allocator,
        io: std.Io,
        destination: []const u8,
    ) !ManagedModelTransaction {
        const owned_destination = try allocator.dupe(u8, destination);
        errdefer allocator.free(owned_destination);
        const staging = try managedSiblingPath(allocator, destination, managed_download_staging_suffix);
        errdefer allocator.free(staging);
        const backup = try managedSiblingPath(allocator, destination, managed_download_backup_suffix);
        errdefer allocator.free(backup);
        const transaction = ManagedModelTransaction{
            .allocator = allocator,
            .destination = owned_destination,
            .staging = staging,
            .backup = backup,
        };

        const cwd = std.Io.Dir.cwd();
        const destination_exists = try pathExists(cwd, io, transaction.destination);
        const backup_exists = try pathExists(cwd, io, transaction.backup);
        var recovered_publication = false;
        if (backup_exists) {
            if (!destination_exists) {
                try std.Io.Dir.rename(cwd, transaction.backup, cwd, transaction.destination, io);
            } else if (managedDownloadState(allocator, io, transaction.destination) == .complete) {
                try removeTreeIfExists(cwd, io, transaction.backup);
            } else {
                try removeTreeIfExists(cwd, io, transaction.destination);
                try std.Io.Dir.rename(cwd, transaction.backup, cwd, transaction.destination, io);
            }
            recovered_publication = true;
        }
        if (recovered_publication) {
            try syncManagedDirectory(io, std.fs.path.dirname(transaction.destination) orelse ".");
        }

        if (!try pathExists(cwd, io, transaction.staging)) {
            try cwd.createDirPath(io, transaction.staging);
        }
        try seedManagedArtifactsWithHardLinks(allocator, io, transaction.destination, transaction.staging);
        try beginManagedDownload(allocator, io, transaction.staging);
        return transaction;
    }

    pub fn commit(self: *ManagedModelTransaction, io: std.Io) !void {
        if (self.committed) return error.ManagedDownloadAlreadyCommitted;
        if (managedDownloadState(self.allocator, io, self.staging) != .complete) {
            return error.IncompleteManagedDownload;
        }

        const cwd = std.Io.Dir.cwd();
        try removeTreeIfExists(cwd, io, self.backup);
        const had_destination = try pathExists(cwd, io, self.destination);
        if (had_destination) {
            try std.Io.Dir.rename(cwd, self.destination, cwd, self.backup, io);
        }
        std.Io.Dir.rename(cwd, self.staging, cwd, self.destination, io) catch |err| {
            if (had_destination) {
                std.Io.Dir.rename(cwd, self.backup, cwd, self.destination, io) catch {};
            }
            return err;
        };

        const parent = std.fs.path.dirname(self.destination) orelse ".";
        syncManagedDirectory(io, parent) catch |err| {
            removeTreeIfExists(cwd, io, self.destination) catch {};
            if (had_destination) {
                std.Io.Dir.rename(cwd, self.backup, cwd, self.destination, io) catch {};
            }
            return err;
        };

        // Publication is durable at this point. Backup cleanup is best effort:
        // retaining it is safe, and the next transaction will remove it.
        self.committed = true;
        if (had_destination) {
            removeTreeIfExists(cwd, io, self.backup) catch return;
            syncManagedDirectory(io, parent) catch {};
        }
    }

    pub fn deinit(self: *ManagedModelTransaction, io: std.Io) void {
        _ = io;
        // Failed pulls intentionally retain the hidden staging tree. Verified
        // final artifacts and digest-bound .part files can then be reused by
        // the next locked attempt without exposing them through discovery.
        self.freePaths();
        self.* = undefined;
    }

    fn freePaths(self: *ManagedModelTransaction) void {
        self.allocator.free(self.destination);
        self.allocator.free(self.staging);
        self.allocator.free(self.backup);
    }
};

fn writeFileAtomically(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    data: []const u8,
) !void {
    const temp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{path});
    defer allocator.free(temp_path);

    var cwd = std.Io.Dir.cwd();
    cwd.deleteFile(io, temp_path) catch {};
    errdefer cwd.deleteFile(io, temp_path) catch {};
    {
        var file = try cwd.createFile(io, temp_path, .{ .read = true });
        defer file.close(io);
        try file.writePositionalAll(io, data, 0);
        try file.sync(io);
    }
    try std.Io.Dir.rename(cwd, temp_path, cwd, path, io);
    try syncManagedDirectory(io, std.fs.path.dirname(path) orelse ".");
}

/// Atomically replace a private staging artifact and update its validated plan.
/// Callers must hold the model pull lock; published directories must never use
/// this API because the artifact and plan renames are intentionally separate.
pub fn writeManagedArtifactAndUpdatePlan(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
    relative_path: []const u8,
    data: []const u8,
) !void {
    if (!managed_receipt.artifactPathIsSafe(relative_path)) return error.InvalidModelArtifactPath;

    var validated = try managed_receipt.loadValidatedPlan(allocator, io, dest_dir);
    defer validated.deinit();

    var artifacts = std.ArrayListUnmanaged(ManagedArtifactReceipt).empty;
    defer artifacts.deinit(allocator);
    try artifacts.ensureTotalCapacity(allocator, validated.artifacts.len + 1);
    var replaced = false;
    for (validated.artifacts) |artifact| {
        if (std.mem.eql(u8, artifact.path, relative_path)) {
            try artifacts.append(allocator, .{
                .path = relative_path,
                .size = data.len,
            });
            replaced = true;
        } else {
            try artifacts.append(allocator, .{
                .path = artifact.path,
                .size = artifact.size,
                .sha256 = artifact.sha256,
            });
        }
    }
    if (!replaced) {
        if (validated.artifacts.len >= managed_receipt.max_artifact_count) {
            return error.InvalidManagedDownload;
        }
        try artifacts.append(allocator, .{
            .path = relative_path,
            .size = data.len,
        });
    }

    const receipt_json = try std.json.Stringify.valueAlloc(
        allocator,
        ManagedDownloadReceipt{
            .version = validated.parsed.value.version,
            .source = validated.parsed.value.source,
            .artifacts = artifacts.items,
        },
        .{},
    );
    defer allocator.free(receipt_json);
    if (receipt_json.len > managed_receipt.max_receipt_bytes) return error.InvalidManagedDownload;
    const artifact_path = try managedPath(allocator, dest_dir, relative_path);
    defer allocator.free(artifact_path);
    const plan_path = try managedPath(allocator, dest_dir, managed_download_plan_filename);
    defer allocator.free(plan_path);

    try writeFileAtomically(allocator, io, artifact_path, data);
    try writeFileAtomically(allocator, io, plan_path, receipt_json);
}

fn syncManagedDirectory(io: std.Io, path: []const u8) !void {
    // Windows does not expose directory FlushFileBuffers through std.Io.
    // Atomic rename still provides fail-safe visibility there; POSIX targets
    // additionally persist directory-entry ordering across power loss.
    if (builtin.os.tag == .windows or
        builtin.os.tag == .wasi or
        builtin.os.tag == .freestanding)
    {
        return;
    }
    // On Linux, a non-iterable std.Io.Dir is opened with O_PATH. fsync(2)
    // rejects O_PATH descriptors with EBADF, so request a regular O_RDONLY
    // directory descriptor even though no iteration is performed.
    const open_options: std.Io.Dir.OpenOptions = .{ .iterate = true };
    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, open_options)
    else
        try std.Io.Dir.cwd().openDir(io, path, open_options);
    defer dir.close(io);
    while (true) switch (std.posix.errno(std.posix.system.fsync(dir.handle))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return,
        .BADF => return error.InvalidFileDescriptor,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

pub fn beginManagedDownload(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) !void {
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const in_progress_path = try managedPath(allocator, dest_dir, managed_download_in_progress_filename);
    defer allocator.free(in_progress_path);
    const plan_path = try managedPath(allocator, dest_dir, managed_download_plan_filename);
    defer allocator.free(plan_path);
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);

    try writeFileAtomically(
        allocator,
        io,
        in_progress_path,
        "{\"version\":1,\"state\":\"in_progress\"}\n",
    );
    var cwd = std.Io.Dir.cwd();
    cwd.deleteFile(io, plan_path) catch {};
    cwd.deleteFile(io, complete_path) catch {};
    try syncManagedDirectory(io, dest_dir);
}

pub fn completeManagedDownload(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) !void {
    const in_progress_path = try managedPath(allocator, dest_dir, managed_download_in_progress_filename);
    defer allocator.free(in_progress_path);
    const plan_path = try managedPath(allocator, dest_dir, managed_download_plan_filename);
    defer allocator.free(plan_path);
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);

    var cwd = std.Io.Dir.cwd();
    try std.Io.Dir.rename(cwd, plan_path, cwd, complete_path, io);
    // Persist the completion receipt before removing the fail-closed marker.
    try syncManagedDirectory(io, dest_dir);
    try cwd.deleteFile(io, in_progress_path);
    try syncManagedDirectory(io, dest_dir);
}

/// Return the publication state of a model directory managed by `pull`.
///
/// A completion receipt is accepted only when every recorded artifact still
/// exists at the recorded size. This is intentionally metadata-only: hashing
/// multi-gigabyte model files on every discovery would make startup
/// prohibitively expensive, while downloads are digest-verified before the
/// receipt is published.
pub fn managedDownloadState(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) ManagedDownloadState {
    var receipt = managed_receipt.loadValidated(allocator, io, dest_dir) catch return .incomplete;
    if (receipt) |*validated| {
        validated.deinit();
        return .complete;
    }
    return .unmanaged;
}

/// Verify that a completed managed directory was published for the exact Hub
/// reference being requested. Version-1 receipts predate source identity and
/// therefore cannot prove an explicit variant match.
pub fn managedDownloadMatchesSource(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
    owner: []const u8,
    name: []const u8,
    variant: []const u8,
) bool {
    const receipt = managed_receipt.loadValidated(allocator, io, dest_dir) catch return false;
    var validated = receipt orelse return false;
    defer validated.deinit();
    const source = validated.parsed.value.source orelse return false;
    return validated.parsed.value.version == 2 and
        std.mem.eql(u8, source.owner, owner) and
        std.mem.eql(u8, source.name, name) and
        std.mem.eql(u8, source.variant, variant);
}

pub fn managedDownloadPublicationBlocked(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
) bool {
    return managed_receipt.publicationBlocked(allocator, io, dest_dir);
}

/// Download a model from HuggingFace Hub.
pub fn downloadModel(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    name: []const u8,
    variant: []const u8,
    dest_dir: []const u8,
    config: HubConfig,
    projector_selection: ProjectorSelection,
    progress: ProgressSink,
) !void {
    if (config.max_artifact_bytes == 0 or config.max_model_bytes == 0) {
        return error.InvalidDownloadSizeLimit;
    }

    // Create destination directory (pure Zig, cross-platform)
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);

    // List model files from Hub API
    const files = try listModelFiles(allocator, io, owner, name, config);
    defer {
        for (files) |f| {
            allocator.free(f.name);
            if (f.sha256) |sum| allocator.free(sum);
        }
        allocator.free(files);
    }

    // Determine which files to download
    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);
    var synthetic_metadata: SyntheticMetadataPlan = .none;

    const want_mmproj = std.mem.eql(u8, variant, "mmproj") or
        std.mem.eql(u8, variant, "projector") or
        std.mem.startsWith(u8, variant, "mmproj:") or
        std.mem.startsWith(u8, variant, "projector:");

    // Always-download files (config, tokenizer, etc.) unless explicitly only
    // fetching the external multimodal projector.
    if (!want_mmproj) {
        for (&always_files) |candidate| {
            for (files) |f| {
                if (std.mem.eql(u8, f.name, candidate)) {
                    try to_download.append(allocator, f);
                    break;
                }
            }
        }
        _ = try appendAdapterArtifacts(allocator, &to_download, files);
    }

    var found_model_payload = false;

    const want_gguf = std.mem.eql(u8, variant, "gguf") or std.mem.startsWith(u8, variant, "gguf:");
    const want_onnx = std.mem.eql(u8, variant, "onnx") or std.mem.eql(u8, variant, "f32") or std.mem.eql(u8, variant, "i8");
    const want_safetensors = std.mem.eql(u8, variant, "safetensors");
    const want_hybrid = std.mem.eql(u8, variant, "hybrid") or
        std.mem.eql(u8, variant, "onnx+native") or
        std.mem.eql(u8, variant, "native+onnx");
    // Auto-detect: no specific format requested — grab everything available.
    const auto_detect = !want_gguf and !want_onnx and !want_safetensors and !want_hybrid and !want_mmproj;

    // GGUF
    if (want_gguf or auto_detect) {
        const quant_filter: ?[]const u8 = if (std.mem.startsWith(u8, variant, "gguf:"))
            variant["gguf:".len..]
        else
            null;
        if (try appendBestRequestedGgufPayload(allocator, &to_download, files, quant_filter, projector_selection)) {
            found_model_payload = true;
        }
    }

    // External multimodal projector only.
    if (want_mmproj) {
        const mmproj_selection: ProjectorSelection = if (std.mem.startsWith(u8, variant, "mmproj:"))
            .{ .match = variant["mmproj:".len..] }
        else if (std.mem.startsWith(u8, variant, "projector:"))
            .{ .match = variant["projector:".len..] }
        else
            projector_selection;
        if (try appendSelectedGgufProjectorFile(allocator, &to_download, files, mmproj_selection))
            found_model_payload = true;
    }

    // ONNX
    if (want_onnx or want_hybrid or auto_detect) {
        if (std.mem.eql(u8, variant, "i8")) {
            if (try appendFirstMatchingFile(allocator, &to_download, files, &[_][]const u8{ "model_i8.onnx", "model_quantized.onnx", "onnx/model_quantized.onnx" }))
                found_model_payload = true;
        } else {
            const found_primary = try appendPreferredOnnxPayload(allocator, &to_download, files, &onnx_candidates);
            const found_multimodal = try appendAllMatchingFiles(allocator, &to_download, files, &multimodal_onnx_candidates);
            const found_seq2seq = try appendAllMatchingFiles(allocator, &to_download, files, &seq2seq_onnx_candidates);
            const found_decoder_only_vlm_decoder = try appendPreferredOnnxPayload(allocator, &to_download, files, &decoder_only_vlm_decoder_candidates);
            const found_decoder_only_vlm_embed = try appendPreferredOnnxPayload(allocator, &to_download, files, &decoder_only_vlm_embed_candidates);
            const found_decoder_only_vlm_vision = try appendPreferredOnnxPayload(allocator, &to_download, files, &decoder_only_vlm_vision_candidates);
            const found_decoder_only_vlm = found_decoder_only_vlm_decoder or found_decoder_only_vlm_embed or found_decoder_only_vlm_vision;
            const found_multistage = try appendMultiStageArtifacts(allocator, &to_download, files);
            if (!found_multistage and synthetic_metadata == .none) {
                synthetic_metadata = try appendPreexportedPaddleOCRArtifacts(allocator, &to_download, files);
            }
            if (found_primary or found_multimodal or found_seq2seq or found_decoder_only_vlm or found_multistage or synthetic_metadata != .none)
                found_model_payload = true;
        }
    }

    // SafeTensors
    if (want_safetensors or want_hybrid or auto_detect or
        // Legacy: "onnx"/"f32" falls back to safetensors when no ONNX found
        (want_onnx and !found_model_payload))
    {
        if (try appendPreferredSafetensorsPayload(allocator, &to_download, files))
            found_model_payload = true;
    }

    if (!found_model_payload) {
        const advice = noModelFilesAdviceAlloc(allocator, owner, name, variant, files) catch null;
        if (advice) |message| {
            defer allocator.free(message);
            std.debug.print("{s}", .{message});
        }
        return error.NoModelFilesFound;
    }

    var resolved = std.ArrayListUnmanaged(ResolvedArtifact).empty;
    defer resolved.deinit(allocator);
    var known_model_bytes: u64 = 0;
    for (to_download.items) |file_meta| {
        const filename = file_meta.name;
        const total_bytes = if (file_meta.size) |declared_size| blk: {
            if (shouldProbeLinkedPayloadSize(filename, declared_size)) {
                // A small size for a binary artifact is normally the Git LFS
                // pointer size, not the payload size. If probing fails, leave
                // the size unknown rather than treating the pointer as a
                // trustworthy completion boundary.
                break :blk probeDownloadSize(allocator, io, owner, name, filename, config) catch null;
            }
            break :blk declared_size;
        } else (probeDownloadSize(allocator, io, owner, name, filename, config) catch null);
        if (total_bytes) |total| {
            if (total > config.max_artifact_bytes) {
                return error.DownloadSizeLimitExceeded;
            }
            known_model_bytes = try addKnownModelBytes(
                known_model_bytes,
                total,
                config.max_model_bytes,
            );
        }
        try resolved.append(allocator, .{
            .file = file_meta,
            .total_bytes = total_bytes,
        });
    }

    var receipts = std.ArrayListUnmanaged(ManagedArtifactReceipt).empty;
    defer receipts.deinit(allocator);
    var remaining_model_bytes = config.max_model_bytes;

    // Download each file. Unknown-size artifacts receive the remaining model
    // budget as their tighter streaming ceiling.
    for (resolved.items, 0..) |artifact, i| {
        const file_meta = artifact.file;
        const filename = file_meta.name;
        const total_bytes = artifact.total_bytes;
        if (total_bytes) |total| {
            if (total > remaining_model_bytes) return error.ModelSizeLimitExceeded;
        }

        var artifact_config = config;
        artifact_config.max_artifact_bytes = @min(
            config.max_artifact_bytes,
            remaining_model_bytes,
        );
        if (artifact_config.max_artifact_bytes == 0) {
            return error.ModelSizeLimitExceeded;
        }

        const progress_total_bytes = existingFinalFileProgressSize(
            allocator,
            io,
            dest_dir,
            filename,
            total_bytes,
        ) catch total_bytes;
        if (progress.callback) |cb| {
            cb(.{
                .file = filename,
                .bytes_downloaded = 0,
                .total_bytes = progress_total_bytes,
                .files_done = i,
                .files_total = to_download.items.len,
            }, progress.context);
        }

        try downloadFile(allocator, io, owner, name, filename, dest_dir, artifact_config, progress, i, resolved.items.len, total_bytes, file_meta.sha256);

        const dest_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dest_dir, filename });
        defer allocator.free(dest_path);
        const artifact_size = try requiredFileSize(std.Io.Dir.cwd(), io, dest_path);
        remaining_model_bytes = try consumeModelBudget(
            remaining_model_bytes,
            artifact_size,
        );
        try receipts.append(allocator, .{
            .path = filename,
            .size = artifact_size,
            .sha256 = file_meta.sha256,
        });

        if (progress.callback) |cb| {
            cb(.{
                .file = filename,
                .bytes_downloaded = progress_total_bytes orelse 0,
                .total_bytes = progress_total_bytes,
                .files_done = i + 1,
                .files_total = to_download.items.len,
            }, progress.context);
        }
    }

    if (try writeSyntheticMetadata(allocator, io, dest_dir, synthetic_metadata)) |metadata_receipt| {
        var replaced = false;
        for (receipts.items) |*receipt| {
            if (!std.mem.eql(u8, receipt.path, metadata_receipt.path)) continue;
            receipt.* = metadata_receipt;
            replaced = true;
            break;
        }
        if (!replaced) try receipts.append(allocator, metadata_receipt);
    }

    const receipt_json = try std.json.Stringify.valueAlloc(
        allocator,
        ManagedDownloadReceipt{
            .version = 2,
            .source = .{ .owner = owner, .name = name, .variant = variant },
            .artifacts = receipts.items,
        },
        .{},
    );
    defer allocator.free(receipt_json);
    if (receipts.items.len == 0 or
        receipts.items.len > managed_receipt.max_artifact_count or
        receipt_json.len > managed_receipt.max_receipt_bytes)
    {
        return error.InvalidManagedDownload;
    }
    const plan_path = try managedPath(allocator, dest_dir, managed_download_plan_filename);
    defer allocator.free(plan_path);
    try writeFileAtomically(allocator, io, plan_path, receipt_json);
}

fn probeDownloadSize(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    name: []const u8,
    filename: []const u8,
    config: HubConfig,
) !?u64 {
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/resolve/main/{s}", .{ config.base_url, owner, name, filename });
    defer allocator.free(url);

    var client = httpx.Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .retry_policy = httpx.RetryPolicy.aggressive(),
    });
    defer client.deinit();

    var headers_buf: [2][2][]const u8 = undefined;
    var n_headers: usize = 0;
    var auth_header: ?[]u8 = null;
    defer if (auth_header) |auth| allocator.free(auth);
    headers_buf[n_headers] = .{ "Connection", "close" };
    n_headers += 1;
    if (config.token) |token| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
        headers_buf[n_headers] = .{ "Authorization", auth_header.? };
        n_headers += 1;
    }

    var resp = try client.request(.HEAD, url, .{
        .headers = if (n_headers > 0) headers_buf[0..n_headers] else null,
        .follow_redirects = false,
    });
    defer resp.deinit();

    return chooseProbedDownloadSize(
        resp.ok(),
        resp.isRedirect(),
        resp.header("x-linked-size"),
        resp.contentLength(),
    );
}

fn chooseProbedDownloadSize(
    response_ok: bool,
    response_is_redirect: bool,
    linked_size: ?[]const u8,
    content_length: ?u64,
) ?u64 {
    if (!response_ok and !response_is_redirect) return null;
    if (linked_size) |value| {
        return std.fmt.parseInt(u64, value, 10) catch null;
    }
    // Content-Length on a redirect describes the redirect response, not the
    // target artifact. Treat it as unknown unless the Hub supplies its
    // explicit linked-object size.
    if (!response_ok) return null;
    return content_length;
}

fn shouldProbeLinkedPayloadSize(filename: []const u8, declared_size: u64) bool {
    if (declared_size > 4096) return false;
    return isLargeBinaryModelArtifact(filename);
}

fn isLargeBinaryModelArtifact(filename: []const u8) bool {
    return std.mem.endsWith(u8, filename, ".gguf") or
        std.mem.endsWith(u8, filename, ".safetensors") or
        std.mem.endsWith(u8, filename, ".onnx") or
        std.mem.endsWith(u8, filename, ".onnx.data") or
        std.mem.endsWith(u8, filename, ".bin");
}

fn existingFinalFileProgressSize(
    allocator: std.mem.Allocator,
    io: std.Io,
    dest_dir: []const u8,
    filename: []const u8,
    total_bytes: ?u64,
) !?u64 {
    const total = total_bytes orelse return null;
    if (!shouldProbeLinkedPayloadSize(filename, total)) return total_bytes;
    const dest_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dest_dir, filename });
    defer allocator.free(dest_path);
    const size = existingFileSize(std.Io.Dir.cwd(), io, dest_path) catch |err| switch (err) {
        error.FileNotFound => return total_bytes,
        else => return err,
    };
    if (size > total) return size;
    return total_bytes;
}

/// List all files in a HuggingFace model repo.
pub fn listModelFiles(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    name: []const u8,
    config: HubConfig,
) ![]HubFile {
    const url = try std.fmt.allocPrint(allocator, "{s}/api/models/{s}/{s}", .{ config.base_url, owner, name });
    defer allocator.free(url);

    var client = httpx.Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
    });
    defer client.deinit();

    var headers_buf: [2][2][]const u8 = undefined;
    var n_headers: usize = 0;
    var auth_header: ?[]u8 = null;
    defer if (auth_header) |auth| allocator.free(auth);
    headers_buf[n_headers] = .{ "Connection", "close" };
    n_headers += 1;
    if (config.token) |token| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
        headers_buf[n_headers] = .{ "Authorization", auth_header.? };
        n_headers += 1;
    }

    var resp = try client.get(url, .{
        .headers = if (n_headers > 0) headers_buf[0..n_headers] else null,
    });
    defer resp.deinit();

    if (!resp.ok()) {
        const body = resp.body orelse "";
        const snippet_len = @min(body.len, 256);
        const snippet = body[0..snippet_len];
        std.debug.print(
            "hub api failed for {s}/{s}: HTTP {d}\n{s}{s}\n",
            .{
                owner,
                name,
                resp.status.code,
                snippet,
                if (snippet_len < body.len) "..." else "",
            },
        );
        return error.HubApiError;
    }

    const body = resp.body orelse return error.EmptyResponse;

    // Parse JSON response — extract siblings[].rfilename
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    const siblings = (obj.get("siblings") orelse return error.InvalidResponse).array;

    var files = std.ArrayListUnmanaged(HubFile).empty;
    for (siblings.items) |sibling| {
        if (sibling == .object) {
            if (sibling.object.get("rfilename")) |rf| {
                if (rf == .string) {
                    if (!managed_receipt.artifactPathIsSafe(rf.string)) {
                        return error.InvalidModelArtifactPath;
                    }
                    var file = HubFile{
                        .name = try allocator.dupe(u8, rf.string),
                    };

                    if (sibling.object.get("size")) |size_val| {
                        if (size_val == .integer and size_val.integer >= 0) {
                            file.size = @intCast(size_val.integer);
                        }
                    }

                    if (sibling.object.get("lfs")) |lfs| {
                        if (lfs == .object) {
                            if (lfs.object.get("size")) |lfs_size| {
                                if (lfs_size == .integer and lfs_size.integer >= 0) {
                                    file.size = @intCast(lfs_size.integer);
                                }
                            }
                            if (lfs.object.get("oid")) |oid| {
                                if (oid == .string and oid.string.len == 64) {
                                    file.sha256 = try allocator.dupe(u8, oid.string);
                                }
                            }
                        }
                    }

                    try files.append(allocator, file);
                }
            }
        }
    }

    return try files.toOwnedSlice(allocator);
}

pub fn readModelFileAlloc(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    name: []const u8,
    filename: []const u8,
    config: HubConfig,
    max_bytes: usize,
) ![]u8 {
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/resolve/main/{s}", .{ config.base_url, owner, name, filename });
    defer allocator.free(url);

    var client = httpx.Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .max_response_size = max_bytes,
        .retry_policy = httpx.RetryPolicy.aggressive(),
    });
    defer client.deinit();

    var headers_buf: [3][2][]const u8 = undefined;
    var n_headers: usize = 0;
    var auth_header: ?[]u8 = null;
    defer if (auth_header) |auth| allocator.free(auth);
    headers_buf[n_headers] = .{ "Connection", "close" };
    n_headers += 1;
    headers_buf[n_headers] = .{ "Accept-Encoding", "identity" };
    n_headers += 1;
    if (config.token) |token| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
        headers_buf[n_headers] = .{ "Authorization", auth_header.? };
        n_headers += 1;
    }

    const download_url = try resolveDownloadUrl(
        allocator,
        &client,
        url,
        headers_buf[0..n_headers],
        headers_buf[0..2],
    );
    defer allocator.free(download_url);
    const request_headers = if (sameHttpOrigin(url, download_url))
        headers_buf[0..n_headers]
    else
        headers_buf[0..2];
    var resp = try client.get(download_url, .{
        .headers = request_headers,
        .follow_redirects = false,
        .timeout_ms = 300_000,
    });
    defer resp.deinit();

    if (!resp.ok()) {
        std.debug.print("hub download failed for {s}/{s}/{s}: HTTP {d}\n", .{ owner, name, filename, resp.status.code });
        return error.DownloadFailed;
    }
    const body = resp.body orelse return error.EmptyResponse;
    return try allocator.dupe(u8, body);
}

fn resolveRedirectUrl(allocator: std.mem.Allocator, base_url: []const u8, location: []const u8) ![]u8 {
    if (std.mem.indexOf(u8, location, "://") != null) {
        const has_http = location.len >= 7 and std.ascii.eqlIgnoreCase(location[0..7], "http://");
        const has_https = location.len >= 8 and std.ascii.eqlIgnoreCase(location[0..8], "https://");
        if (!has_http and !has_https) return error.UnsafeRedirect;
        return allocator.dupe(u8, location);
    }

    const base = try httpx.Uri.parse(base_url);
    const scheme = base.scheme orelse "http";
    const host = base.host orelse return error.InvalidUri;
    const port = base.effectivePort();

    if (location.len > 0 and location[0] == '/') {
        return std.fmt.allocPrint(allocator, "{s}://{s}:{d}{s}", .{ scheme, host, port, location });
    }

    const slash = std.mem.lastIndexOfScalar(u8, base.path, '/') orelse 0;
    const prefix = base.path[0 .. slash + 1];
    return std.fmt.allocPrint(allocator, "{s}://{s}:{d}{s}{s}", .{ scheme, host, port, prefix, location });
}

fn sameHttpOrigin(left_url: []const u8, right_url: []const u8) bool {
    const left = httpx.Uri.parse(left_url) catch return false;
    const right = httpx.Uri.parse(right_url) catch return false;
    const left_scheme = left.scheme orelse return false;
    const right_scheme = right.scheme orelse return false;
    const left_host = left.host orelse return false;
    const right_host = right.host orelse return false;
    return std.ascii.eqlIgnoreCase(left_scheme, right_scheme) and
        std.ascii.eqlIgnoreCase(left_host, right_host) and
        left.effectivePort() == right.effectivePort();
}

fn redirectDowngradesTransport(current_url: []const u8, next_url: []const u8) bool {
    const current = httpx.Uri.parse(current_url) catch return true;
    const next = httpx.Uri.parse(next_url) catch return true;
    const current_scheme = current.scheme orelse return true;
    const next_scheme = next.scheme orelse return true;
    return std.ascii.eqlIgnoreCase(current_scheme, "https") and
        !std.ascii.eqlIgnoreCase(next_scheme, "https");
}

fn resolveDownloadUrl(
    allocator: std.mem.Allocator,
    client: *httpx.Client,
    start_url: []const u8,
    authenticated_headers: []const [2][]const u8,
    anonymous_headers: []const [2][]const u8,
) ![]u8 {
    var current_url = try allocator.dupe(u8, start_url);
    errdefer allocator.free(current_url);

    var redirects: u32 = 0;
    while (true) {
        const headers = if (sameHttpOrigin(start_url, current_url))
            authenticated_headers
        else
            anonymous_headers;
        var resp = try client.request(.HEAD, current_url, .{
            .headers = headers,
            .follow_redirects = false,
        });
        defer resp.deinit();

        if (!resp.isRedirect()) return current_url;
        if (redirects >= 10) return error.TooManyRedirects;

        const location = resp.header("Location") orelse return error.InvalidResponse;
        const next_url = try resolveRedirectUrl(allocator, current_url, location);
        if (redirectDowngradesTransport(current_url, next_url)) {
            allocator.free(next_url);
            return error.UnsafeRedirect;
        }
        allocator.free(current_url);
        current_url = next_url;
        redirects += 1;
    }
}

fn downloadResponseLimit(config: HubConfig) !usize {
    if (config.max_artifact_bytes == 0) return error.InvalidDownloadSizeLimit;
    const artifact_limit = std.math.cast(usize, config.max_artifact_bytes) orelse
        return error.InvalidDownloadSizeLimit;
    // httpx counts the initial receive buffer, which can contain both response
    // headers and body bytes, against its client-wide limit. Reserve one full
    // receive buffer for framing; OffsetFileWriter remains the authoritative
    // artifact-size boundary.
    return std.math.add(usize, artifact_limit, 16 * 1024) catch
        return error.InvalidDownloadSizeLimit;
}

fn downloadClientConfig(max_response_size: usize) httpx.ClientConfig {
    return .{
        .keep_alive = false,
        // Model artifacts are streamed directly to disk. The client's
        // in-memory 100 MiB default is inappropriate, but the disk stream must
        // retain a finite response ceiling.
        .max_response_size = max_response_size,
    };
}

const ParsedContentRange = struct {
    start: u64,
    end: u64,
    total: u64,
};

fn parseContentRange(value: []const u8) ?ParsedContentRange {
    const prefix = "bytes ";
    if (!std.mem.startsWith(u8, value, prefix)) return null;
    const range_and_total = value[prefix.len..];
    const slash = std.mem.indexOfScalar(u8, range_and_total, '/') orelse return null;
    const range = range_and_total[0..slash];
    const total_text = range_and_total[slash + 1 ..];
    if (std.mem.eql(u8, total_text, "*")) return null;
    const dash = std.mem.indexOfScalar(u8, range, '-') orelse return null;
    const start = std.fmt.parseInt(u64, range[0..dash], 10) catch return null;
    const end = std.fmt.parseInt(u64, range[dash + 1 ..], 10) catch return null;
    const total = std.fmt.parseInt(u64, total_text, 10) catch return null;
    if (end < start or total == 0 or end >= total) return null;
    return .{ .start = start, .end = end, .total = total };
}

fn contentRangeMatches(
    value: ?[]const u8,
    expected_start: u64,
    final_offset: u64,
    expected_total: ?u64,
) bool {
    const parsed = parseContentRange(value orelse return false) orelse return false;
    if (parsed.start != expected_start) return false;
    if (final_offset == 0 or parsed.end != final_offset - 1) return false;
    if (expected_total) |total| {
        if (parsed.total != total) return false;
    }
    // The client asks for an open-ended range (`bytes=N-`), so a valid
    // response must reach the representation's declared end.
    return final_offset == parsed.total;
}

/// Download a single file from HuggingFace Hub.
fn downloadFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    name: []const u8,
    filename: []const u8,
    dest_dir: []const u8,
    config: HubConfig,
    progress: ProgressSink,
    file_index: usize,
    files_total: usize,
    total_bytes: ?u64,
    expected_sha256: ?[]const u8,
) !void {
    if (!managed_receipt.artifactPathIsSafe(filename)) return error.InvalidModelArtifactPath;
    const max_response_size = try downloadResponseLimit(config);
    if (total_bytes) |total| {
        if (total > config.max_artifact_bytes) return error.DownloadSizeLimitExceeded;
    }

    var dest = std.Io.Dir.cwd();
    const dest_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dest_dir, filename });
    defer allocator.free(dest_path);
    if (try existingFinalFileSatisfies(dest, io, dest_path, expected_sha256)) {
        return;
    }

    var headers_buf: [4][2][]const u8 = undefined;
    var n_headers: usize = 0;
    var auth_header: ?[]u8 = null;
    var range_header: ?[]u8 = null;
    defer if (auth_header) |auth| allocator.free(auth);
    defer if (range_header) |range| allocator.free(range);
    headers_buf[n_headers] = .{ "Connection", "close" };
    n_headers += 1;
    // Don't request gzip — binary model files (GGUF, ONNX, SafeTensors)
    // are incompressible and decompressing them on the fly tanks throughput.
    headers_buf[n_headers] = .{ "Accept-Encoding", "identity" };
    n_headers += 1;
    if (config.token) |token| {
        auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
        headers_buf[n_headers] = .{ "Authorization", auth_header.? };
        n_headers += 1;
    }

    const url = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/resolve/main/{s}", .{ config.base_url, owner, name, filename });
    defer allocator.free(url);

    // Create parent dirs if filename has slashes (e.g., "onnx/model.onnx")
    if (std.mem.lastIndexOfScalar(u8, filename, '/')) |last_slash| {
        const parent = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dest_dir, filename[0..last_slash] });
        defer allocator.free(parent);
        try std.Io.Dir.cwd().createDirPath(io, parent);
    }

    const temp_path = try std.fmt.allocPrint(allocator, "{s}.part", .{dest_path});
    defer allocator.free(temp_path);

    var resume_from = existingFileSize(dest, io, temp_path) catch 0;
    if (resume_from > config.max_artifact_bytes) {
        dest.deleteFile(io, temp_path) catch {};
        return error.DownloadSizeLimitExceeded;
    }
    // Without a stable content digest, a partial cannot be proven to belong
    // to the current revision—even when its size happens to equal the
    // advertised size. Small non-LFS sidecars are restarted instead of
    // risking a mixed-version artifact.
    if (resume_from > 0 and expected_sha256 == null) {
        dest.deleteFile(io, temp_path) catch {};
        resume_from = 0;
    }
    if (total_bytes) |total| {
        if (resume_from == total) {
            try finalizeDownloadedFile(dest, io, temp_path, dest_path, expected_sha256);
            return;
        }
        if (resume_from > total) {
            dest.deleteFile(io, temp_path) catch {};
            resume_from = 0;
        }
    }

    var client = httpx.Client.initWithConfig(
        allocator,
        io,
        downloadClientConfig(max_response_size),
    );
    defer client.deinit();

    const download_url = try resolveDownloadUrl(
        allocator,
        &client,
        url,
        headers_buf[0..n_headers],
        headers_buf[0..2],
    );
    defer allocator.free(download_url);

    while (true) {
        if (range_header) |range| {
            allocator.free(range);
            range_header = null;
        }
        n_headers = 0;
        headers_buf[n_headers] = .{ "Connection", "close" };
        n_headers += 1;
        headers_buf[n_headers] = .{ "Accept-Encoding", "identity" };
        n_headers += 1;
        if (auth_header) |auth| {
            if (sameHttpOrigin(url, download_url)) {
                headers_buf[n_headers] = .{ "Authorization", auth };
                n_headers += 1;
            }
        }
        if (resume_from > 0) {
            range_header = try std.fmt.allocPrint(allocator, "bytes={d}-", .{resume_from});
            headers_buf[n_headers] = .{ "Range", range_header.? };
            n_headers += 1;
        }

        var file = try dest.createFile(io, temp_path, .{ .truncate = resume_from == 0 });

        var resume_writer = OffsetFileWriter{
            .file = file,
            .io = io,
            .offset = resume_from,
            .max_offset = config.max_artifact_bytes,
        };

        var progress_ctx = FileProgressCtx{
            .progress = progress,
            .file = filename,
            .total_bytes = total_bytes,
            .files_done = file_index,
            .files_total = files_total,
            .base_offset = resume_from,
            .next_report_at = resume_from + progress_report_bytes,
        };

        if (resume_from > 0) {
            if (progress.callback) |cb| {
                cb(.{
                    .file = filename,
                    .bytes_downloaded = resume_from,
                    .total_bytes = total_bytes,
                    .files_done = file_index,
                    .files_total = files_total,
                }, progress.context);
            }
        }

        var streamed = client.getToWriter(download_url, .{
            .headers = headers_buf[0..n_headers],
            .follow_redirects = false,
        }, &resume_writer, FileProgressCtx.onWriterProgress, &progress_ctx) catch |err| {
            file.close(io);
            // A server that ignores Range can send a complete response after
            // the existing prefix, making the temporary file exceed the
            // artifact limit before its HTTP 200 status is available here.
            // Retry once without the stale prefix; a genuinely oversized
            // response will then fail at the same finite limit.
            if (err == error.DownloadSizeLimitExceeded or err == error.ResponseTooLarge) {
                dest.deleteFile(io, temp_path) catch {};
                if (resume_from > 0) {
                    resume_from = 0;
                    continue;
                }
            }
            return err;
        };

        if (!streamed.ok()) {
            std.debug.print("download failed for {s}: HTTP {d}\n", .{ filename, streamed.status.code });
            streamed.deinit();
            file.close(io);
            // The streaming client may already have written the error body.
            // Never preserve it as a resumable artifact prefix.
            dest.deleteFile(io, temp_path) catch {};
            if (resume_from > 0) {
                resume_from = 0;
                continue;
            }
            return error.DownloadFailed;
        }

        if (resume_from > 0 and streamed.status.code != 206) {
            streamed.deinit();
            file.close(io);
            dest.deleteFile(io, temp_path) catch {};
            resume_from = 0;
            continue;
        }
        if (streamed.status.code == 206 and !contentRangeMatches(
            streamed.header("Content-Range"),
            resume_from,
            resume_writer.offset,
            total_bytes,
        )) {
            streamed.deinit();
            file.close(io);
            dest.deleteFile(io, temp_path) catch {};
            if (resume_from > 0) {
                resume_from = 0;
                continue;
            }
            return error.InvalidContentRange;
        }

        if (total_bytes) |total| {
            if (resume_writer.offset != total) {
                streamed.deinit();
                file.close(io);
                if (resume_writer.offset > total) {
                    dest.deleteFile(io, temp_path) catch {};
                }
                return error.DownloadSizeMismatch;
            }
        }

        streamed.deinit();
        file.close(io);
        break;
    }

    try finalizeDownloadedFile(dest, io, temp_path, dest_path, expected_sha256);
}

fn finalizeDownloadedFile(
    dest: std.Io.Dir,
    io: std.Io,
    temp_path: []const u8,
    dest_path: []const u8,
    expected_sha256: ?[]const u8,
) !void {
    if (expected_sha256) |sum| {
        verifyFileSha256(dest, io, temp_path, sum) catch |err| {
            dest.deleteFile(io, temp_path) catch {};
            return err;
        };
    }
    {
        var file = try dest.openFile(io, temp_path, .{ .mode = .read_write });
        defer file.close(io);
        try file.sync(io);
    }
    // Dir.rename replaces an existing destination atomically. Do not unlink a
    // known-good model first: a failed rename must leave it intact.
    try std.Io.Dir.rename(dest, temp_path, dest, dest_path, io);
    try syncManagedDirectory(io, std.fs.path.dirname(dest_path) orelse ".");
}

fn existingFinalFileSatisfies(
    dir: std.Io.Dir,
    io: std.Io,
    path: []const u8,
    expected_sha256: ?[]const u8,
) !bool {
    dir.access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    if (expected_sha256) |sum| {
        verifyFileSha256(dir, io, path, sum) catch return false;
        return true;
    }
    // A size match alone is not an identity check. Re-fetch artifacts without
    // a repository digest so repeated pulls cannot silently retain stale
    // same-size metadata from a newer Hub revision.
    return false;
}

fn existingFileSize(dir: std.Io.Dir, io: std.Io, path: []const u8) !u64 {
    var file = dir.openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer file.close(io);
    const stat = try file.stat(io);
    return stat.size;
}

fn requiredFileSize(dir: std.Io.Dir, io: std.Io, path: []const u8) !u64 {
    var file = try dir.openFile(io, path, .{});
    defer file.close(io);
    return (try file.stat(io)).size;
}

fn verifyFileSha256(dir: std.Io.Dir, io: std.Io, path: []const u8, expected_hex: []const u8) !void {
    var file = try dir.openFile(io, path, .{});
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

    const actual_hex = std.fmt.bytesToHex(digest, .lower);

    if (expected_hex.len != 64) return error.ChecksumMismatch;
    if (!std.mem.eql(u8, &actual_hex, expected_hex)) {
        return error.ChecksumMismatch;
    }
}

test "appendMultiStageArtifacts selects multistage OCR payloads and sidecars" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "antfly_metadata.json" },
        .{ .name = "det.onnx" },
        .{ .name = "det.onnx.data" },
        .{ .name = "rec.onnx" },
        .{ .name = "ppocr_keys_v1.txt" },
        .{ .name = "nested/preprocessor_config.json" },
        .{ .name = "README.md" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendMultiStageArtifacts(allocator, &to_download, &files));

    try std.testing.expectEqual(@as(usize, 6), to_download.items.len);
    try std.testing.expect(std.mem.eql(u8, "antfly_metadata.json", to_download.items[0].name));
    try std.testing.expect(std.mem.eql(u8, "det.onnx", to_download.items[1].name));
    try std.testing.expect(std.mem.eql(u8, "det.onnx.data", to_download.items[2].name));
    try std.testing.expect(std.mem.eql(u8, "rec.onnx", to_download.items[3].name));
    try std.testing.expect(std.mem.eql(u8, "ppocr_keys_v1.txt", to_download.items[4].name));
    try std.testing.expect(std.mem.eql(u8, "nested/preprocessor_config.json", to_download.items[5].name));
}

test "appendPreexportedPaddleOCRArtifacts selects nested raw PaddleOCR assets" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "detection/v5/det.onnx" },
        .{ .name = "detection/v3/det.onnx" },
        .{ .name = "languages/latin/rec.onnx" },
        .{ .name = "languages/english/rec.onnx" },
        .{ .name = "languages/latin/dict.txt" },
        .{ .name = "languages/english/dict.txt" },
        .{ .name = "languages/english/config.json" },
        .{ .name = "detection/v3/config.json" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    const plan = try appendPreexportedPaddleOCRArtifacts(allocator, &to_download, &files);
    try std.testing.expect(plan != .none);
    try std.testing.expectEqual(@as(usize, 5), to_download.items.len);
    try std.testing.expectEqualStrings("detection/v3/det.onnx", to_download.items[0].name);
    try std.testing.expectEqualStrings("languages/english/rec.onnx", to_download.items[1].name);
    try std.testing.expectEqualStrings("languages/english/dict.txt", to_download.items[2].name);
    try std.testing.expectEqualStrings("detection/v3/config.json", to_download.items[3].name);
    try std.testing.expectEqualStrings("languages/english/config.json", to_download.items[4].name);
}

test "gguf selection skips projector and appends it as companion artifact" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-e2b-it-Q8_0.gguf" },
        .{ .name = "mmproj-gemma-4-e2b-it-f16.gguf" },
        .{ .name = "gemma-4-e2b-it-Q4_K_M.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestGgufFile(allocator, &to_download, &files, null));
    _ = try appendBestGgufProjectorFile(allocator, &to_download, &files);

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("gemma-4-e2b-it-Q4_K_M.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("mmproj-gemma-4-e2b-it-f16.gguf", to_download.items[1].name);
}

test "gguf selection handles google gemma4 e4b qat q4_0 layout" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-E4B-it-mmproj.gguf" },
        .{ .name = "gemma-4-E4B-q4_0ish-it.gguf" },
        .{ .name = "gemma-4-E4B_q4_0-it.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestRequestedGgufPayload(allocator, &to_download, &files, "Q4_0", .auto));

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("gemma-4-E4B_q4_0-it.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("gemma-4-E4B-it-mmproj.gguf", to_download.items[1].name);
}

test "gguf auto selection includes q4_0 qat model with trailing projector" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-E4B-it-mmproj.gguf" },
        .{ .name = "gemma-4-E4B_q4_0-it.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestRequestedGgufPayload(allocator, &to_download, &files, null, .auto));

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("gemma-4-E4B_q4_0-it.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("gemma-4-E4B-it-mmproj.gguf", to_download.items[1].name);
}

test "gguf selection prefers smaller q4_k variants by default" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-26B-A4B-it-Q4_K_M.gguf" },
        .{ .name = "gemma-4-26B-A4B-it-UD-Q4_K_M.gguf" },
        .{ .name = "gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf" },
        .{ .name = "mmproj-BF16.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestGgufFile(allocator, &to_download, &files, null));

    try std.testing.expectEqual(@as(usize, 1), to_download.items.len);
    try std.testing.expectEqualStrings("gemma-4-26B-A4B-it-Q4_K_M.gguf", to_download.items[0].name);
}

test "gguf selection keeps clipclap clip and clap pair together" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "clipclap-clip.Q4_K_M.gguf" },
        .{ .name = "clipclap-clap.Q4_K_M.gguf" },
        .{ .name = "clipclap-clip.Q4_K.gguf" },
        .{ .name = "clipclap-clap.Q4_K.gguf" },
        .{ .name = "clipclap-clip.Q8_0.gguf" },
        .{ .name = "clipclap-clap.Q8_0.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestClipclapGgufPair(allocator, &to_download, &files, "Q4_K"));

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("clipclap-clip.Q4_K.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("clipclap-clap.Q4_K.gguf", to_download.items[1].name);
}

test "payload support summary distinguishes framework-only repos" {
    const files = [_]HubFile{
        .{ .name = "config.json" },
        .{ .name = "pytorch_model.bin" },
        .{ .name = "tf_model.h5" },
        .{ .name = "flax_model.msgpack" },
    };

    const summary = summarizePayloadSupport(&files);
    try std.testing.expect(!summary.hasCompatiblePayload());
    try std.testing.expect(summary.has_framework_weights);
    try std.testing.expect(!summary.has_onnx);
    try std.testing.expect(!summary.has_safetensors);
    try std.testing.expect(!summary.has_gguf);
}

test "no model files advice recommends blessed clipclap ref for openai clip" {
    const files = [_]HubFile{
        .{ .name = "config.json" },
        .{ .name = "pytorch_model.bin" },
    };

    const advice = try noModelFilesAdviceAlloc(
        std.testing.allocator,
        "openai",
        "clip-vit-base-patch32",
        "hybrid",
        &files,
    );
    defer std.testing.allocator.free(advice);

    try std.testing.expect(std.mem.indexOf(u8, advice, "openai/clip-vit-base-patch32:hybrid") != null);
    try std.testing.expect(std.mem.indexOf(u8, advice, "framework weights only") != null);
    try std.testing.expect(std.mem.indexOf(u8, advice, "antflydb/clipclap:gguf:Q4_K") != null);
    try std.testing.expect(std.mem.indexOf(u8, advice, "Xenova/clip-vit-base-patch32:hybrid") != null);
}

test "gguf clipclap selection does not mix quantized pairs" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "clipclap-clip.Q4_K.gguf" },
        .{ .name = "clipclap-clap.Q4_K_M.gguf" },
        .{ .name = "clipclap-clip.Q8_0.gguf" },
        .{ .name = "clipclap-clap.Q8_0.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(!try appendBestClipclapGgufPair(allocator, &to_download, &files, "Q4_K"));
    try std.testing.expectEqual(@as(usize, 0), to_download.items.len);
}

test "gguf clipclap partial pair does not fall back to generic single file" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "clipclap-clip.Q4_K.gguf" },
        .{ .name = "unrelated.Q4_K.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(!try appendBestRequestedGgufPayload(allocator, &to_download, &files, "Q4_K", .auto));
    try std.testing.expectEqual(@as(usize, 0), to_download.items.len);
}

test "gguf selection keeps gliner encoder and head together" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "encoder.gguf" },
        .{ .name = "gliner_head.gguf" },
        .{ .name = "unrelated-Q4_K_M.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestRequestedGgufPayload(allocator, &to_download, &files, null, .auto));

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("encoder.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("gliner_head.gguf", to_download.items[1].name);
}

test "gguf gliner partial bundle does not fall back to generic single file" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "encoder.gguf" },
        .{ .name = "unrelated-Q4_K_M.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(!try appendBestRequestedGgufPayload(allocator, &to_download, &files, null, .auto));
    try std.testing.expectEqual(@as(usize, 0), to_download.items.len);
}

test "gguf selection keeps canonical gliner quantized pairs together" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gliner2-encoder.Q8_0.gguf" },
        .{ .name = "gliner2-head.Q8_0.gguf" },
        .{ .name = "gliner2-encoder.Q4_K.gguf" },
        .{ .name = "gliner2-head.Q4_K.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestRequestedGgufPayload(allocator, &to_download, &files, "Q4_K", .auto));

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("gliner2-encoder.Q4_K.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("gliner2-head.Q4_K.gguf", to_download.items[1].name);
}

test "projector-only selection finds mmproj gguf" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-e2b-it-Q4_K_M.gguf" },
        .{ .name = "nested/mmproj-gemma-4-e2b-it-q8_0.gguf" },
        .{ .name = "mmproj-gemma-4-e2b-it-f16.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestGgufProjectorFile(allocator, &to_download, &files));

    try std.testing.expectEqual(@as(usize, 1), to_download.items.len);
    try std.testing.expectEqualStrings("nested/mmproj-gemma-4-e2b-it-q8_0.gguf", to_download.items[0].name);
}

test "automatic projector selection prefers q8 while dense remains explicit" {
    const allocator = std.testing.allocator;
    const files = [_]HubFile{
        .{ .name = "mmproj-gemma-4-E2B-it-BF16.gguf", .size = 928 * 1024 * 1024 },
        .{ .name = "mmproj-gemma-4-E2B-it-Q8_0.gguf", .size = 480 * 1024 * 1024 },
    };

    var automatic = std.ArrayListUnmanaged(HubFile).empty;
    defer automatic.deinit(allocator);
    try std.testing.expect(try appendSelectedGgufProjectorFile(allocator, &automatic, &files, .auto));
    try std.testing.expectEqualStrings("mmproj-gemma-4-E2B-it-Q8_0.gguf", automatic.items[0].name);

    var explicit = std.ArrayListUnmanaged(HubFile).empty;
    defer explicit.deinit(allocator);
    try std.testing.expect(try appendSelectedGgufProjectorFile(allocator, &explicit, &files, .{ .match = "BF16" }));
    try std.testing.expectEqualStrings("mmproj-gemma-4-E2B-it-BF16.gguf", explicit.items[0].name);
}

test "gguf selection honors requested projector suffix" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-E4B-it-Q4_K_M.gguf" },
        .{ .name = "mmproj-gemma-4-E4B-it-bf16.gguf" },
        .{ .name = "mmproj-gemma-4-E4B-it-Q8_0.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestRequestedGgufPayload(allocator, &to_download, &files, "Q4_K_M", .{ .match = "Q8_0" }));

    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("gemma-4-E4B-it-Q4_K_M.gguf", to_download.items[0].name);
    try std.testing.expectEqualStrings("mmproj-gemma-4-E4B-it-Q8_0.gguf", to_download.items[1].name);
}

test "gguf selection can skip projector sidecar" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "gemma-4-E4B-it-Q4_K_M.gguf" },
        .{ .name = "mmproj-gemma-4-E4B-it-bf16.gguf" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendBestRequestedGgufPayload(allocator, &to_download, &files, "Q4_K_M", .none));

    try std.testing.expectEqual(@as(usize, 1), to_download.items.len);
    try std.testing.expectEqualStrings("gemma-4-E4B-it-Q4_K_M.gguf", to_download.items[0].name);
}

test "safetensors selection prefers model index and matching shards" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "model.safetensors" },
        .{ .name = "model.safetensors.index.json" },
        .{ .name = "model-00001-of-00002.safetensors" },
        .{ .name = "model-00002-of-00002.safetensors" },
        .{ .name = "pytorch_model-00001-of-00002.safetensors" },
        .{ .name = "nested/model-00001-of-00002.safetensors" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendPreferredSafetensorsPayload(allocator, &to_download, &files));

    try std.testing.expectEqual(@as(usize, 3), to_download.items.len);
    try std.testing.expectEqualStrings("model.safetensors.index.json", to_download.items[0].name);
    try std.testing.expectEqualStrings("model-00001-of-00002.safetensors", to_download.items[1].name);
    try std.testing.expectEqualStrings("model-00002-of-00002.safetensors", to_download.items[2].name);
}

test "safetensors selection includes pytorch index and shards" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "pytorch_model.safetensors.index.json" },
        .{ .name = "pytorch_model-00001-of-00003.safetensors" },
        .{ .name = "pytorch_model-00002-of-00003.safetensors" },
        .{ .name = "pytorch_model-00003-of-00003.safetensors" },
        .{ .name = "pytorch_model-final-of-00003.safetensors" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendPreferredSafetensorsPayload(allocator, &to_download, &files));

    try std.testing.expectEqual(@as(usize, 4), to_download.items.len);
    try std.testing.expectEqualStrings("pytorch_model.safetensors.index.json", to_download.items[0].name);
    try std.testing.expectEqualStrings("pytorch_model-00001-of-00003.safetensors", to_download.items[1].name);
    try std.testing.expectEqualStrings("pytorch_model-00002-of-00003.safetensors", to_download.items[2].name);
    try std.testing.expectEqualStrings("pytorch_model-00003-of-00003.safetensors", to_download.items[3].name);
}

test "safetensors selection falls back to single file without usable index" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "model.safetensors.index.json" },
        .{ .name = "model.safetensors" },
        .{ .name = "pytorch_model.safetensors" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    try std.testing.expect(try appendPreferredSafetensorsPayload(allocator, &to_download, &files));

    try std.testing.expectEqual(@as(usize, 1), to_download.items.len);
    try std.testing.expectEqualStrings("model.safetensors", to_download.items[0].name);
}

test "adapter artifact selection includes jina task adapter sidecars" {
    const allocator = std.testing.allocator;

    const files = [_]HubFile{
        .{ .name = "config.json" },
        .{ .name = "adapters/retrieval/adapter_config.json" },
        .{ .name = "adapters/retrieval/adapter_model.safetensors" },
        .{ .name = "adapters/retrieval/README.md" },
        .{ .name = "model.safetensors" },
    };

    var to_download = std.ArrayListUnmanaged(HubFile).empty;
    defer to_download.deinit(allocator);

    const count = try appendAdapterArtifacts(allocator, &to_download, &files);

    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqual(@as(usize, 2), to_download.items.len);
    try std.testing.expectEqualStrings("adapters/retrieval/adapter_config.json", to_download.items[0].name);
    try std.testing.expectEqualStrings("adapters/retrieval/adapter_model.safetensors", to_download.items[1].name);
}

test "verify file sha256 matches expected digest" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "payload.bin",
        .data = "hello world",
    });

    try verifyFileSha256(tmp.dir, std.testing.io, "payload.bin", "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
}

const python_download_server_script =
    "import pathlib\n" ++
    "import socketserver\n" ++
    "import sys\n" ++
    "from http.server import BaseHTTPRequestHandler\n" ++
    "\n" ++
    "port = int(sys.argv[1])\n" ++
    "mode = sys.argv[2]\n" ++
    "payload = pathlib.Path(sys.argv[3]).read_bytes()\n" ++
    "log_path = pathlib.Path(sys.argv[4])\n" ++
    "\n" ++
    "class Handler(BaseHTTPRequestHandler):\n" ++
    "    protocol_version = 'HTTP/1.1'\n" ++
    "    def do_GET(self):\n" ++
    "        rng = self.headers.get('Range')\n" ++
    "        with log_path.open('ab') as f:\n" ++
    "            f.write(((rng or '-') + '\\n').encode())\n" ++
    "        if mode == 'resume' and rng and rng.startswith('bytes='):\n" ++
    "            start = int(rng[6:].split('-', 1)[0])\n" ++
    "            body = payload[start:]\n" ++
    "            self.send_response(206)\n" ++
    "            self.send_header('Content-Length', str(len(body)))\n" ++
    "            self.send_header('Content-Range', f'bytes {start}-{len(payload)-1}/{len(payload)}')\n" ++
    "            self.send_header('Connection', 'close')\n" ++
    "            self.end_headers()\n" ++
    "            self.wfile.write(body)\n" ++
    "            return\n" ++
    "        if mode == 'range-error' and rng and rng.startswith('bytes='):\n" ++
    "            body = b'range rejected'\n" ++
    "            self.send_response(416)\n" ++
    "            self.send_header('Content-Length', str(len(body)))\n" ++
    "            self.send_header('Connection', 'close')\n" ++
    "            self.end_headers()\n" ++
    "            self.wfile.write(body)\n" ++
    "            return\n" ++
    "        if mode == 'wrong-range' and rng and rng.startswith('bytes='):\n" ++
    "            start = int(rng[6:].split('-', 1)[0])\n" ++
    "            body = payload[start:]\n" ++
    "            self.send_response(206)\n" ++
    "            self.send_header('Content-Length', str(len(body)))\n" ++
    "            self.send_header('Content-Range', f'bytes 0-{len(body)-1}/{len(payload)}')\n" ++
    "            self.send_header('Connection', 'close')\n" ++
    "            self.end_headers()\n" ++
    "            self.wfile.write(body)\n" ++
    "            return\n" ++
    "        self.send_response(200)\n" ++
    "        self.send_header('Content-Length', str(len(payload)))\n" ++
    "        self.send_header('Connection', 'close')\n" ++
    "        self.end_headers()\n" ++
    "        self.wfile.write(payload)\n" ++
    "    def log_message(self, fmt, *args):\n" ++
    "        pass\n" ++
    "\n" ++
    "class ReuseTCPServer(socketserver.TCPServer):\n" ++
    "    allow_reuse_address = True\n" ++
    "\n" ++
    "with ReuseTCPServer(('127.0.0.1', port), Handler) as httpd:\n" ++
    "    httpd.serve_forever()\n";

fn reserveEphemeralPort(io: std.Io) !u16 {
    const listen_addr = httpx.socket.Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try httpx.TcpListener.init(listen_addr, io);
    defer listener.deinit();
    return listener.getLocalAddress().ip4.port;
}

fn testTmpPath(allocator: std.mem.Allocator, tmp: anytype, tail: []const u8) ![]u8 {
    return std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], tail });
}

test "streaming model downloads use a finite disk-oriented response limit" {
    const response_limit = try downloadResponseLimit(.{ .max_artifact_bytes = 1024 });
    try std.testing.expectEqual(@as(usize, 17 * 1024), response_limit);
    try std.testing.expectEqual(
        response_limit,
        downloadClientConfig(response_limit).max_response_size,
    );
    try std.testing.expectError(
        error.InvalidDownloadSizeLimit,
        downloadResponseLimit(.{ .max_artifact_bytes = 0 }),
    );
    try std.testing.expectError(
        error.InvalidDownloadSizeLimit,
        downloadResponseLimit(.{ .max_artifact_bytes = std.math.maxInt(u64) }),
    );
}

test "aggregate model budget uses checked arithmetic" {
    try std.testing.expectEqual(
        @as(u64, 7),
        try addKnownModelBytes(3, 4, 7),
    );
    try std.testing.expectError(
        error.ModelSizeLimitExceeded,
        addKnownModelBytes(3, 5, 7),
    );
    try std.testing.expectError(
        error.ModelSizeLimitExceeded,
        addKnownModelBytes(std.math.maxInt(u64), 1, std.math.maxInt(u64)),
    );
    try std.testing.expectEqual(@as(u64, 3), try consumeModelBudget(7, 4));
    try std.testing.expectError(
        error.ModelSizeLimitExceeded,
        consumeModelBudget(7, 8),
    );
}

test "downloadFile rejects artifact paths outside the model root before network access" {
    try std.testing.expectError(
        error.InvalidModelArtifactPath,
        downloadFile(
            std.testing.allocator,
            std.testing.io,
            "owner",
            "model",
            "../escape.gguf",
            "unused",
            .{ .base_url = "http://127.0.0.1:1" },
            .{},
            0,
            1,
            null,
            null,
        ),
    );
}

test "managed download publication keeps incomplete state fail closed" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "managed");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = complete_path, .data = "old" });

    try beginManagedDownload(allocator, io, dest_dir);
    try std.testing.expectEqual(
        ManagedDownloadState.incomplete,
        managedDownloadState(allocator, io, dest_dir),
    );
    const in_progress_path = try managedPath(allocator, dest_dir, managed_download_in_progress_filename);
    defer allocator.free(in_progress_path);
    try std.Io.Dir.cwd().access(io, in_progress_path, .{});
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().openFile(io, complete_path, .{}),
    );

    const plan_path = try managedPath(allocator, dest_dir, managed_download_plan_filename);
    defer allocator.free(plan_path);
    const artifact_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.onnx" });
    defer allocator.free(artifact_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = artifact_path, .data = "payload" });
    try writeFileAtomically(
        allocator,
        io,
        plan_path,
        "{\"version\":1,\"artifacts\":[{\"path\":\"model.onnx\",\"size\":7}]}",
    );
    try completeManagedDownload(allocator, io, dest_dir);
    try std.testing.expectEqual(
        ManagedDownloadState.complete,
        managedDownloadState(allocator, io, dest_dir),
    );
    try std.Io.Dir.cwd().access(io, complete_path, .{});
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().openFile(io, in_progress_path, .{}),
    );

    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = complete_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"config.json\",\"size\":2}]}",
    });
    const config_path = try std.fs.path.join(allocator, &.{ dest_dir, "config.json" });
    defer allocator.free(config_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = config_path, .data = "{}" });
    try std.testing.expectEqual(
        ManagedDownloadState.incomplete,
        managedDownloadState(allocator, io, dest_dir),
    );
}

test "managed download receipt source identity is exact" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "managed-source");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const artifact_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(artifact_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = artifact_path, .data = "payload" });
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = complete_path,
        .data =
        \\{"version":2,"source":{"owner":"owner","name":"model","variant":"gguf:Q4_K_M"},"artifacts":[{"path":"model.gguf","size":7}]}
        ,
    });

    try std.testing.expect(managedDownloadMatchesSource(allocator, io, dest_dir, "owner", "model", "gguf:Q4_K_M"));
    try std.testing.expect(!managedDownloadMatchesSource(allocator, io, dest_dir, "owner", "model", "gguf:Q8_0"));
}

test "managed staging artifact replacement updates the publication receipt" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "managed-update");
    defer allocator.free(dest_dir);
    try beginManagedDownload(allocator, io, dest_dir);
    const decoder_path = try managedPath(allocator, dest_dir, "model.gguf");
    defer allocator.free(decoder_path);
    try writeFileAtomically(allocator, io, decoder_path, "decoder");
    const plan_path = try managedPath(allocator, dest_dir, managed_download_plan_filename);
    defer allocator.free(plan_path);
    try writeFileAtomically(
        allocator,
        io,
        plan_path,
        "{\"version\":2,\"source\":{\"owner\":\"owner\",\"name\":\"model\",\"variant\":\"gguf:Q4_K_M\"},\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}",
    );

    const manifest_json = "{\"type\":\"generator\"}";
    try writeManagedArtifactAndUpdatePlan(
        allocator,
        io,
        dest_dir,
        "model_manifest.json",
        manifest_json,
    );
    var plan = try managed_receipt.loadValidatedPlan(allocator, io, dest_dir);
    defer plan.deinit();
    const manifest_artifact = plan.find("model_manifest.json") orelse
        return error.TestExpectedManifestArtifact;
    try std.testing.expectEqual(@as(u64, manifest_json.len), manifest_artifact.size);
    try std.testing.expectEqual(@as(u32, 2), plan.parsed.value.version);
    try std.testing.expectEqualStrings("gguf:Q4_K_M", plan.parsed.value.source.?.variant);

    try completeManagedDownload(allocator, io, dest_dir);
    try std.testing.expectEqual(ManagedDownloadState.complete, managedDownloadState(allocator, io, dest_dir));
    try std.testing.expect(managedDownloadMatchesSource(allocator, io, dest_dir, "owner", "model", "gguf:Q4_K_M"));
}

test "managed model transaction reuses artifacts and publishes completed repair" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "transactional-model");
    defer allocator.free(dest_dir);
    try cwd.createDirPath(io, dest_dir);
    const decoder_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(decoder_path);
    try cwd.writeFile(io, .{ .sub_path = decoder_path, .data = "decoder" });
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);
    try cwd.writeFile(io, .{
        .sub_path = complete_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}",
    });
    try std.testing.expectEqual(ManagedDownloadState.complete, managedDownloadState(allocator, io, dest_dir));

    var transaction = try ManagedModelTransaction.begin(allocator, io, dest_dir);
    defer transaction.deinit(io);
    const staged_decoder_path = try std.fs.path.join(allocator, &.{ transaction.staging, "model.gguf" });
    defer allocator.free(staged_decoder_path);
    var published_decoder = try cwd.openFile(io, decoder_path, .{});
    defer published_decoder.close(io);
    var staged_decoder = try cwd.openFile(io, staged_decoder_path, .{});
    defer staged_decoder.close(io);
    try std.testing.expectEqual((try published_decoder.stat(io)).inode, (try staged_decoder.stat(io)).inode);

    const projector_path = try std.fs.path.join(allocator, &.{ transaction.staging, "nested/mmproj-model-Q8_0.gguf" });
    defer allocator.free(projector_path);
    try cwd.createDirPath(io, std.fs.path.dirname(projector_path).?);
    try cwd.writeFile(io, .{ .sub_path = projector_path, .data = "projector" });
    const plan_path = try managedPath(allocator, transaction.staging, managed_download_plan_filename);
    defer allocator.free(plan_path);
    try writeFileAtomically(
        allocator,
        io,
        plan_path,
        "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7},{\"path\":\"nested/mmproj-model-Q8_0.gguf\",\"size\":9}]}",
    );
    try completeManagedDownload(allocator, io, transaction.staging);
    try transaction.commit(io);

    try std.testing.expectEqual(ManagedDownloadState.complete, managedDownloadState(allocator, io, dest_dir));
    const published_projector_path = try std.fs.path.join(allocator, &.{ dest_dir, "nested/mmproj-model-Q8_0.gguf" });
    defer allocator.free(published_projector_path);
    try cwd.access(io, published_projector_path, .{});
}

test "managed model transaction never reuses receipt symlinks outside model root" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "symlink-model");
    defer allocator.free(dest_dir);
    try cwd.createDirPath(io, dest_dir);
    const outside_path = try testTmpPath(allocator, tmp, "outside.gguf");
    defer allocator.free(outside_path);
    try cwd.writeFile(io, .{ .sub_path = outside_path, .data = "outside" });
    const decoder_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(decoder_path);
    try cwd.symLink(io, "../outside.gguf", decoder_path, .{});
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);
    try cwd.writeFile(io, .{
        .sub_path = complete_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}",
    });

    try std.testing.expectEqual(ManagedDownloadState.incomplete, managedDownloadState(allocator, io, dest_dir));
    var transaction = try ManagedModelTransaction.begin(allocator, io, dest_dir);
    defer transaction.deinit(io);
    const staged_decoder_path = try std.fs.path.join(allocator, &.{ transaction.staging, "model.gguf" });
    defer allocator.free(staged_decoder_path);
    try std.testing.expectError(error.FileNotFound, cwd.openFile(io, staged_decoder_path, .{}));
}

test "managed model transaction failure preserves last known good cache" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "failed-transactional-model");
    defer allocator.free(dest_dir);
    try cwd.createDirPath(io, dest_dir);
    const decoder_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(decoder_path);
    try cwd.writeFile(io, .{ .sub_path = decoder_path, .data = "known-good" });
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);
    try cwd.writeFile(io, .{
        .sub_path = complete_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":10}]}",
    });

    var transaction = try ManagedModelTransaction.begin(allocator, io, dest_dir);
    const staged_decoder_path = try std.fs.path.join(allocator, &.{ transaction.staging, "model.gguf" });
    defer allocator.free(staged_decoder_path);
    const staged_replacement_path = try std.fmt.allocPrint(allocator, "{s}.part", .{staged_decoder_path});
    defer allocator.free(staged_replacement_path);
    try cwd.writeFile(io, .{ .sub_path = staged_replacement_path, .data = "replacement" });
    try std.Io.Dir.rename(cwd, staged_replacement_path, cwd, staged_decoder_path, io);
    const retained_staging_path = try allocator.dupe(u8, transaction.staging);
    defer allocator.free(retained_staging_path);
    transaction.deinit(io);

    try std.testing.expectEqual(ManagedDownloadState.complete, managedDownloadState(allocator, io, dest_dir));
    const decoder = try cwd.readFileAlloc(io, decoder_path, allocator, .limited(64));
    defer allocator.free(decoder);
    try std.testing.expectEqualStrings("known-good", decoder);

    var resumed = try ManagedModelTransaction.begin(allocator, io, dest_dir);
    defer resumed.deinit(io);
    try std.testing.expectEqualStrings(retained_staging_path, resumed.staging);
    const resumed_decoder_path = try std.fs.path.join(allocator, &.{ resumed.staging, "model.gguf" });
    defer allocator.free(resumed_decoder_path);
    const resumed_decoder = try cwd.readFileAlloc(io, resumed_decoder_path, allocator, .limited(64));
    defer allocator.free(resumed_decoder);
    try std.testing.expectEqualStrings("replacement", resumed_decoder);
}

test "managed model transaction recovers interrupted directory publication" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const cwd = std.Io.Dir.cwd();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "interrupted-transactional-model");
    defer allocator.free(dest_dir);
    try cwd.createDirPath(io, dest_dir);
    const decoder_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(decoder_path);
    try cwd.writeFile(io, .{ .sub_path = decoder_path, .data = "known-good" });
    const complete_path = try managedPath(allocator, dest_dir, managed_download_complete_filename);
    defer allocator.free(complete_path);
    try cwd.writeFile(io, .{
        .sub_path = complete_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":10}]}",
    });

    const backup_path = try managedSiblingPath(allocator, dest_dir, managed_download_backup_suffix);
    defer allocator.free(backup_path);
    const stale_staging_path = try managedSiblingPath(allocator, dest_dir, managed_download_staging_suffix);
    defer allocator.free(stale_staging_path);
    try std.Io.Dir.rename(cwd, dest_dir, cwd, backup_path, io);
    try cwd.createDirPath(io, stale_staging_path);
    const partial_path = try std.fs.path.join(allocator, &.{ stale_staging_path, "model.gguf.part" });
    defer allocator.free(partial_path);
    try cwd.writeFile(io, .{ .sub_path = partial_path, .data = "partial" });

    var transaction = try ManagedModelTransaction.begin(allocator, io, dest_dir);
    defer transaction.deinit(io);
    try std.testing.expectEqual(ManagedDownloadState.complete, managedDownloadState(allocator, io, dest_dir));
    try std.testing.expectError(error.FileNotFound, cwd.openDir(io, backup_path, .{}));
    try cwd.access(io, partial_path, .{});
    try std.testing.expectEqualStrings(stale_staging_path, transaction.staging);
    const staged_decoder_path = try std.fs.path.join(allocator, &.{ transaction.staging, "model.gguf" });
    defer allocator.free(staged_decoder_path);
    try cwd.access(io, staged_decoder_path, .{});
}

test "content range validation requires exact resume boundaries" {
    try std.testing.expect(contentRangeMatches("bytes 6-18/19", 6, 19, 19));
    try std.testing.expect(!contentRangeMatches("bytes 0-12/19", 6, 19, 19));
    try std.testing.expect(!contentRangeMatches("bytes 6-17/19", 6, 19, 19));
    try std.testing.expect(!contentRangeMatches("bytes 6-18/20", 6, 19, 19));
    try std.testing.expect(!contentRangeMatches("bytes 6-17/19", 6, 18, null));
    try std.testing.expect(!contentRangeMatches(null, 6, 19, 19));
}

test "download size probe ignores redirect response body length" {
    try std.testing.expectEqual(
        @as(?u64, 4096),
        chooseProbedDownloadSize(false, true, "4096", 128),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        chooseProbedDownloadSize(false, true, null, 128),
    );
    try std.testing.expectEqual(
        @as(?u64, 4096),
        chooseProbedDownloadSize(true, false, null, 4096),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        chooseProbedDownloadSize(false, false, "4096", 128),
    );
}

test "redirect origin policy protects authorization and transport" {
    try std.testing.expect(sameHttpOrigin(
        "https://huggingface.co/owner/model",
        "https://HUGGINGFACE.CO:443/redirected",
    ));
    try std.testing.expect(!sameHttpOrigin(
        "https://huggingface.co/owner/model",
        "https://cdn.example/model",
    ));
    try std.testing.expect(!sameHttpOrigin(
        "https://huggingface.co/owner/model",
        "https://huggingface.co:8443/model",
    ));
    try std.testing.expect(redirectDowngradesTransport(
        "https://huggingface.co/owner/model",
        "http://huggingface.co/model",
    ));
    try std.testing.expect(!redirectDowngradesTransport(
        "http://127.0.0.1:8080/model",
        "http://127.0.0.1:8081/model",
    ));
}

test "downloadFile rejects artifacts above the configured disk limit before network access" {
    try std.testing.expectError(
        error.DownloadSizeLimitExceeded,
        downloadFile(
            std.testing.allocator,
            std.testing.io,
            "owner",
            "name",
            "model.onnx",
            ".",
            .{
                .base_url = "http://127.0.0.1:1",
                .max_artifact_bytes = 128,
            },
            .{},
            0,
            1,
            129,
            null,
        ),
    );
}

test "downloadFile removes an oversized partial before network access" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const part_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.onnx.part" });
    defer allocator.free(part_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = part_path, .data = "oversized" });

    try std.testing.expectError(
        error.DownloadSizeLimitExceeded,
        downloadFile(
            allocator,
            io,
            "owner",
            "name",
            "model.onnx",
            dest_dir,
            .{
                .base_url = "http://127.0.0.1:1",
                .max_artifact_bytes = 4,
            },
            .{},
            0,
            1,
            null,
            null,
        ),
    );
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().openFile(io, part_path, .{}),
    );
}

test "downloadFile removes a fresh partial after streamed size overflow" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = "oversized payload" });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_download_server_script });
    try tmp.dir.writeFile(io, .{ .sub_path = "requests.log", .data = "" });

    const port = try reserveEphemeralPort(io);
    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});
    var child = try std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", port_arg, "ignore", "payload.bin", "requests.log" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    defer child.kill(io);
    io.sleep(std.Io.Duration.fromMilliseconds(200), .awake) catch {};

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const base_url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
    defer allocator.free(base_url);

    try std.testing.expectError(
        error.DownloadSizeLimitExceeded,
        downloadFile(
            allocator,
            io,
            "owner",
            "name",
            "model.onnx",
            dest_dir,
            .{
                .base_url = base_url,
                .max_artifact_bytes = 4,
            },
            .{},
            0,
            1,
            null,
            null,
        ),
    );
    const part_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.onnx.part" });
    defer allocator.free(part_path);
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().openFile(io, part_path, .{}),
    );
}

test "downloadFile verifies a complete partial before installing it" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const part_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.onnx.part" });
    defer allocator.free(part_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = part_path, .data = "bad" });
    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.onnx" });
    defer allocator.free(final_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = final_path, .data = "known-good" });

    try std.testing.expectError(
        error.ChecksumMismatch,
        downloadFile(
            allocator,
            io,
            "owner",
            "name",
            "model.onnx",
            dest_dir,
            .{ .base_url = "http://127.0.0.1:1" },
            .{},
            0,
            1,
            3,
            "0000000000000000000000000000000000000000000000000000000000000000",
        ),
    );
    try std.testing.expectError(
        error.FileNotFound,
        std.Io.Dir.cwd().openFile(io, part_path, .{}),
    );
    var final_file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer final_file.close(io);
    var final_buf: [16]u8 = undefined;
    const final_len = try final_file.readStreaming(io, &.{final_buf[0..]});
    try std.testing.expectEqualStrings("known-good", final_buf[0..final_len]);
}

test "offset file writer enforces the final artifact size across resumed writes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file = try tmp.dir.createFile(std.testing.io, "bounded.bin", .{ .read = true });
    defer file.close(std.testing.io);
    var writer = OffsetFileWriter{
        .file = file,
        .io = std.testing.io,
        .offset = 3,
        .max_offset = 5,
    };
    try writer.writeAll("ok");
    try std.testing.expectEqual(@as(u64, 5), writer.offset);
    try std.testing.expectError(error.DownloadSizeLimitExceeded, writer.writeAll("!"));
    try std.testing.expectEqual(@as(u64, 5), writer.offset);
}

test "downloadFile resumes from partial file with 206 response" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "hello resumed world";
    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = payload });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_download_server_script });
    try tmp.dir.writeFile(io, .{ .sub_path = "requests.log", .data = "" });

    const port = try reserveEphemeralPort(io);
    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", port_arg, "resume", "payload.bin", "requests.log" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);
    io.sleep(std.Io.Duration.fromMilliseconds(200), .awake) catch {};

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const partial_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json.part" });
    defer allocator.free(partial_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = partial_path, .data = payload[0..6] });

    const base_url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
    defer allocator.free(base_url);
    try downloadFile(allocator, io, "owner", "name", "tokenizer.json", dest_dir, .{
        .base_url = base_url,
    }, .{}, 0, 1, payload.len, "f6b6d844d9e70a622a4b1f2eab9cd77aa2b09280930adc897fad746fdf2c6a1c");

    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json" });
    defer allocator.free(final_path);
    var file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer file.close(io);
    var buf: [64]u8 = undefined;
    const n = try file.readStreaming(io, &.{buf[0..]});
    try std.testing.expectEqualStrings(payload, buf[0..n]);

    const log_path = try testTmpPath(allocator, tmp, "requests.log");
    defer allocator.free(log_path);
    var log_file = try std.Io.Dir.cwd().openFile(io, log_path, .{});
    defer log_file.close(io);
    var log_buf: [128]u8 = undefined;
    const log_n = try log_file.readStreaming(io, &.{log_buf[0..]});
    try std.testing.expect(std.mem.indexOf(u8, log_buf[0..log_n], "bytes=6-") != null);
}

test "downloadFile restarts when a 206 content range is inconsistent" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "hello resumed world";
    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = payload });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_download_server_script });
    try tmp.dir.writeFile(io, .{ .sub_path = "requests.log", .data = "" });

    const port = try reserveEphemeralPort(io);
    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", port_arg, "wrong-range", "payload.bin", "requests.log" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);
    io.sleep(std.Io.Duration.fromMilliseconds(200), .awake) catch {};

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const partial_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json.part" });
    defer allocator.free(partial_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = partial_path, .data = payload[0..6] });

    const base_url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
    defer allocator.free(base_url);
    try downloadFile(allocator, io, "owner", "name", "tokenizer.json", dest_dir, .{
        .base_url = base_url,
    }, .{}, 0, 1, payload.len, "f6b6d844d9e70a622a4b1f2eab9cd77aa2b09280930adc897fad746fdf2c6a1c");

    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json" });
    defer allocator.free(final_path);
    var file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer file.close(io);
    var buf: [64]u8 = undefined;
    const n = try file.readStreaming(io, &.{buf[0..]});
    try std.testing.expectEqualStrings(payload, buf[0..n]);

    const log_path = try testTmpPath(allocator, tmp, "requests.log");
    defer allocator.free(log_path);
    var log_file = try std.Io.Dir.cwd().openFile(io, log_path, .{});
    defer log_file.close(io);
    var log_buf: [128]u8 = undefined;
    const log_n = try log_file.readStreaming(io, &.{log_buf[0..]});
    try std.testing.expect(std.mem.indexOf(u8, log_buf[0..log_n], "bytes=6-") != null);
    try std.testing.expect(std.mem.endsWith(u8, log_buf[0..log_n], "-\n"));
}

test "downloadFile discards a streamed range error before retrying fresh" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "hello resumed world";
    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = payload });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_download_server_script });
    try tmp.dir.writeFile(io, .{ .sub_path = "requests.log", .data = "" });

    const port = try reserveEphemeralPort(io);
    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});
    var child = try std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", port_arg, "range-error", "payload.bin", "requests.log" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    defer child.kill(io);
    io.sleep(std.Io.Duration.fromMilliseconds(200), .awake) catch {};

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const partial_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json.part" });
    defer allocator.free(partial_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = partial_path, .data = payload[0..6] });

    const base_url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
    defer allocator.free(base_url);
    try downloadFile(allocator, io, "owner", "name", "tokenizer.json", dest_dir, .{
        .base_url = base_url,
    }, .{}, 0, 1, payload.len, "f6b6d844d9e70a622a4b1f2eab9cd77aa2b09280930adc897fad746fdf2c6a1c");

    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json" });
    defer allocator.free(final_path);
    var file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer file.close(io);
    var buf: [64]u8 = undefined;
    const n = try file.readStreaming(io, &.{buf[0..]});
    try std.testing.expectEqualStrings(payload, buf[0..n]);
}

test "downloadFile skips existing complete destination before network" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "already downloaded";
    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(final_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = final_path, .data = payload });

    try downloadFile(allocator, io, "owner", "name", "model.gguf", dest_dir, .{
        .base_url = "http://127.0.0.1:1",
    }, .{}, 0, 1, payload.len, "5d4601650452897026e60f1d6996d5b2941f3e3634ffebe6cc11458508c756f2");

    const part_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf.part" });
    defer allocator.free(part_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, part_path, .{}));

    var file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer file.close(io);
    var buf: [64]u8 = undefined;
    const n = try file.readStreaming(io, &.{buf[0..]});
    try std.testing.expectEqualStrings(payload, buf[0..n]);
}

test "downloadFile skips existing large artifact with pointer-sized metadata" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = [_]u8{'G'} ** 8192;
    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(final_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = final_path, .data = &payload });

    try downloadFile(allocator, io, "owner", "name", "model.gguf", dest_dir, .{
        .base_url = "http://127.0.0.1:1",
    }, .{}, 0, 1, 102, "8fc662ff2c1d2293998c59eff63476a828c715345d33d0219a1260be634422d1");

    const part_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf.part" });
    defer allocator.free(part_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, part_path, .{}));

    var file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer file.close(io);
    const stat = try file.stat(io);
    try std.testing.expectEqual(@as(u64, payload.len), stat.size);
}

test "tiny declared sizes are reprobed for large binary model artifacts" {
    try std.testing.expect(shouldProbeLinkedPayloadSize("model.gguf", 102));
    try std.testing.expect(shouldProbeLinkedPayloadSize("model.safetensors", 102));
    try std.testing.expect(!shouldProbeLinkedPayloadSize("README.md", 102));
    try std.testing.expect(!shouldProbeLinkedPayloadSize("model.gguf", 10 * 1024));
}

test "existing final file progress uses real size for pointer-sized artifacts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = [_]u8{'G'} ** 8192;
    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "model.gguf" });
    defer allocator.free(final_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = final_path, .data = &payload });

    try std.testing.expectEqual(
        @as(?u64, payload.len),
        try existingFinalFileProgressSize(allocator, io, dest_dir, "model.gguf", 102),
    );
    try std.testing.expectEqual(
        @as(?u64, 102),
        try existingFinalFileProgressSize(allocator, io, dest_dir, "README.md", 102),
    );
}

test "downloadFile restarts cleanly when range is ignored" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "range ignored payload";
    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = payload });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_download_server_script });
    try tmp.dir.writeFile(io, .{ .sub_path = "requests.log", .data = "" });

    const port = try reserveEphemeralPort(io);
    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", port_arg, "ignore", "payload.bin", "requests.log" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);
    io.sleep(std.Io.Duration.fromMilliseconds(200), .awake) catch {};

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);
    const partial_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json.part" });
    defer allocator.free(partial_path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = partial_path, .data = payload[0..5] });

    const base_url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
    defer allocator.free(base_url);
    try downloadFile(allocator, io, "owner", "name", "tokenizer.json", dest_dir, .{
        .base_url = base_url,
        // Force the bounded writer to detect the ignored Range response before
        // getToWriter can return its HTTP 200 status.
        .max_artifact_bytes = payload.len,
    }, .{}, 0, 1, payload.len, "b237163979797d22f20311e364eb817ae436427249a000f222051f65b6fcadc9");

    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json" });
    defer allocator.free(final_path);
    var file = try std.Io.Dir.cwd().openFile(io, final_path, .{});
    defer file.close(io);
    var buf: [64]u8 = undefined;
    const n = try file.readStreaming(io, &.{buf[0..]});
    try std.testing.expectEqualStrings(payload, buf[0..n]);

    const log_path = try testTmpPath(allocator, tmp, "requests.log");
    defer allocator.free(log_path);
    var log_file = try std.Io.Dir.cwd().openFile(io, log_path, .{});
    defer log_file.close(io);
    var log_buf: [128]u8 = undefined;
    const log_n = try log_file.readStreaming(io, &.{log_buf[0..]});
    try std.testing.expect(std.mem.indexOf(u8, log_buf[0..log_n], "bytes=5-") != null);
    try std.testing.expect(std.mem.endsWith(u8, log_buf[0..log_n], "-\n"));
}

test "downloadFile deletes partial file on checksum mismatch" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const payload = "checksum payload";
    try tmp.dir.writeFile(io, .{ .sub_path = "payload.bin", .data = payload });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_download_server_script });
    try tmp.dir.writeFile(io, .{ .sub_path = "requests.log", .data = "" });

    const port = try reserveEphemeralPort(io);
    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{ "python3", "server.py", port_arg, "ignore", "payload.bin", "requests.log" },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);
    io.sleep(std.Io.Duration.fromMilliseconds(200), .awake) catch {};

    const dest_dir = try testTmpPath(allocator, tmp, "downloads");
    defer allocator.free(dest_dir);
    try std.Io.Dir.cwd().createDirPath(io, dest_dir);

    const base_url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}", .{port});
    defer allocator.free(base_url);
    try std.testing.expectError(error.ChecksumMismatch, downloadFile(allocator, io, "owner", "name", "tokenizer.json", dest_dir, .{
        .base_url = base_url,
    }, .{}, 0, 1, payload.len, "0000000000000000000000000000000000000000000000000000000000000000"));

    const final_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json" });
    defer allocator.free(final_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, final_path, .{}));
    const part_path = try std.fs.path.join(allocator, &.{ dest_dir, "tokenizer.json.part" });
    defer allocator.free(part_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, part_path, .{}));
}
