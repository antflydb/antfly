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

pub const HubConfig = struct {
    /// HuggingFace Hub API token (optional, for private/gated models).
    token: ?[]const u8 = null,
    /// Base URL for the Hub API.
    base_url: []const u8 = "https://huggingface.co",
};

const HubFile = struct {
    name: []const u8,
    size: ?u64 = null,
    sha256: ?[]const u8 = null,
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
    "model_manifest.json",
    "termite_metadata.json",
    "termite_variants.json",
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
    "Q5_K_S",
    "Q5_K_M",
    "Q6_K",
    "Q8_0",
    "Q3_K_M",
    "Q2_K",
};

/// Preferred external multimodal projector GGUF payloads, best quality first.
const gguf_projector_preference = [_][]const u8{
    "f16", "bf16", "F16", "BF16", "Q8_0", "q8_0", "Q6_K", "q6_k", "Q5_K_M", "q5_k_m", "Q4_K_M", "q4_k_m",
};

fn basename(path: []const u8) []const u8 {
    if (std.mem.lastIndexOfScalar(u8, path, '/')) |slash| return path[slash + 1 ..];
    return path;
}

fn isGgufFile(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".gguf");
}

fn isGgufProjectorFile(path: []const u8) bool {
    if (!isGgufFile(path)) return false;
    const base = basename(path);
    return std.mem.eql(u8, base, "mmproj.gguf") or
        std.mem.startsWith(u8, base, "mmproj-") or
        std.mem.startsWith(u8, base, "mmproj_");
}

fn ggufQuantSuffixMatches(path: []const u8, quant: []const u8) bool {
    if (!isGgufFile(path)) return false;
    const base = basename(path);
    const ext = ".gguf";
    if (base.len <= ext.len or !std.mem.endsWith(u8, base, ext)) return false;
    const stem = base[0 .. base.len - ext.len];
    if (!std.mem.endsWith(u8, stem, quant)) return false;
    const start = stem.len - quant.len;
    if (start == 0) return true;
    return switch (stem[start - 1]) {
        '-', '_', '.' => true,
        else => false,
    };
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
) !bool {
    const has_clipclap_gguf = hasClipclapGgufCandidate(files);
    if (try appendBestClipclapGgufPair(allocator, to_download, files, quant_filter)) {
        return true;
    }
    if (has_clipclap_gguf) return false;
    if (try appendBestGgufFile(allocator, to_download, files, quant_filter)) {
        _ = try appendBestGgufProjectorFile(allocator, to_download, files);
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
    if (!hasFile(files, "termite_metadata.json")) return false;

    _ = try appendMatchingFileIfMissing(allocator, to_download, files, "termite_metadata.json");

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
    if (hasFile(files, "termite_metadata.json")) return .none;

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
) !void {
    switch (plan) {
        .none => {},
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

            const metadata_path = try std.fmt.allocPrint(allocator, "{s}/termite_metadata.json", .{dest_dir});
            defer allocator.free(metadata_path);
            try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = metadata_path, .data = metadata });
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

    pub fn writeAll(self: *OffsetFileWriter, data: []const u8) !void {
        try self.file.writePositionalAll(self.io, data, self.offset);
        self.offset += data.len;
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

/// Download a model from HuggingFace Hub.
pub fn downloadModel(
    allocator: std.mem.Allocator,
    io: std.Io,
    owner: []const u8,
    name: []const u8,
    variant: []const u8,
    dest_dir: []const u8,
    config: HubConfig,
    progress: ProgressSink,
) !void {
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

    const want_mmproj = std.mem.eql(u8, variant, "mmproj") or std.mem.eql(u8, variant, "projector");

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
        if (try appendBestRequestedGgufPayload(allocator, &to_download, files, quant_filter)) {
            found_model_payload = true;
        }
    }

    // External multimodal projector only.
    if (want_mmproj) {
        if (try appendBestGgufProjectorFile(allocator, &to_download, files))
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
        return error.NoModelFilesFound;
    }

    // Download each file
    for (to_download.items, 0..) |file_meta, i| {
        const filename = file_meta.name;
        const total_bytes = file_meta.size orelse (probeDownloadSize(allocator, io, owner, name, filename, config) catch null);
        if (progress.callback) |cb| {
            cb(.{
                .file = filename,
                .bytes_downloaded = 0,
                .total_bytes = total_bytes,
                .files_done = i,
                .files_total = to_download.items.len,
            }, progress.context);
        }

        try downloadFile(allocator, io, owner, name, filename, dest_dir, config, progress, i, to_download.items.len, total_bytes, file_meta.sha256);

        if (progress.callback) |cb| {
            cb(.{
                .file = filename,
                .bytes_downloaded = total_bytes orelse 0,
                .total_bytes = total_bytes,
                .files_done = i + 1,
                .files_total = to_download.items.len,
            }, progress.context);
        }
    }

    try writeSyntheticMetadata(allocator, io, dest_dir, synthetic_metadata);
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

    if (!resp.ok() and !resp.isRedirect()) return null;
    if (resp.header("x-linked-size")) |value| {
        return std.fmt.parseInt(u64, value, 10) catch null;
    }
    return resp.contentLength();
}

/// List all files in a HuggingFace model repo.
fn listModelFiles(
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

fn resolveDownloadUrl(
    allocator: std.mem.Allocator,
    client: *httpx.Client,
    start_url: []const u8,
    headers: []const [2][]const u8,
) ![]u8 {
    var current_url = try allocator.dupe(u8, start_url);
    errdefer allocator.free(current_url);

    var redirects: u32 = 0;
    while (true) {
        var resp = try client.request(.HEAD, current_url, .{
            .headers = headers,
            .follow_redirects = false,
        });
        defer resp.deinit();

        if (!resp.isRedirect()) return current_url;
        if (redirects >= 10) return error.TooManyRedirects;

        const location = resp.header("Location") orelse return error.InvalidResponse;
        const next_url = try resolveRedirectUrl(allocator, current_url, location);
        allocator.free(current_url);
        current_url = next_url;
        redirects += 1;
    }
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
    const url = try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/resolve/main/{s}", .{ config.base_url, owner, name, filename });
    defer allocator.free(url);

    var client = httpx.Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
    });
    defer client.deinit();

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

    const download_url = try resolveDownloadUrl(allocator, &client, url, headers_buf[0..n_headers]);
    defer allocator.free(download_url);

    // Create parent dirs if filename has slashes (e.g., "onnx/model.onnx")
    if (std.mem.lastIndexOfScalar(u8, filename, '/')) |last_slash| {
        const parent = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dest_dir, filename[0..last_slash] });
        defer allocator.free(parent);
        try std.Io.Dir.cwd().createDirPath(io, parent);
    }

    var dest = std.Io.Dir.cwd();
    const dest_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dest_dir, filename });
    defer allocator.free(dest_path);
    const temp_path = try std.fmt.allocPrint(allocator, "{s}.part", .{dest_path});
    defer allocator.free(temp_path);

    var resume_from = existingFileSize(dest, io, temp_path) catch 0;
    if (total_bytes) |total| {
        if (resume_from >= total) {
            try std.Io.Dir.rename(dest, temp_path, dest, dest_path, io);
            return;
        }
    }

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
            headers_buf[n_headers] = .{ "Authorization", auth };
            n_headers += 1;
        }
        if (resume_from > 0) {
            range_header = try std.fmt.allocPrint(allocator, "bytes={d}-", .{resume_from});
            headers_buf[n_headers] = .{ "Range", range_header.? };
            n_headers += 1;
        }

        var file = try dest.createFile(io, temp_path, .{ .truncate = resume_from == 0 });
        defer file.close(io);

        var resume_writer = OffsetFileWriter{
            .file = file,
            .io = io,
            .offset = resume_from,
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

        var streamed = try client.getToWriter(download_url, .{
            .headers = headers_buf[0..n_headers],
            .follow_redirects = false,
        }, &resume_writer, FileProgressCtx.onWriterProgress, &progress_ctx);
        defer streamed.deinit();

        if (!streamed.ok()) {
            std.debug.print("download failed for {s}: HTTP {d}\n", .{ filename, streamed.status.code });
            return error.DownloadFailed;
        }

        if (resume_from > 0 and streamed.status.code != 206) {
            dest.deleteFile(io, temp_path) catch {};
            resume_from = 0;
            continue;
        }

        break;
    }

    if (expected_sha256) |sum| {
        verifyFileSha256(dest, io, temp_path, sum) catch |err| {
            dest.deleteFile(io, temp_path) catch {};
            return err;
        };
    }

    try std.Io.Dir.rename(dest, temp_path, dest, dest_path, io);
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
        .{ .name = "termite_metadata.json" },
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
    try std.testing.expect(std.mem.eql(u8, "termite_metadata.json", to_download.items[0].name));
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

    try std.testing.expect(!try appendBestRequestedGgufPayload(allocator, &to_download, &files, "Q4_K"));
    try std.testing.expectEqual(@as(usize, 0), to_download.items.len);
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
    try std.testing.expectEqualStrings("mmproj-gemma-4-e2b-it-f16.gguf", to_download.items[0].name);
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
    }, .{}, 0, 1, payload.len, null);

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
    }, .{}, 0, 1, payload.len, null);

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
