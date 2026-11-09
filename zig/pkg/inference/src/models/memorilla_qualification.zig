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
fn pythonWhitespace(cp: u21) bool {
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

pub fn requireCudaExecutionStats(stats: anytype) !void {
    @setEvalBranchQuota(100_000);
    inline for (@typeInfo(@TypeOf(stats)).@"struct".field_names) |name| {
        if (comptime std.mem.startsWith(u8, name, "bf16_") or std.mem.startsWith(u8, name, "f16_") or
            std.mem.eql(u8, name, "rms_norm_bf16_mirror_hits") or std.mem.eql(u8, name, "fused_gate_up_bf16_hits"))
        {
            if (@field(stats, name) != 0) return error.UnsupportedMemorillaPrecision;
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

pub const Profile = enum { qwen3_encoder_f32_v1, qwen3_decoder_f32_kv_f32_v1 };
pub const Role = enum { encoder, decoder };
pub const lock_bytes = @embedFile("memorilla_v1_base_models.json");
pub const max_text_tokens: usize = 256;
pub const max_documents: usize = 64;
pub const width: usize = 2560;
pub const cuda_kernel_workspace_bytes = @import("memorilla_constants.zig").cuda_kernel_workspace_bytes;
pub const encoder_weight_bytes: usize = 16_087_097_344;
// Covers widening the largest BF16 tensor, temporary upload storage and one
// bounded GEMM workspace while all mapped source pages remain admitted.
pub const decoder_weight_bytes: usize = 32_762_941_440;

pub const ResidencyPlan = struct { peak: memory.AdmissionAmounts, resident: memory.AdmissionAmounts };

pub fn residencyPlan(a: A, profile: Profile) !ResidencyPlan {
    const source = try sourceBytes(a, profile);
    const backend = switch (profile) {
        .qwen3_encoder_f32_v1 => encoder_weight_bytes,
        .qwen3_decoder_f32_kv_f32_v1 => decoder_weight_bytes,
    };
    // Retain a conservative charge for the complete clean source cache. The
    // loader never globally evicts pages potentially used by another owner.
    const resident = memory.AdmissionAmounts{ .host_weight_bytes = source, .backend_weight_bytes = backend, .backend_scratch_bytes = cuda_kernel_workspace_bytes };
    var peak = resident;
    peak.host_scratch_bytes = switch (profile) {
        .qwen3_encoder_f32_v1 => 2 * 1024 * 1024 * 1024,
        .qwen3_decoder_f32_kv_f32_v1 => 3 * 1024 * 1024 * 1024,
    };
    return .{ .peak = peak, .resident = resident };
}

pub fn role(profile: Profile) Role {
    return switch (profile) {
        .qwen3_encoder_f32_v1 => .encoder,
        .qwen3_decoder_f32_kv_f32_v1 => .decoder,
    };
}

/// Portable across install paths, distinct from a publication/lifetime pin.
/// Changing source, recipe, arithmetic, role or measured device domain changes
/// the producer identity and therefore cannot reuse another profile's vectors.
pub fn executionIdentity(profile: Profile) [64]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.memorilla.execution.v1\x00");
    hash.update(@tagName(profile));
    hash.update("\x00");
    hash.update(if (role(profile) == .encoder) catalog.encoder_manifest else catalog.decoder_manifest);
    hash.update("\x00");
    hash.update(lock_bytes);
    hash.update("\x00bf16-source/f32-resident/f32-activations\x00sm_121\x00");
    hash.update(question_recipe);
    return std.fmt.bytesToHex(hash.finalResult(), .lower);
}

pub fn requireRecipe(profile: Profile, manifest: anytype) !void {
    try requireRoleRecipe(profile, manifest);
    if (manifest.nativeWeightArtifactKind() != .sharded_safetensors)
        return error.UnsupportedMemorillaArtifact;
}

pub fn requireRoleRecipe(profile: Profile, manifest: anytype) !void {
    if (manifest.memorilla_profile != profile) return error.UnsupportedMemorillaArtifact;
    switch (profile) {
        .qwen3_encoder_f32_v1 => {
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

fn pinnedPath(entries: std.json.Value, name: []const u8) bool {
    for (entries.array.items) |entry| {
        const value = entry.object.get("path") orelse continue;
        if (value == .string and std.mem.eql(u8, value.string, name)) return true;
    }
    return false;
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
    const parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    defer parsed.deinit();
    const model = try member(try member(parsed.value, "models"), @tagName(role(profile)));
    const entries = try member(model, "files");
    if (entries != .array) return error.InvalidMemorillaSourceLock;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.memorilla.artifact-generation.v1\x00");
    hash.update(&executionIdentity(profile));
    for (entries.array.items) |entry| {
        const path = try member(entry, "path");
        if (path != .string) return error.InvalidMemorillaSourceLock;
        try generationEntry(&hash, a, io, directory, path.string, true, false, control);
    }
    try generationEntry(&hash, a, io, directory, "model_manifest.json", true, false, control);
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

pub fn sourceBytes(a: A, profile: Profile) !usize {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    defer parsed.deinit();
    const model = try member(try member(parsed.value, "models"), @tagName(role(profile)));
    const entries = try member(model, "files");
    if (entries != .array) return error.InvalidMemorillaSourceLock;
    var total: usize = 0;
    for (entries.array.items) |entry| {
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
    const parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    defer parsed.deinit();
    const model = try member(try member(parsed.value, "models"), @tagName(role(profile)));
    const entries = try member(model, "files");
    if (entries != .array) return error.InvalidMemorillaSourceLock;
    for (entries.array.items) |entry| {
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
    const canonical = if (role(profile) == .encoder) catalog.encoder_manifest else catalog.decoder_manifest;
    if (!std.mem.eql(u8, bytes, canonical)) return error.UnsupportedMemorillaArtifact;
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
    const parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    defer parsed.deinit();
    const profile = manifest.memorilla_profile orelse return error.UnsupportedMemorillaArtifact;
    const entries = try member(try member(try member(parsed.value, "models"), @tagName(role(profile))), "files");
    for (optional_paths) |name| {
        if (pinnedPath(entries, name)) continue;
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
    try std.testing.expectEqual(Role.decoder, role(.qwen3_decoder_f32_kv_f32_v1));
    try std.testing.expect(try sourceBytes(std.testing.allocator, .qwen3_encoder_f32_v1) > 8_000_000_000);
    try std.testing.expect(try sourceBytes(std.testing.allocator, .qwen3_decoder_f32_kv_f32_v1) > 16_000_000_000);
    const encoder = try residencyPlan(std.testing.allocator, .qwen3_encoder_f32_v1);
    const decoder = try residencyPlan(std.testing.allocator, .qwen3_decoder_f32_kv_f32_v1);
    try std.testing.expect(encoder.peak.host_scratch_bytes > 151665 * @as(usize, 2560) * 4);
    try std.testing.expect(decoder.peak.host_scratch_bytes > 151936 * @as(usize, 4096) * 4);
    try std.testing.expectEqual(try sourceBytes(std.testing.allocator, .qwen3_encoder_f32_v1), encoder.resident.host_weight_bytes);
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
