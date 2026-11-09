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

//! Offline strict-F32 encoder diagnostic. Accepts the bounded embed command
//! used by encoder_reference.py, without changing production serving policy.
const std = @import("std");
const inference = @import("inference_internal");
const build_options = @import("build_options");
const lock_bytes = @import("memorilla_base_model_lock").bytes;
const factory = inference.architectures.session_factory;
const A = std.mem.Allocator;
const model_name = "Qwen/Qwen3-Embedding-4B";
const revision = "5cf2132abc99cad020ac570b19d031efec650f2b";
const recipe_sha256 = "cde71e9919a43e374ad7aae68f56943f01b9ddd84d26601cf8155a4c123631fa";

fn member(value: std.json.Value, key: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidEncoderLock;
    return value.object.get(key) orelse error.InvalidEncoderLock;
}

fn verifyIdentity(a: A, io: std.Io, directory: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, a, lock_bytes, .{});
    defer parsed.deinit();
    const pinned = try member(try member(parsed.value, "models"), "encoder");
    const name = try member(pinned, "model");
    const rev = try member(pinned, "revision");
    if (name != .string or rev != .string or !std.mem.eql(u8, name.string, model_name) or !std.mem.eql(u8, rev.string, revision)) return error.InvalidEncoderLock;
    const files = try member(pinned, "files");
    if (files != .array) return error.InvalidEncoderLock;
    for (files.array.items) |entry| {
        const relative = try member(entry, "path");
        const size = try member(entry, "size_bytes");
        if (relative != .string or size != .integer or size.integer <= 0) return error.InvalidEncoderLock;
        const path = try std.fs.path.join(a, &.{ directory, relative.string });
        defer a.free(path);
        if (try inference.util.c_file.fileSize(a, path) != size.integer) return error.EncoderArtifactMismatch;
        if (entry.object.get("sha256")) |expected| {
            if (expected != .string) return error.InvalidEncoderLock;
            const digest = try inference.util.c_file.sha256File(a, path);
            if (!std.mem.eql(u8, &std.fmt.bytesToHex(digest, .lower), expected.string)) return error.EncoderArtifactMismatch;
        } else {
            const expected = try member(entry, "git_blob_sha1");
            if (expected != .string) return error.InvalidEncoderLock;
            const bytes = try inference.util.c_file.readFileMax(a, path, 64 * 1024 * 1024);
            defer a.free(bytes);
            var buffer: [64]u8 = undefined;
            const prefix = try std.fmt.bufPrint(&buffer, "blob {d}\x00", .{bytes.len});
            var hash = std.crypto.hash.Sha1.init(.{});
            hash.update(prefix);
            hash.update(bytes);
            if (!std.mem.eql(u8, &std.fmt.bytesToHex(hash.finalResult(), .lower), expected.string)) return error.EncoderArtifactMismatch;
        }
    }
    const recipe_path = try std.fs.path.join(a, &.{ directory, "model_manifest.json" });
    defer a.free(recipe_path);
    const recipe_digest = try inference.util.c_file.sha256File(a, recipe_path);
    if (!std.mem.eql(u8, &std.fmt.bytesToHex(recipe_digest, .lower), recipe_sha256)) return error.EncoderRecipeMismatch;
    var manifest = try inference.models.manifest.loadFromDir(a, directory);
    defer manifest.deinit();
    if (manifest.nativeWeightArtifactKind() != .sharded_safetensors) return error.EncoderArtifactMismatch;
    const selected = manifest.safetensors_index_path orelse return error.EncoderArtifactMismatch;
    const requested = try std.fs.path.join(a, &.{ directory, "model.safetensors.index.json" });
    defer a.free(requested);
    const selected_real = try std.Io.Dir.cwd().realPathFileAlloc(io, selected, a);
    defer a.free(selected_real);
    const requested_real = try std.Io.Dir.cwd().realPathFileAlloc(io, requested, a);
    defer a.free(requested_real);
    if (!std.mem.eql(u8, selected_real, requested_real)) return error.EncoderArtifactMismatch;
}

const Capture = struct {
    a: A,
    texts: []const []const u8,
    rows: []?[]i32,

    fn observe(raw: *anyopaque, text: []const u8, ids: []const i32, mask: []const i32) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (ids.len != mask.len) return error.InvalidTokenization;
        var active: std.ArrayListUnmanaged(i32) = .empty;
        defer active.deinit(self.a);
        for (ids, mask) |id, used| {
            if (used != 0 and used != 1) return error.InvalidTokenization;
            if (used == 1) try active.append(self.a, id);
        }
        if (active.items.len == 0 or active.items.len > 256 or active.items[active.items.len - 1] != 151643) return error.InvalidTokenization;
        var found = false;
        for (self.texts, self.rows) |input, *row| {
            if (!std.mem.eql(u8, input, text)) continue;
            found = true;
            if (row.*) |previous| {
                if (!std.mem.eql(i32, previous, active.items)) return error.InconsistentTokenization;
            } else row.* = try self.a.dupe(i32, active.items);
        }
        if (!found) return error.InvalidTokenization;
    }
};

pub fn main(init: std.process.Init) !void {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    // Match the existing reference helper's native embed command interface.
    if (args.len < 7 or !std.mem.eql(u8, args[1], "embed")) return error.InvalidArguments;
    const directory = args[2];
    var texts: std.ArrayListUnmanaged([]const u8) = .empty;
    defer texts.deinit(a);
    var diagnostics = false;
    var cuda = false;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--diagnostics")) {
            diagnostics = true;
            continue;
        }
        if (i + 1 >= args.len) return error.InvalidArguments;
        const key = args[i];
        i += 1;
        if (std.mem.eql(u8, key, "--text")) {
            if (args[i].len > 8192 or texts.items.len == 16) return error.InvalidArguments;
            try texts.append(a, args[i]);
        } else if (std.mem.eql(u8, key, "--backend")) {
            if (!std.mem.eql(u8, args[i], "cuda")) return error.InvalidArguments;
            cuda = true;
        } else return error.InvalidArguments;
    }
    if (!diagnostics or !cuda or texts.items.len == 0) return error.InvalidArguments;
    try verifyIdentity(a, init.io, directory);
    const session = try factory.createQwen3EmbeddingCudaReferenceSession(a, directory);
    defer session.close();
    const cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    const compute: *inference.native_compute.cuda.CudaCompute = @ptrCast(@alignCast(cb.ptr));
    if (!compute.strict_f32_weights) return error.UnqualifiedEncoderPrecision;
    var weights = compute.resident_weights.valueIterator();
    while (weights.next()) |weight| if (weight.dtype != .f32 or weight.quant_type != null) return error.UnqualifiedEncoderPrecision;
    const hf = try inference.server.model_manager.loadHuggingFaceTokenizerFromDir(a, directory);
    const tok = hf.tokenizer();
    defer tok.deinitTokenizer();
    const rows = try a.alloc(?[]i32, texts.items.len);
    defer a.free(rows);
    @memset(rows, null);
    defer for (rows) |row| if (row) |owned| a.free(owned);
    var capture = Capture{ .a = a, .texts = texts.items, .rows = rows };
    var pipeline = inference.pipelines.embedding.EmbeddingPipeline.init(a, session, tok, .{
        .normalize = true,
        .pooling = .last,
        .max_length = 256,
        .strict_max_length = true,
        .reject_truncation = true,
        .trim_padding_to_batch_max = true,
        .resident_qwen3_embedding = true,
        .ensure_trailing_eos_id = 151643,
    });
    pipeline.tokenization_observer = .{ .ptr = &capture, .observe = Capture.observe };
    pipeline.execution_control = .{ .io = init.io, .native_call_policy = .in_process };
    const before = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaEvidence;
    const vectors = try pipeline.embed(texts.items);
    defer {
        for (vectors) |vector| a.free(vector);
        a.free(vectors);
    }
    const after = factory.getCudaRuntimeStats(session) orelse return error.MissingCudaEvidence;
    const kernel_launches = after.kernel_launches -| before.kernel_launches;
    const linear = after.launch_linear -| before.launch_linear;
    const attention = after.launch_attention -| before.launch_attention;
    const host_attention = after.host_attention_fallbacks -| before.host_attention_fallbacks;
    const paged_host_attention = after.paged_attention_host_fallbacks -| before.paged_attention_host_fallbacks;
    const rope = after.rope_host_fallbacks -| before.rope_host_fallbacks;
    const sync = after.cross_backend_sync_fallbacks -| before.cross_backend_sync_fallbacks;
    const staging = (after.bf16_cublaslt_activation_staging_calls -| before.bf16_cublaslt_activation_staging_calls) + (after.f16_cublaslt_activation_staging_calls -| before.f16_cublaslt_activation_staging_calls);
    const low_precision = (after.bf16_cublaslt_linear_calls -| before.bf16_cublaslt_linear_calls) + (after.bf16_cublaslt_qkv_calls -| before.bf16_cublaslt_qkv_calls) + (after.f16_cublaslt_linear_calls -| before.f16_cublaslt_linear_calls) + (after.f16_cublaslt_qkv_calls -| before.f16_cublaslt_qkv_calls) + (after.bf16_bucketed_linear_calls -| before.bf16_bucketed_linear_calls) + (after.bf16_decode_output_calls -| before.bf16_decode_output_calls) + (after.rms_norm_bf16_mirror_hits -| before.rms_norm_bf16_mirror_hits);
    if (kernel_launches == 0 or linear == 0 or attention == 0 or host_attention != 0 or paged_host_attention != 0 or rope != 0 or sync != 0 or staging != 0 or low_precision != 0) return error.UnqualifiedEncoderExecution;
    const token_ids = try a.alloc([]const i32, rows.len);
    defer a.free(token_ids);
    for (rows, token_ids) |row, *output| output.* = row orelse return error.MissingTokenization;
    const json = try std.json.Stringify.valueAlloc(a, .{
        .schema = "antfly.embedding.diagnostics.v1",
        .backend = "cuda",
        .arithmetic = "BF16-source-weights-widened-F32; F32-activations",
        .scope = "offline-strict-f32-encoder-reference",
        .model = model_name,
        .revision = revision,
        .base_model_lock_sha256 = inference.models.memorilla.digest(lock_bytes),
        .text_prefix = "",
        .trailing_eos_id = @as(i32, 151643),
        .pooling = "last",
        .normalize = true,
        .token_ids = token_ids,
        .embeddings = vectors,
        .cuda_execution = .{
            .scope = "text-forward-after-asset-init",
            .host_output_readback_allowed = true,
            .resident_weight_bytes_before = before.resident_weight_bytes,
            .resident_weight_bytes_after = after.resident_weight_bytes,
            .strict_f32_weights = compute.strict_f32_weights,
            .resident_weights = compute.resident_weights.count(),
            .low_precision_activation_staging_calls = staging,
            .low_precision_matmul_calls = low_precision,
            .counters = .{
                .kernel_launches = kernel_launches,
                .linear_kernel_launches = linear,
                .linear_qkv_kernel_launches = after.launch_linear_qkv -| before.launch_linear_qkv,
                .attention_kernel_launches = attention,
                .bf16_cublaslt_linear_calls = @as(usize, 0),
                .bf16_cublaslt_qkv_calls = @as(usize, 0),
                .f16_cublaslt_linear_calls = @as(usize, 0),
                .f16_cublaslt_qkv_calls = @as(usize, 0),
                .host_attention_fallbacks = host_attention,
                .paged_attention_host_fallbacks = paged_host_attention,
                .rope_host_fallbacks = rope,
                .cross_backend_sync_fallbacks = sync,
                .d2h_bytes = after.d2h_bytes -| before.d2h_bytes,
            },
        },
    }, .{});
    defer a.free(json);
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(init.io, &buffer);
    try stdout.interface.writeAll(json);
    try stdout.interface.writeByte('\n');
    try stdout.interface.flush();
}
