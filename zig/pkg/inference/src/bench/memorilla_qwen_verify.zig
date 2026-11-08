// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Offline soft-prompt decoder qualification. Precomputed prompt embeddings
//! isolate decoder integration from the separately checked memory module.
const std = @import("std");
const build_options = @import("build_options");
const inference = @import("inference_internal");
const base_lock = @import("memorilla_base_model_lock").bytes;
const factory = inference.architectures.session_factory;
const soft_prompt = inference.architectures.soft_prompt;
const gpt = inference.architectures.gpt;
const generation = inference.pipelines.generation;
const sf = inference.models.safetensors;
const Tensor = inference.backends.Tensor;
const A = std.mem.Allocator;

const Options = struct {
    model: []const u8 = "",
    fixture: []const u8 = "",
    output: []const u8 = "",
    backend: enum { native, cuda } = .cuda,
    kv_dtype: enum { f32, f16 } = .f32,
    atol: f32 = 0.001,
    rtol: f32 = 0.001,
    allow_synthetic: bool = false,
};

fn parse(args: []const []const u8) !Options {
    var result = Options{};
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--allow-synthetic")) {
            result.allow_synthetic = true;
            continue;
        }
        if (i + 1 >= args.len) return error.MissingArgument;
        const name = args[i];
        i += 1;
        const value = args[i];
        if (std.mem.eql(u8, name, "--model")) {
            result.model = value;
        } else if (std.mem.eql(u8, name, "--fixture")) {
            result.fixture = value;
        } else if (std.mem.eql(u8, name, "--output")) {
            result.output = value;
        } else if (std.mem.eql(u8, name, "--backend")) {
            result.backend = std.meta.stringToEnum(@TypeOf(result.backend), value) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, name, "--kv-dtype")) {
            result.kv_dtype = std.meta.stringToEnum(@TypeOf(result.kv_dtype), value) orelse return error.InvalidKvDtype;
        } else if (std.mem.eql(u8, name, "--atol")) {
            result.atol = try std.fmt.parseFloat(f32, value);
        } else if (std.mem.eql(u8, name, "--rtol")) {
            result.rtol = try std.fmt.parseFloat(f32, value);
        } else return error.UnknownArgument;
    }
    if (result.model.len == 0 or result.fixture.len == 0 or result.output.len == 0 or
        !std.math.isFinite(result.atol) or !std.math.isFinite(result.rtol) or
        result.atol <= 0 or result.rtol < 0) return error.InvalidArguments;
    return result;
}

const Comparison = struct {
    elements: usize,
    failed: usize = 0,
    max_absolute_error: f64 = 0,
    max_tolerance_ratio: f64 = 0,
    native_argmax: usize = 0,
    reference_argmax: usize = 0,
};

fn compare(actual: []const f32, expected: []const f32, opts: Options) !Comparison {
    if (actual.len == 0 or actual.len != expected.len) return error.InvalidLogitShape;
    var result = Comparison{ .elements = actual.len };
    for (actual, expected, 0..) |x, y, i| {
        if (!std.math.isFinite(x) or !std.math.isFinite(y)) return error.NonFiniteLogits;
        const difference = @abs(@as(f64, x) - y);
        const tolerance = @as(f64, opts.atol) + @as(f64, opts.rtol) * @abs(@as(f64, y));
        if (difference > tolerance) result.failed += 1;
        result.max_absolute_error = @max(result.max_absolute_error, difference);
        result.max_tolerance_ratio = @max(result.max_tolerance_ratio, difference / tolerance);
        if (x > actual[result.native_argmax]) result.native_argmax = i;
        if (y > expected[result.reference_argmax]) result.reference_argmax = i;
    }
    return result;
}

fn tensor(reader: *const sf.MMapReader, name: []const u8, dtype: inference.backends.DType, shape: []const i64) !Tensor {
    var result = try reader.readTensor(name);
    errdefer result.deinit();
    if (result.dtype != dtype or !std.mem.eql(i64, result.shape, shape)) return error.InvalidFixtureTensor;
    return result;
}

const CudaEvidence = struct {
    kernel_launches: usize = 0,
    device_kv_reads: usize = 0,
    device_kv_writes: usize = 0,
    host_attention_fallbacks: usize = 0,
    paged_attention_host_fallbacks: usize = 0,
    rope_host_fallbacks: usize = 0,
    cross_backend_sync_fallbacks: usize = 0,
    low_precision_activation_staging_calls: usize = 0,
    low_precision_matmul_calls: usize = 0,
};

fn member(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidDecoderReference;
    return value.object.get(name) orelse error.InvalidDecoderReference;
}

fn verifyReference(receipt: std.json.Value, fixture: []const u8, bytes: []const u8, opts: Options) !bool {
    const schema = try member(receipt, "schema");
    if (schema != .string or !std.mem.eql(u8, schema.string, "antfly.memorilla.decoder-reference.v1"))
        return error.InvalidDecoderReference;
    const synthetic = receipt.object.get("synthetic") orelse std.json.Value{ .bool = false };
    if (synthetic != .bool) return error.InvalidDecoderReference;
    if (synthetic.bool and !opts.allow_synthetic) return error.SyntheticDecoderReferenceNotAllowed;
    const precision = try member(receipt, "precision");
    const expected = if (synthetic.bool) "F32-source-weights; F32-activations-and-KV" else "BF16-source-weights-widened-F32; F32-activations-and-KV";
    const rounded_f16 = if (synthetic.bool) "F32-source-weights; F32-activations; FP16-KV-roundtrip-F32" else "BF16-source-weights-widened-F32; F32-activations; FP16-KV-roundtrip-F32";
    if (precision != .string or
        !(std.mem.eql(u8, precision.string, expected) or
            (opts.kv_dtype == .f16 and std.mem.eql(u8, precision.string, rounded_f16))))
        return error.InvalidDecoderReferencePrecision;
    const file = try member(try member(receipt, "files"), std.fs.path.basename(fixture));
    const size = try member(file, "size_bytes");
    const digest = try member(file, "sha256");
    const actual = inference.models.memorilla.digest(bytes);
    if (size != .integer or size.integer < 0 or size.integer != bytes.len or
        digest != .string or !std.mem.eql(u8, digest.string, &actual)) return error.DecoderReferenceArtifactMismatch;
    return synthetic.bool;
}

const ModelIdentity = struct {
    model: []const u8,
    revision: []const u8,
    verified_files: usize,
    artifact_inventory_sha256: ?[64]u8 = null,
};

fn equalText(value: std.json.Value, expected: []const u8) !void {
    if (value != .string or !std.mem.eql(u8, value.string, expected)) return error.DecoderModelIdentityMismatch;
}

/// Compare the receipt with the compiled source lock, then hash the actual
/// model files before loading. A same-shaped checkpoint is not this release.
fn verifyModelIdentity(a: A, io: std.Io, receipt: std.json.Value, directory: []const u8, synthetic: bool) !ModelIdentity {
    if (synthetic) return .{ .model = "synthetic-qwen3", .revision = "test-only", .verified_files = 0 };
    const lock_digest = inference.models.memorilla.digest(base_lock);
    try equalText(try member(receipt, "base_model_lock_sha256"), &lock_digest);
    const lock = try std.json.parseFromSlice(std.json.Value, a, base_lock, .{});
    defer lock.deinit();
    const pinned = try member(try member(lock.value, "models"), "decoder");
    const declared = try member(receipt, "model");
    const model_name = "Qwen/Qwen3-8B";
    const revision = "b968826d9c46dd6066d109eabc6255188de91218";
    try equalText(try member(pinned, "model"), model_name);
    try equalText(try member(pinned, "revision"), revision);
    try equalText(try member(declared, "model"), model_name);
    try equalText(try member(declared, "revision"), revision);
    const files = try member(pinned, "files");
    const declared_files = try member(declared, "files");
    if (files != .array or declared_files != .array or files.array.items.len != declared_files.array.items.len)
        return error.DecoderModelIdentityMismatch;
    var inventory = std.crypto.hash.sha2.Sha256.init(.{});
    inventory.update("antfly.memorilla.decoder-model.v1\x00");
    for (files.array.items, declared_files.array.items) |entry, supplied| {
        const name = try member(entry, "path");
        const size = try member(entry, "size_bytes");
        const supplied_size = try member(supplied, "size_bytes");
        if (name != .string or size != .integer or size.integer <= 0 or
            supplied_size != .integer or supplied_size.integer != size.integer) return error.DecoderModelIdentityMismatch;
        try equalText(try member(supplied, "path"), name.string);
        const path = try std.fs.path.join(a, &.{ directory, name.string });
        defer a.free(path);
        const actual_size = try inference.util.c_file.fileSize(a, path);
        if (actual_size != size.integer) return error.DecoderModelArtifactMismatch;
        inventory.update(name.string);
        inventory.update("\x00");
        var size_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &size_bytes, actual_size, .little);
        inventory.update(&size_bytes);
        if (entry.object.get("sha256")) |expected| {
            if (expected != .string) return error.DecoderModelIdentityMismatch;
            try equalText(try member(supplied, "sha256"), expected.string);
            const raw = try inference.util.c_file.sha256File(a, path);
            const actual = std.fmt.bytesToHex(raw, .lower);
            if (!std.mem.eql(u8, &actual, expected.string)) return error.DecoderModelArtifactMismatch;
            inventory.update("sha256");
            inventory.update(&raw);
        } else {
            const expected = try member(entry, "git_blob_sha1");
            if (expected != .string) return error.DecoderModelIdentityMismatch;
            try equalText(try member(supplied, "git_blob_sha1"), expected.string);
            const bytes = try inference.util.c_file.readFileMax(a, path, 64 * 1024 * 1024);
            defer a.free(bytes);
            if (bytes.len != actual_size) return error.DecoderModelArtifactMismatch;
            var prefix_buffer: [64]u8 = undefined;
            const prefix = try std.fmt.bufPrint(&prefix_buffer, "blob {d}\x00", .{bytes.len});
            var hash = std.crypto.hash.Sha1.init(.{});
            hash.update(prefix);
            hash.update(bytes);
            const raw = hash.finalResult();
            const actual = std.fmt.bytesToHex(raw, .lower);
            if (!std.mem.eql(u8, &actual, expected.string)) return error.DecoderModelArtifactMismatch;
            inventory.update("git-blob-sha1");
            inventory.update(&raw);
        }
    }
    // Do not hash the pinned shards but then load an unverified single-file
    // or GGUF alternative selected by a local manifest.
    var manifest = try inference.models.manifest.loadFromDir(a, directory);
    defer manifest.deinit();
    if (manifest.nativeWeightArtifactKind() != .sharded_safetensors) return error.DecoderModelIdentityMismatch;
    const selected = manifest.safetensors_index_path orelse return error.DecoderModelIdentityMismatch;
    const requested = try std.fs.path.join(a, &.{ directory, "model.safetensors.index.json" });
    defer a.free(requested);
    const selected_real = try std.Io.Dir.cwd().realPathFileAlloc(io, selected, a);
    defer a.free(selected_real);
    const requested_real = try std.Io.Dir.cwd().realPathFileAlloc(io, requested, a);
    defer a.free(requested_real);
    if (!std.mem.eql(u8, selected_real, requested_real)) return error.DecoderModelIdentityMismatch;
    return .{
        .model = model_name,
        .revision = revision,
        .verified_files = files.array.items.len,
        .artifact_inventory_sha256 = std.fmt.bytesToHex(inventory.finalResult(), .lower),
    };
}

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    const args = try init.minimal.args.toSlice(a);
    defer a.free(args);
    const opts = try parse(args);
    if (opts.backend == .cuda and !build_options.enable_cuda) return error.CudaNotEnabled;
    const config_path = try std.fs.path.join(a, &.{ opts.model, "config.json" });
    defer a.free(config_path);
    const config_bytes = try inference.util.c_file.readFileMax(a, config_path, 1024 * 1024);
    defer a.free(config_bytes);

    var reader = try sf.MMapReader.openFileAbsoluteLimited(a, opts.fixture, 256 * 1024 * 1024, 1024 * 1024);
    defer reader.deinit();
    try sf.validateReader(a, &reader);
    const reference_path = try std.fs.path.join(a, &.{ std.fs.path.dirname(opts.fixture) orelse ".", "reference.json" });
    defer a.free(reference_path);
    const reference_bytes = try inference.util.c_file.readFileMax(a, reference_path, 4 * 1024 * 1024);
    defer a.free(reference_bytes);
    const reference = try std.json.parseFromSlice(std.json.Value, a, reference_bytes, .{});
    defer reference.deinit();
    const synthetic = try verifyReference(reference.value, opts.fixture, reader.file_bytes, opts);
    const model_identity = try verifyModelIdentity(a, init.io, reference.value, opts.model, synthetic);
    const ids_meta = reader.header.tensors.get("input_ids") orelse return error.MissingFixtureTensor;
    const embed_meta = reader.header.tensors.get("input_embeddings") orelse return error.MissingFixtureTensor;
    const decode_meta = reader.header.tensors.get("decode_input_ids") orelse return error.MissingFixtureTensor;
    if (ids_meta.shape.len != 1 or embed_meta.shape.len != 2 or decode_meta.shape.len != 1 or
        ids_meta.shape[0] <= 0 or ids_meta.shape[0] > 40960 or decode_meta.shape[0] <= 0 or decode_meta.shape[0] > 32 or
        embed_meta.shape[0] != ids_meta.shape[0] or embed_meta.shape[1] <= 0)
        return error.InvalidFixtureShape;
    const sequence: usize = @intCast(ids_meta.shape[0]);
    const hidden: usize = @intCast(embed_meta.shape[1]);
    const steps: usize = @intCast(decode_meta.shape[0]);
    var ids_tensor = try tensor(&reader, "input_ids", .i64, &.{@intCast(sequence)});
    defer ids_tensor.deinit();
    var embeddings_tensor = try tensor(&reader, "input_embeddings", .f32, &.{ @intCast(sequence), @intCast(hidden) });
    defer embeddings_tensor.deinit();
    var decode_ids_tensor = try tensor(&reader, "decode_input_ids", .i64, &.{@intCast(steps)});
    defer decode_ids_tensor.deinit();

    var session = switch (opts.backend) {
        .native => try factory.createNativeSession(a, opts.model),
        .cuda => try factory.createQwen3CudaReferenceSession(a, opts.model),
    };
    defer session.close();
    const expected_backend: inference.backends.BackendType = switch (opts.backend) {
        .native => .native,
        .cuda => .cuda,
    };
    if (session.backend() != expected_backend) return error.UnexpectedBackend;
    const cfg = factory.getGptConfig(session) orelse return error.NotGptModel;
    if (hidden != cfg.hidden_size or sequence + steps > cfg.max_position_embeddings) return error.InvalidFixtureShape;
    var expected_prefill = try tensor(&reader, "prefill_logits", .f32, &.{@intCast(cfg.vocab_size)});
    defer expected_prefill.deinit();
    var expected_decode = try tensor(&reader, "decode_logits", .f32, &.{ @intCast(steps), @intCast(cfg.vocab_size) });
    defer expected_decode.deinit();
    const input = soft_prompt.Input{ .input_ids = ids_tensor.asInt64(), .input_embeddings = embeddings_tensor.asFloat32() };
    try input.validate(cfg);
    for (decode_ids_tensor.asInt64()) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidSoftPromptToken;

    var cb = try factory.getComputeBackend(session, a);
    defer cb.deinit();
    if (comptime build_options.enable_cuda) {
        if (opts.backend == .cuda) {
            const compute: *inference.native_compute.cuda.CudaCompute = @ptrCast(@alignCast(cb.ptr));
            if (!compute.strict_f32_weights) return error.UnqualifiedDecoderPrecision;
            var iter = compute.resident_weights.valueIterator();
            while (iter.next()) |weight| {
                if (weight.dtype != .f32 or weight.quant_type != null) return error.UnqualifiedDecoderPrecision;
            }
        }
    }
    var kv_manager = inference.runtime.kv.manager.KvManager.init(a);
    defer kv_manager.deinit();
    const backend_kind: inference.runtime.kv.pool.BackendKind = if (opts.backend == .cuda) .cuda else .native;
    const dtype: inference.runtime.kv.pool.KvDType = if (opts.kv_dtype == .f32) .f32 else .f16;
    var pool_config = generation.kvPoolConfig(backend_kind, dtype, cfg, false);
    if (opts.backend == .cuda) pool_config.store_cpu_bytes = false;
    const pool = try kv_manager.addPool(pool_config);
    var storage = try inference.runtime.kv.storage_runtime.KvStorageRuntime.init(a, pool_config);
    defer storage.deinit();
    try cb.provisionKvDeviceWriteHook(&storage);
    if (opts.backend == .cuda and storage.device_write_hook == null) return error.MissingDeviceKvHook;
    var state = generation.NativeDecodeState.initPaged(a, &kv_manager, pool, null);
    state.kv_storage = &storage;
    state.configureForGptConfig(cfg);
    defer state.deinit();

    const stats_before = if (comptime build_options.enable_cuda) factory.getCudaRuntimeStats(session) else null;
    try state.notePrefill(sequence);
    const prefill_context = state.gptDecodeContext(sequence, sequence);
    const logits = try soft_prompt.prefillLastLogits(&cb, a, cfg, input, &prefill_context);
    defer a.free(logits);
    const prefill_comparison = try compare(logits, expected_prefill.asFloat32(), opts);
    const decode_comparisons = try a.alloc(Comparison, steps);
    defer a.free(decode_comparisons);
    var failed = prefill_comparison.failed != 0;
    for (decode_ids_tensor.asInt64(), 0..) |id, step| {
        try state.appendGeneratedToken();
        const total = sequence + step + 1;
        const context = state.gptDecodeContext(total, 1);
        const values = try gpt.forward(&cb, a, cfg, &.{id}, 1, total, &context);
        defer a.free(values);
        decode_comparisons[step] = try compare(values, expected_decode.asFloat32()[step * cfg.vocab_size ..][0..cfg.vocab_size], opts);
        failed = failed or decode_comparisons[step].failed != 0;
    }

    var evidence = CudaEvidence{};
    if (comptime build_options.enable_cuda) {
        if (factory.getCudaRuntimeStats(session)) |after| {
            const before = stats_before orelse return error.MissingCudaEvidence;
            evidence = .{
                .kernel_launches = after.kernel_launches -| before.kernel_launches,
                .device_kv_reads = after.device_kv_reads -| before.device_kv_reads,
                .device_kv_writes = after.device_kv_writes -| before.device_kv_writes,
                .host_attention_fallbacks = after.host_attention_fallbacks -| before.host_attention_fallbacks,
                .paged_attention_host_fallbacks = after.paged_attention_host_fallbacks -| before.paged_attention_host_fallbacks,
                .rope_host_fallbacks = after.rope_host_fallbacks -| before.rope_host_fallbacks,
                .cross_backend_sync_fallbacks = after.cross_backend_sync_fallbacks -| before.cross_backend_sync_fallbacks,
                .low_precision_activation_staging_calls = (after.bf16_cublaslt_activation_staging_calls -| before.bf16_cublaslt_activation_staging_calls) +
                    (after.f16_cublaslt_activation_staging_calls -| before.f16_cublaslt_activation_staging_calls),
                .low_precision_matmul_calls = (after.bf16_cublaslt_linear_calls -| before.bf16_cublaslt_linear_calls) +
                    (after.bf16_cublaslt_qkv_calls -| before.bf16_cublaslt_qkv_calls) +
                    (after.f16_cublaslt_linear_calls -| before.f16_cublaslt_linear_calls) +
                    (after.f16_cublaslt_qkv_calls -| before.f16_cublaslt_qkv_calls) +
                    (after.bf16_bucketed_linear_calls -| before.bf16_bucketed_linear_calls) +
                    (after.bf16_decode_output_calls -| before.bf16_decode_output_calls) +
                    (after.rms_norm_bf16_mirror_hits -| before.rms_norm_bf16_mirror_hits),
            };
        }
    }
    const device_ok = opts.backend != .cuda or
        (evidence.kernel_launches > 0 and evidence.device_kv_reads > 0 and evidence.device_kv_writes > 0 and
            evidence.host_attention_fallbacks == 0 and evidence.paged_attention_host_fallbacks == 0 and evidence.rope_host_fallbacks == 0 and
            evidence.cross_backend_sync_fallbacks == 0 and evidence.low_precision_activation_staging_calls == 0 and evidence.low_precision_matmul_calls == 0);
    const report = .{
        .schema = "antfly.memorilla.decoder-conformance.v1",
        .scope = "decoder-only-precomputed-soft-prompt",
        .synthetic_model = synthetic,
        .model_identity = model_identity,
        .passed = !failed and device_ok,
        .backend = @tagName(opts.backend),
        .arithmetic = "source-weights-widened-f32-and-f32-activations",
        .reference_precision = (try member(reference.value, "precision")).string,
        .kv_dtype = @tagName(opts.kv_dtype),
        .fixture_sha256 = inference.models.memorilla.digest(reader.file_bytes),
        .reference_metadata_sha256 = inference.models.memorilla.digest(reference_bytes),
        .base_model_lock_sha256 = inference.models.memorilla.digest(base_lock),
        .model_config_sha256 = inference.models.memorilla.digest(config_bytes),
        .source_tokens = sequence,
        .decode_steps = steps,
        .atol = opts.atol,
        .rtol = opts.rtol,
        .prefill = prefill_comparison,
        .decode = decode_comparisons,
        .cuda = evidence,
    };
    const json = try std.json.Stringify.valueAlloc(a, report, .{ .whitespace = .indent_2 });
    defer a.free(json);
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = opts.output, .data = json });
    std.debug.print("Memorilla decoder backend={s} prompt={d} decode={d} passed={} report={s}\n", .{ @tagName(opts.backend), sequence, steps, !failed and device_ok, opts.output });
    if (!device_ok) return error.MissingCudaEvidence;
    if (failed) return error.DecoderConformanceFailed;
}

test "Memorilla decoder comparison gates every logit and records argmax" {
    const exact = try compare(&.{ 1, 3, 2 }, &.{ 1, 3, 2 }, .{});
    try std.testing.expectEqual(@as(usize, 0), exact.failed);
    try std.testing.expectEqual(@as(usize, 1), exact.native_argmax);
    const bad = try compare(&.{ 1, 3, 2.1 }, &.{ 1, 3, 2 }, .{});
    try std.testing.expectEqual(@as(usize, 1), bad.failed);
    try std.testing.expectError(error.NonFiniteLogits, compare(&.{std.math.nan(f32)}, &.{0}, .{}));
}
