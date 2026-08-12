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
const ops = @import("ops/ops.zig");
const tensor_store_mod = @import("models/tensor_store.zig");
const weight_source_mod = @import("models/weight_source.zig");
const gguf_mod = @import("gguf/root.zig");
const gpt_mod = @import("models/gpt.zig");
const session_factory = @import("architectures/session_factory.zig");

const print = std.debug.print;

const cuda_context = if (build_options.enable_cuda) @import("ops/cuda/context.zig") else struct {};
const cuda_compute = if (build_options.enable_cuda) @import("ops/cuda/cuda_compute.zig") else struct {};
const cuda_kernels = if (build_options.enable_cuda) @import("ops/cuda/kernels.zig") else struct {};
const cuda_artifact = if (build_options.enable_cuda) @import("ops/cuda/artifact.zig") else struct {};
const cuda_buffer = if (build_options.enable_cuda) @import("ops/cuda/buffer.zig") else struct {};
const cuda_cublaslt = if (build_options.enable_cuda) @import("ops/cuda/cublaslt.zig") else struct {};

const NormRopeParityCase = struct {
    weight_name: []const u8,
    layer: usize,
    proj: []const u8,
    rows: usize,
    total_dim: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    scale: f32,
};

pub fn main(allocator: std.mem.Allocator, _: std.Io, args: []const []const u8) !void {
    var smoke = false;
    var artifact_identity = false;
    var e4b_q8_prefill_smoke = false;
    var gemma4_parity_path: ?[]const u8 = null;
    var gemma4_hf_parity_path: ?[]const u8 = null;
    var gemma4_cross_gguf_path: ?[]const u8 = null;
    var gemma4_cross_hf_dir: ?[]const u8 = null;
    var gemma4_cross_rmse_gguf_path: ?[]const u8 = null;
    var gemma4_cross_rmse_hf_dir: ?[]const u8 = null;
    var gemma4_cross_layer0_gguf_path: ?[]const u8 = null;
    var gemma4_cross_layer0_hf_dir: ?[]const u8 = null;
    var gguf_meta_path: ?[]const u8 = null;
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--smoke")) {
            smoke = true;
        } else if (std.mem.eql(u8, arg, "--artifact-identity")) {
            artifact_identity = true;
        } else if (std.mem.eql(u8, arg, "--e4b-q8-prefill-smoke")) {
            e4b_q8_prefill_smoke = true;
        } else if (std.mem.eql(u8, arg, "--gguf-meta")) {
            i += 1;
            if (i >= args.len) {
                print("missing value for --gguf-meta\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gguf_meta_path = args[i];
        } else if (std.mem.eql(u8, arg, "--gemma4-parity")) {
            i += 1;
            if (i >= args.len) {
                print("missing value for --gemma4-parity\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_parity_path = args[i];
        } else if (std.mem.eql(u8, arg, "--gemma4-hf-parity")) {
            i += 1;
            if (i >= args.len) {
                print("missing value for --gemma4-hf-parity\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_hf_parity_path = args[i];
        } else if (std.mem.eql(u8, arg, "--gemma4-cross-parity")) {
            i += 1;
            if (i >= args.len) {
                print("missing GGUF value for --gemma4-cross-parity\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_cross_gguf_path = args[i];
            i += 1;
            if (i >= args.len) {
                print("missing HF model-dir value for --gemma4-cross-parity\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_cross_hf_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--gemma4-cross-rmse")) {
            i += 1;
            if (i >= args.len) {
                print("missing GGUF value for --gemma4-cross-rmse\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_cross_rmse_gguf_path = args[i];
            i += 1;
            if (i >= args.len) {
                print("missing HF model-dir value for --gemma4-cross-rmse\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_cross_rmse_hf_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--gemma4-cross-layer0")) {
            i += 1;
            if (i >= args.len) {
                print("missing GGUF value for --gemma4-cross-layer0\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_cross_layer0_gguf_path = args[i];
            i += 1;
            if (i >= args.len) {
                print("missing HF model-dir value for --gemma4-cross-layer0\n", .{});
                printUsage();
                std.process.exit(1);
            }
            gemma4_cross_layer0_hf_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else {
            print("unknown cuda-info option: {s}\n", .{arg});
            printUsage();
            std.process.exit(1);
        }
    }

    if (!build_options.enable_cuda) {
        print("cuda: unavailable\nreason: backend not built; rebuild with -Dcuda=true\n", .{});
        std.process.exit(1);
    }

    if (comptime build_options.enable_cuda) {
        if (artifact_identity) {
            var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(cuda_artifact.image, &digest, .{});
            const digest_hex = std.fmt.bytesToHex(digest, .lower);
            print("cuda_artifact_identity_schema: antfly.cuda_artifact_identity.v1\n", .{});
            print("cuda_artifact_mode: {s}\n", .{cuda_artifact.mode});
            print("cuda_artifact_format: {s}\n", .{cuda_artifact.format});
            print("cuda_artifact_target: {s}\n", .{cuda_artifact.target});
            print("cuda_artifact_image_bytes: {d}\n", .{cuda_artifact.image.len});
            print("cuda_artifact_image_sha256: {s}\n", .{digest_hex});
            return;
        }
        const info = cuda_context.probeDefault() catch |err| {
            print("cuda: unavailable\nreason: {s}\n", .{@errorName(err)});
            std.process.exit(1);
        };

        print("cuda: available\n", .{});
        print("driver_version: {d}\n", .{info.driver_version});
        print("device_count: {d}\n", .{info.device_count});
        print("selected_device: {d}\n", .{info.selected_device});
        print("device_name: {s}\n", .{info.nameSlice()});
        print("compute_capability: sm_{d}{d}\n", .{ info.compute_major, info.compute_minor });
        print("artifacts: {s}\n", .{build_options.cuda_artifacts});
        var compute = cuda_compute.CudaCompute.init(allocator) catch |err| {
            print("capabilities: unavailable\nreason: {s}\n", .{@errorName(err)});
            std.process.exit(1);
        };
        defer compute.deinit();
        print("capability_clipclap: {}\n", .{compute.supportsProfile(.clipclap)});
        print("capability_deberta_reranker: {}\n", .{compute.supportsProfile(.deberta_reranker)});
        print("capability_florence2: {}\n", .{compute.supportsProfile(.florence2)});
        print("capability_gliner2: {}\n", .{compute.supportsProfile(.gliner2)});
        print("capability_gemma4: {}\n", .{compute.supportsProfile(.gemma4)});

        if (smoke) {
            cuda_kernels.smokeFill(allocator) catch |err| {
                print("smoke: fill_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: fill_f32 ok\n", .{});
            cuda_kernels.smokeGraphCapture(allocator) catch |err| {
                print("smoke: graph_capture failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: graph_capture ok\n", .{});
            cuda_kernels.smokeDenseF32(allocator) catch |err| {
                print("smoke: dense_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: dense_f32 ok\n", .{});
            cuda_kernels.smokeQ8_0(allocator) catch |err| {
                print("smoke: q8_0_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q8_0_f32 ok\n", .{});
            cuda_kernels.smokeQ4_0(allocator) catch |err| {
                print("smoke: q4_0_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q4_0_f32 ok\n", .{});
            cuda_kernels.smokeQ4_K(allocator) catch |err| {
                print("smoke: q4_k_f32 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q4_k_f32 ok\n", .{});
            cuda_kernels.smokeQ6_K(allocator) catch |err| {
                print("smoke: q6_k_embedding failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: q6_k_embedding ok\n", .{});
            cuda_kernels.smokeGemma4Primitives(allocator) catch |err| {
                print("smoke: gemma4_primitives failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: gemma4_primitives ok\n", .{});
            cuda_kernels.smokeFlorence2Primitives(allocator) catch |err| {
                print("smoke: florence2_primitives failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: florence2_primitives ok\n", .{});
            cuda_kernels.smokeTurboquantKv(allocator) catch |err| {
                print("smoke: turboquant_kv failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: turboquant_kv ok\n", .{});
            cuda_compute.smokeDecoderRuntimeSlots(allocator) catch |err| {
                print("smoke: decoder_runtime_slots failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: decoder_runtime_slots ok\n", .{});
            const cublaslt_ok = smokeCublasLtBf16(allocator) catch |err| {
                print("smoke: cublaslt_bf16 failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            if (cublaslt_ok) {
                print("smoke: cublaslt_bf16 ok\n", .{});
            } else {
                print("smoke: cublaslt_bf16 skipped\n", .{});
            }
        }

        if (e4b_q8_prefill_smoke) {
            cuda_kernels.smokeQ4_0E4BPairActivationQ8_1Rows(allocator) catch |err| {
                print("smoke: e4b_q8_prefill_rows failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("smoke: e4b_q8_prefill_rows ok\n", .{});
        }

        if (gemma4_parity_path) |path| {
            runGemma4Parity(allocator, path) catch |err| {
                print("gemma4_parity: failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("gemma4_parity: ok\n", .{});
        }
        if (gemma4_hf_parity_path) |path| {
            runGemma4HfParity(allocator, path) catch |err| {
                print("gemma4_hf_parity: failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("gemma4_hf_parity: ok\n", .{});
        }
        if (gemma4_cross_gguf_path) |gguf_path| {
            runGemma4CrossParity(allocator, gguf_path, gemma4_cross_hf_dir orelse return error.InvalidArguments) catch |err| {
                print("gemma4_cross_parity: failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("gemma4_cross_parity: ok\n", .{});
        }
        if (gemma4_cross_rmse_gguf_path) |gguf_path| {
            runGemma4CrossRmse(allocator, gguf_path, gemma4_cross_rmse_hf_dir orelse return error.InvalidArguments) catch |err| {
                print("gemma4_cross_rmse: failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("gemma4_cross_rmse: ok\n", .{});
        }
        if (gemma4_cross_layer0_gguf_path) |gguf_path| {
            runGemma4CrossLayer0(allocator, gguf_path, gemma4_cross_layer0_hf_dir orelse return error.InvalidArguments) catch |err| {
                print("gemma4_cross_layer0: failed\nreason: {s}\n", .{@errorName(err)});
                std.process.exit(1);
            };
            print("gemma4_cross_layer0: ok\n", .{});
        }
        if (gguf_meta_path) |path| {
            try runGgufMetaDump(allocator, path);
        }
    }
}

fn smokeCublasLtBf16(allocator: std.mem.Allocator) !bool {
    if (comptime !build_options.enable_cuda) return false;
    var ctx = try cuda_context.CudaContext.initDefault();
    defer ctx.deinit();
    if (ctx.info.compute_major < 8) return false;

    var module = try cuda_kernels.KernelModule.load(&ctx);
    defer module.unload(&ctx);
    var blas = cuda_cublaslt.CublasLt.open(&ctx) catch return false;
    defer blas.deinit();

    const rows: usize = 2;
    const in_dim: usize = 16;
    const out_dim: usize = 16;
    var input_data: [rows * in_dim]f32 = undefined;
    for (&input_data, 0..) |*value, i| value.* = @floatFromInt((i % 7) + 1);
    var weight_data: [out_dim * in_dim]u16 = .{0} ** (out_dim * in_dim);
    for (0..out_dim) |row| weight_data[row * in_dim + row] = bf16Bits(1.0);

    var input = try cuda_buffer.DeviceBuffer.alloc(&ctx, input_data.len * @sizeOf(f32));
    defer input.free(&ctx);
    var input_bf16 = try cuda_buffer.DeviceBuffer.alloc(&ctx, input_data.len * @sizeOf(u16));
    defer input_bf16.free(&ctx);
    var weight = try cuda_buffer.DeviceBuffer.alloc(&ctx, weight_data.len * @sizeOf(u16));
    defer weight.free(&ctx);
    var output = try cuda_buffer.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer output.free(&ctx);

    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(&ctx, std.mem.sliceAsBytes(&weight_data));
    try module.launchF32ToBf16(&ctx, input_bf16, input, input_data.len);
    try blas.matmulBf16WeightF32Out(&ctx, output, input_bf16, weight, .{}, rows, in_dim, out_dim);
    try ctx.synchronize();

    const actual = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(actual);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
    try ctx.synchronize();
    for (actual, input_data) |got, expected| {
        if (@abs(got - expected) > 0.0001) return error.CudaParityMismatch;
    }
    return true;
}

fn bf16Bits(value: f32) u16 {
    return @intCast(@as(u32, @bitCast(value)) >> 16);
}

fn printUsage() void {
    print(
        \\usage: antfly inference cuda-info [--artifact-identity] [--smoke] [--gemma4-parity <gguf>] [--gemma4-hf-parity <model-dir>]
        \\                                      [--gguf-meta <gguf>]
        \\
        \\  --smoke   Run CUDA smoke checks for fill, graph capture, dense/quant kernels, Gemma4 primitives, decoder-runtime slots, and cuBLASLt.
        \\  --artifact-identity
        \\            Print the format, target, byte count, and SHA-256 of the CUDA image embedded in this binary without initializing a GPU.
        \\  --e4b-q8-prefill-smoke
        \\            Run the Gemma 4 E4B row-aware Q4_0 gate/up Q8_1 prefill kernel smoke check.
        \\  --gguf-meta <gguf>
        \\            Dump raw GGUF header metadata and reconstructed GPT config.
        \\  --gemma4-parity <gguf>
        \\            Compare real Gemma 4 GGUF projection tensors on CUDA against CPU dequantized matmul.
        \\  --gemma4-hf-parity <model-dir>
        \\            Compare real Gemma 4 HF safetensors tensors on CUDA against CPU BF16->F32 reference math.
        \\  --gemma4-cross-parity <gguf> <model-dir>
        \\            Compare selected Gemma 4 GGUF tensors against the source HF safetensors tensors.
        \\  --gemma4-cross-rmse <gguf> <model-dir>
        \\            Sweep Gemma 4 GGUF tensors against HF safetensors and print grouped RMSE/relative error.
        \\  --gemma4-cross-layer0 <gguf> <model-dir>
        \\            Compare a CPU layer-0 forward pass for GGUF dequantized weights against HF safetensors.
        \\
    , .{});
}

fn runGgufMetaDump(allocator: std.mem.Allocator, gguf_path: []const u8) !void {
    const store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer store.tensorStore().deinit();

    const file = store.tensorStore().ggufFile() orelse return error.InvalidTensorStore;
    print("gguf_meta: path={s}\n", .{gguf_path});
    print("gguf_meta: version={d} tensors={d} metadata={d} alignment={d} data_region_offset={d}\n", .{
        file.header.version,
        file.header.tensor_count,
        file.header.metadata_count,
        file.alignment,
        file.data_region_offset,
    });
    for (file.metadata) |entry| {
        print("gguf_meta: key={s} type={s} value=", .{ entry.key, @tagName(entry.value) });
        printGgufMetadataValue(entry.value, 0);
        print("\n", .{});
    }

    const view = gguf_mod.metadata.View.init(file);
    if (gpt_mod.parseGgufMetadata(view)) |cfg| {
        var refined_cfg = cfg;
        session_factory.refineGptConfigFromGgufTensorInfo(&refined_cfg, file);
        print("gguf_meta_reconstructed_config: family={s} hidden={d} layers={d} heads={d} kv_heads={d} head_dim={d} global_head_dim={d} global_kv_heads={d} intermediate={d} shared_intermediate={d} vocab={d} context={d} sliding_window={d} sliding_pattern={d} kv_shared_layers={d} rope_theta={d:.9} rope_local_theta={d:.9} rope_partial_factor={d:.9} rope_dim_override={d} norm_eps={d:.9} norm_offset={d:.9} softcap={d:.9} ple_hidden={d} weight_tying={}\n", .{
            @tagName(refined_cfg.family),
            refined_cfg.hidden_size,
            refined_cfg.num_hidden_layers,
            refined_cfg.num_attention_heads,
            refined_cfg.num_key_value_heads,
            refined_cfg.attention_head_dim,
            refined_cfg.global_head_dim,
            refined_cfg.num_global_key_value_heads,
            refined_cfg.intermediate_size,
            refined_cfg.shared_layer_intermediate_size,
            refined_cfg.vocab_size,
            refined_cfg.max_position_embeddings,
            refined_cfg.sliding_window,
            refined_cfg.sliding_window_pattern,
            refined_cfg.num_kv_shared_layers,
            refined_cfg.rope_theta,
            refined_cfg.rope_local_theta,
            refined_cfg.rope_partial_factor,
            refined_cfg.rope_dim_override,
            refined_cfg.norm_eps,
            refined_cfg.norm_weight_offset,
            refined_cfg.final_logit_softcapping,
            refined_cfg.ple_hidden_size,
            refined_cfg.weight_tying,
        });
        if (refined_cfg.gemma4_mtp_assistant) {
            printGgufMtpSummary(file, view, refined_cfg);
        }
    } else {
        print("gguf_meta_reconstructed_config: unavailable\n", .{});
    }
}

fn printGgufMtpSummary(file: *const gguf_mod.format.File, view: gguf_mod.metadata.View, cfg: gpt_mod.Config) void {
    const token_embd = findGgufTensorInfo(file, "token_embd.weight");
    const token_embd_dim0 = if (token_embd) |tensor| if (tensor.dimensions.len > 0) tensor.dimensions[0] else 0 else 0;
    const token_embd_dim1 = if (token_embd) |tensor| if (tensor.dimensions.len > 1) tensor.dimensions[1] else 0 else 0;
    const has_output = findGgufTensorInfo(file, "output.weight") != null;
    const has_masked_centroids = findGgufTensorInfo(file, "masked_embedding.centroids.weight") != null;
    const has_masked_ordering = findGgufTensorInfo(file, "masked_embedding.token_ordering") != null;
    const nextn_layers = view.getU64("gemma4-assistant.nextn_predict_layers") orelse
        view.getU64("gemma4_assistant.nextn_predict_layers") orelse
        view.getU64("gemma4_assistant.nextn_predict_layers") orelse
        @as(u64, cfg.num_hidden_layers);
    print(
        "gguf_meta_mtp: assistant=true vocab={d} tied_embedding={} token_embd_dims=[{d},{d}] nextn_predict_layers={d} backbone_hidden={d} masked_centroids={} masked_ordering={} output_weight={}\n",
        .{
            cfg.vocab_size,
            cfg.weight_tying,
            token_embd_dim0,
            token_embd_dim1,
            nextn_layers,
            cfg.mtp_backbone_hidden_size,
            has_masked_centroids,
            has_masked_ordering,
            has_output,
        },
    );
}

fn findGgufTensorInfo(file: *const gguf_mod.format.File, name: []const u8) ?*const gguf_mod.format.TensorInfo {
    for (file.tensors) |*tensor| {
        if (std.mem.eql(u8, tensor.name, name)) return tensor;
    }
    return null;
}

fn printGgufMetadataValue(value: gguf_mod.format.MetadataValue, depth: usize) void {
    switch (value) {
        .u8 => |v| print("{d}", .{v}),
        .i8 => |v| print("{d}", .{v}),
        .u16 => |v| print("{d}", .{v}),
        .i16 => |v| print("{d}", .{v}),
        .u32 => |v| print("{d}", .{v}),
        .i32 => |v| print("{d}", .{v}),
        .u64 => |v| print("{d}", .{v}),
        .i64 => |v| print("{d}", .{v}),
        .f32 => |v| print("{d:.9}", .{v}),
        .f64 => |v| print("{d:.9}", .{v}),
        .bool_ => |v| print("{}", .{v}),
        .string => |v| printStringPreview(v),
        .array => |arr| {
            print("[type={s} len={d}", .{ @tagName(arr.element_type), arr.values.len });
            const limit = @min(arr.values.len, 64);
            for (arr.values[0..limit]) |item| {
                print(" ", .{});
                if (depth >= 2) {
                    print("...", .{});
                } else {
                    printGgufMetadataValue(item, depth + 1);
                }
            }
            if (arr.values.len > limit) print(" ...", .{});
            print("]", .{});
        },
    }
}

fn printStringPreview(value: []const u8) void {
    const limit = @min(value.len, 256);
    print("\"", .{});
    for (value[0..limit]) |ch| {
        switch (ch) {
            '\n' => print("\\n", .{}),
            '\r' => print("\\r", .{}),
            '\t' => print("\\t", .{}),
            '"' => print("\\\"", .{}),
            '\\' => print("\\\\", .{}),
            else => print("{c}", .{ch}),
        }
    }
    if (value.len > limit) print("...", .{});
    print("\"", .{});
}

const Gemma4ParityCase = struct {
    name: []const u8,
};

const gemma4_parity_cases = [_]Gemma4ParityCase{
    .{ .name = "blk.0.attn_q.weight" },
    .{ .name = "blk.0.attn_k.weight" },
    .{ .name = "blk.0.attn_v.weight" },
    .{ .name = "blk.0.attn_output.weight" },
    .{ .name = "blk.0.ffn_gate.weight" },
    .{ .name = "blk.0.ffn_up.weight" },
    .{ .name = "blk.0.ffn_down.weight" },
    .{ .name = "blk.5.attn_q.weight" },
    .{ .name = "blk.5.attn_k.weight" },
    .{ .name = "blk.5.attn_output.weight" },
    .{ .name = "blk.5.ffn_gate.weight" },
    .{ .name = "blk.5.ffn_up.weight" },
    .{ .name = "blk.5.ffn_down.weight" },
};

const gemma4_cross_parity_cases = [_]Gemma4ParityCase{
    .{ .name = "output_norm.weight" },
    .{ .name = "blk.0.attn_q.weight" },
    .{ .name = "blk.0.attn_output.weight" },
    .{ .name = "blk.0.ffn_gate.weight" },
    .{ .name = "blk.0.ffn_down.weight" },
    .{ .name = "blk.5.attn_q.weight" },
    .{ .name = "blk.5.attn_k.weight" },
    .{ .name = "blk.5.attn_output.weight" },
    .{ .name = "blk.5.ffn_down.weight" },
    .{ .name = "blk.11.attn_q.weight" },
    .{ .name = "blk.11.attn_k.weight" },
    .{ .name = "blk.11.attn_output.weight" },
    .{ .name = "blk.12.ffn_down.weight" },
    .{ .name = "blk.23.ffn_down.weight" },
    .{ .name = "blk.35.ffn_down.weight" },
    .{ .name = "blk.47.attn_q.weight" },
    .{ .name = "blk.47.attn_output.weight" },
};

const DiffStats = struct {
    max_abs: f32 = 0,
    mean_abs: f64 = 0,
    rmse: f64 = 0,
    rel_rmse: f64 = 0,
    rel_mean_abs: f64 = 0,
    actual_rms: f64 = 0,
    expected_rms: f64 = 0,
    sum_abs: f64 = 0,
    sum_sq: f64 = 0,
    expected_abs_sum: f64 = 0,
    expected_sq_sum: f64 = 0,
    actual_sq_sum: f64 = 0,
    max_index: usize = 0,
};

fn runGemma4Parity(allocator: std.mem.Allocator, gguf_path: []const u8) !void {
    const store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer store.tensorStore().deinit();
    try runGemma4ParityOnStore(allocator, store.tensorStore());
}

fn runGemma4HfParity(allocator: std.mem.Allocator, model_dir: []const u8) !void {
    const safetensors_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);
    const single = tensor_store_mod.SafetensorsStore.initAbsolute(allocator, safetensors_path) catch |single_err| blk: {
        if (single_err != error.FileNotFound and single_err != error.NotFound and single_err != error.AccessDenied) return single_err;
        break :blk null;
    };
    if (single) |store| {
        defer store.tensorStore().deinit();
        try runGemma4ParityOnStore(allocator, store.tensorStore());
        return;
    }

    const index_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors.index.json" });
    defer allocator.free(index_path);
    const sharded = try tensor_store_mod.ShardedSafetensorsStore.initAbsolute(allocator, index_path);
    defer sharded.tensorStore().deinit();
    try runGemma4ParityOnStore(allocator, sharded.tensorStore());
}

fn runGemma4CrossParity(allocator: std.mem.Allocator, gguf_path: []const u8, model_dir: []const u8) !void {
    const gguf_store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer gguf_store.tensorStore().deinit();

    const safetensors_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);
    const single = tensor_store_mod.SafetensorsStore.initAbsolute(allocator, safetensors_path) catch |single_err| blk: {
        if (single_err != error.FileNotFound and single_err != error.NotFound and single_err != error.AccessDenied) return single_err;
        break :blk null;
    };
    if (single) |hf_store| {
        defer hf_store.tensorStore().deinit();
        try runGemma4CrossParityOnStores(allocator, gguf_store.tensorStore(), hf_store.tensorStore());
        return;
    }

    const index_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors.index.json" });
    defer allocator.free(index_path);
    const sharded = try tensor_store_mod.ShardedSafetensorsStore.initAbsolute(allocator, index_path);
    defer sharded.tensorStore().deinit();
    try runGemma4CrossParityOnStores(allocator, gguf_store.tensorStore(), sharded.tensorStore());
}

fn runGemma4CrossRmse(allocator: std.mem.Allocator, gguf_path: []const u8, model_dir: []const u8) !void {
    const gguf_store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer gguf_store.tensorStore().deinit();

    const safetensors_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);
    const single = tensor_store_mod.SafetensorsStore.initAbsolute(allocator, safetensors_path) catch |single_err| blk: {
        if (single_err != error.FileNotFound and single_err != error.NotFound and single_err != error.AccessDenied) return single_err;
        break :blk null;
    };
    if (single) |hf_store| {
        defer hf_store.tensorStore().deinit();
        try runGemma4CrossRmseOnStores(allocator, gguf_store.tensorStore(), hf_store.tensorStore());
        return;
    }

    const index_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors.index.json" });
    defer allocator.free(index_path);
    const sharded = try tensor_store_mod.ShardedSafetensorsStore.initAbsolute(allocator, index_path);
    defer sharded.tensorStore().deinit();
    try runGemma4CrossRmseOnStores(allocator, gguf_store.tensorStore(), sharded.tensorStore());
}

fn runGemma4CrossLayer0(allocator: std.mem.Allocator, gguf_path: []const u8, model_dir: []const u8) !void {
    const gguf_store = try tensor_store_mod.GgufStore.initAbsolute(allocator, gguf_path);
    defer gguf_store.tensorStore().deinit();

    const safetensors_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors" });
    defer allocator.free(safetensors_path);
    const single = tensor_store_mod.SafetensorsStore.initAbsolute(allocator, safetensors_path) catch |single_err| blk: {
        if (single_err != error.FileNotFound and single_err != error.NotFound and single_err != error.AccessDenied) return single_err;
        break :blk null;
    };
    if (single) |hf_store| {
        defer hf_store.tensorStore().deinit();
        try runGemma4CrossLayer0OnStores(allocator, gguf_store.tensorStore(), hf_store.tensorStore());
        return;
    }

    const index_path = try std.fs.path.join(allocator, &.{ model_dir, "model.safetensors.index.json" });
    defer allocator.free(index_path);
    const sharded = try tensor_store_mod.ShardedSafetensorsStore.initAbsolute(allocator, index_path);
    defer sharded.tensorStore().deinit();
    try runGemma4CrossLayer0OnStores(allocator, gguf_store.tensorStore(), sharded.tensorStore());
}

const Gemma4RmseCategory = enum {
    token_embd,
    output_norm,
    norm,
    layer_scale,
    attn_q,
    attn_k,
    attn_v,
    attn_output,
    ffn_gate,
    ffn_up,
    ffn_down,
    other,
};

const gemma4_rmse_category_count = @typeInfo(Gemma4RmseCategory).@"enum".fields.len;

const RmseAccumulator = struct {
    tensor_count: usize = 0,
    sample_count: usize = 0,
    elem_count: usize = 0,
    sum_abs: f64 = 0,
    sum_sq: f64 = 0,
    expected_sq_sum: f64 = 0,
    actual_sq_sum: f64 = 0,
    max_abs: f32 = 0,
    max_tensor: []const u8 = "",
    max_index: usize = 0,

    fn add(self: *RmseAccumulator, tensor_name: []const u8, stats: DiffStats, elem_count: usize, sampled: bool) void {
        self.elem_count += elem_count;
        self.sum_abs += stats.sum_abs;
        self.sum_sq += stats.sum_sq;
        self.expected_sq_sum += stats.expected_sq_sum;
        self.actual_sq_sum += stats.actual_sq_sum;
        if (sampled) {
            self.sample_count += 1;
        } else {
            self.tensor_count += 1;
        }
        if (stats.max_abs > self.max_abs) {
            self.max_abs = stats.max_abs;
            self.max_tensor = tensor_name;
            self.max_index = stats.max_index;
        }
    }
};

fn runGemma4CrossRmseOnStores(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
) !void {
    const gguf_file = gguf_store.ggufFile() orelse return error.InvalidTensorStore;
    var groups = [_]RmseAccumulator{.{}} ** gemma4_rmse_category_count;
    var skipped: usize = 0;
    const verbose = envFlag("ANTFLY_INFERENCE_CUDA_RMSE_VERBOSE");
    const max_full_elements = envUsize("ANTFLY_INFERENCE_CUDA_RMSE_MAX_ELEMENTS") orelse 40_000_000;
    print("gemma4_cross_rmse: tensors={d} verbose={} max_full_elements={d}\n", .{ gguf_file.tensors.len, verbose, max_full_elements });

    for (gguf_file.tensors) |tensor| {
        const category = gemma4RmseCategoryForName(tensor.name);
        if (std.mem.eql(u8, tensor.name, "token_embd.weight")) {
            try runGemma4EmbeddingRowRmse(allocator, gguf_store, hf_store, &groups[@intFromEnum(category)], verbose);
            continue;
        }

        const hf_name = resolveGemma4ParityTensorName(allocator, hf_store, tensor.name) catch |err| switch (err) {
            error.TensorNotFound, error.InvalidTensorName => {
                skipped += 1;
                continue;
            },
            else => return err,
        };
        defer allocator.free(hf_name);

        const elem_count = try ggufTensorElementCount(tensor.dimensions);
        if (elem_count > max_full_elements) {
            if (try runGemma4LargeTensorSampleRmse(
                allocator,
                gguf_store,
                hf_store,
                tensor.name,
                hf_name,
                @tagName(category),
                tensor.tensor_type.name(),
                elem_count,
                &groups[@intFromEnum(category)],
                verbose,
            )) {
                print(
                    "gemma4_cross_rmse_tensor: name={s} source={s} category={s} type={s} status=sampled reason=too_large elems={d} max_full_elements={d}\n",
                    .{ tensor.name, hf_name, @tagName(category), tensor.tensor_type.name(), elem_count, max_full_elements },
                );
            } else {
                print(
                    "gemma4_cross_rmse_tensor: name={s} source={s} category={s} type={s} status=skipped reason=too_large elems={d} max_full_elements={d}\n",
                    .{ tensor.name, hf_name, @tagName(category), tensor.tensor_type.name(), elem_count, max_full_elements },
                );
                skipped += 1;
            }
            continue;
        }

        var hf_range_opt = hf_store.describeTensorRange(allocator, hf_name) catch |err| switch (err) {
            error.TensorNotFound => {
                skipped += 1;
                continue;
            },
            else => return err,
        };
        defer if (hf_range_opt) |*hf_range| hf_range.deinit(allocator);
        if (hf_range_opt) |hf_range| {
            if (!sameGgufDimensionsShape(tensor.dimensions, hf_range.shape)) {
                skipped += 1;
                continue;
            }
        }

        var gguf_ref = gguf_store.describeTensor(allocator, tensor.name) catch |err| switch (err) {
            error.TensorNotFound, error.UnsupportedTensorType => {
                skipped += 1;
                continue;
            },
            else => return err,
        };
        defer gguf_ref.deinit(allocator);
        var hf_ref = hf_store.describeTensor(allocator, hf_name) catch |err| switch (err) {
            error.TensorNotFound => {
                skipped += 1;
                continue;
            },
            else => return err,
        };
        defer hf_ref.deinit(allocator);

        var gguf_loaded = try gguf_store.loadTensorRef(&gguf_ref);
        defer gguf_loaded.deinit();
        var hf_loaded = try hf_store.loadTensorRef(&hf_ref);
        defer hf_loaded.deinit();
        const gguf_values = try tensorToFloat32Owned(allocator, &gguf_loaded.tensor);
        defer allocator.free(gguf_values);
        const hf_values = try tensorToFloat32Owned(allocator, &hf_loaded.tensor);
        defer allocator.free(hf_values);
        if (gguf_values.len != hf_values.len) return error.InvalidTensorShape;
        const stats = diffStats(gguf_values, hf_values);
        groups[@intFromEnum(category)].add(tensor.name, stats, elem_count, false);
        if (verbose) {
            print(
                "gemma4_cross_rmse_tensor: name={s} source={s} category={s} type={s} elems={d} max_abs={d:.6} mean_abs={d:.6} rmse={d:.6} rel_rmse={d:.6} actual_rms={d:.6} expected_rms={d:.6} max_index={d}\n",
                .{
                    tensor.name,
                    hf_name,
                    @tagName(category),
                    tensor.tensor_type.name(),
                    elem_count,
                    stats.max_abs,
                    stats.mean_abs,
                    stats.rmse,
                    stats.rel_rmse,
                    stats.actual_rms,
                    stats.expected_rms,
                    stats.max_index,
                },
            );
        }
    }

    for (&groups, 0..) |*group, idx| {
        if (group.elem_count == 0) continue;
        const category: Gemma4RmseCategory = @enumFromInt(idx);
        const count: f64 = @floatFromInt(group.elem_count);
        const mean_abs = group.sum_abs / count;
        const rmse = @sqrt(group.sum_sq / count);
        const rel_rmse = if (group.expected_sq_sum > 1e-24) @sqrt(group.sum_sq) / @sqrt(group.expected_sq_sum) else 0;
        const actual_rms = @sqrt(group.actual_sq_sum / count);
        const expected_rms = @sqrt(group.expected_sq_sum / count);
        print(
            "gemma4_cross_rmse_group: category={s} tensors={d} samples={d} elems={d} max_abs={d:.6} max_tensor={s} max_index={d} mean_abs={d:.6} rmse={d:.6} rel_rmse={d:.6} actual_rms={d:.6} expected_rms={d:.6}\n",
            .{
                @tagName(category),
                group.tensor_count,
                group.sample_count,
                group.elem_count,
                group.max_abs,
                group.max_tensor,
                group.max_index,
                mean_abs,
                rmse,
                rel_rmse,
                actual_rms,
                expected_rms,
            },
        );
    }
    print("gemma4_cross_rmse: skipped={d}\n", .{skipped});
}

fn runGemma4EmbeddingRowRmse(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
    group: *RmseAccumulator,
    verbose: bool,
) !void {
    const gguf_name = "token_embd.weight";
    const hf_name = try resolveGemma4ParityTensorName(allocator, hf_store, gguf_name);
    defer allocator.free(hf_name);

    var gguf_ref = try gguf_store.describeTensor(allocator, gguf_name);
    defer gguf_ref.deinit(allocator);
    var hf_ref = try hf_store.describeTensor(allocator, hf_name);
    defer hf_ref.deinit(allocator);
    var hf_loaded = try hf_store.loadTensorRef(&hf_ref);
    defer hf_loaded.deinit();
    if (hf_loaded.tensor.shape.len != 2) return error.InvalidTensorShape;
    const vocab: usize = @intCast(hf_loaded.tensor.shape[0]);
    const hidden: usize = @intCast(hf_loaded.tensor.shape[1]);

    const sampled_ids = [_]usize{ 0, 1, 2, 100, 101, 105, 107, 14054, 45518, 236751, 236761, 236770, 258882 };
    const gguf_row = try allocator.alloc(f32, hidden);
    defer allocator.free(gguf_row);
    const hf_row = try allocator.alloc(f32, hidden);
    defer allocator.free(hf_row);

    if (try gguf_store.loadQuantizedStorageRef(&gguf_ref)) |storage_value| {
        var storage = storage_value;
        defer storage.deinit();
        if (storage.shape.len != 2) return error.InvalidTensorShape;
        if (@as(usize, @intCast(storage.shape[0])) != vocab or @as(usize, @intCast(storage.shape[1])) != hidden) return error.InvalidTensorShape;
        const values_per_block = gguf_mod.tensor_types.valuesPerBlock(storage.tensor_type) orelse return error.UnsupportedTensorType;
        const bytes_per_block = gguf_mod.tensor_types.bytesPerBlock(storage.tensor_type) orelse return error.UnsupportedTensorType;
        if (hidden == 0 or hidden % values_per_block != 0) return error.InvalidTensorShape;
        const row_bytes = (hidden / values_per_block) * bytes_per_block;
        if (storage.raw_bytes.len < vocab * row_bytes) return error.InvalidTensorShape;
        for (sampled_ids) |token_id| {
            if (token_id >= vocab) continue;
            const raw = storage.raw_bytes[token_id * row_bytes ..][0..row_bytes];
            try gguf_mod.quant_codec.dequantizeToFloat32(storage.tensor_type, raw, gguf_row);
            try tensorRowToFloat32(&hf_loaded.tensor, token_id, hidden, hf_row);
            const stats = diffStats(gguf_row, hf_row);
            group.add(gguf_name, stats, hidden, true);
            if (verbose) printGemma4EmbeddingRmseRow(token_id, hf_name, storage.tensor_type.name(), hidden, stats);
        }
        return;
    }

    var gguf_loaded = try gguf_store.loadTensorRef(&gguf_ref);
    defer gguf_loaded.deinit();
    if (gguf_loaded.tensor.shape.len != 2) return error.InvalidTensorShape;
    if (@as(usize, @intCast(gguf_loaded.tensor.shape[0])) != vocab or @as(usize, @intCast(gguf_loaded.tensor.shape[1])) != hidden) return error.InvalidTensorShape;
    for (sampled_ids) |token_id| {
        if (token_id >= vocab) continue;
        try tensorRowToFloat32(&gguf_loaded.tensor, token_id, hidden, gguf_row);
        try tensorRowToFloat32(&hf_loaded.tensor, token_id, hidden, hf_row);
        const stats = diffStats(gguf_row, hf_row);
        group.add(gguf_name, stats, hidden, true);
        if (verbose) printGemma4EmbeddingRmseRow(token_id, hf_name, "dense", hidden, stats);
    }
}

fn runGemma4LargeTensorSampleRmse(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
    gguf_name: []const u8,
    hf_name: []const u8,
    category_name: []const u8,
    tensor_type_name: []const u8,
    full_elem_count: usize,
    group: *RmseAccumulator,
    verbose: bool,
) !bool {
    var gguf_ref = gguf_store.describeTensor(allocator, gguf_name) catch |err| switch (err) {
        error.TensorNotFound, error.UnsupportedTensorType => return false,
        else => return err,
    };
    defer gguf_ref.deinit(allocator);
    var storage = (try gguf_store.loadQuantizedStorageRef(&gguf_ref)) orelse return false;
    defer storage.deinit();
    if (storage.shape.len != 2) return false;
    const rows: usize = @intCast(storage.shape[0]);
    const cols: usize = @intCast(storage.shape[1]);
    if (rows == 0 or cols == 0) return false;

    const values_per_block = gguf_mod.tensor_types.valuesPerBlock(storage.tensor_type) orelse return false;
    const bytes_per_block = gguf_mod.tensor_types.bytesPerBlock(storage.tensor_type) orelse return false;
    if (cols % values_per_block != 0) return false;
    const row_bytes = (cols / values_per_block) * bytes_per_block;
    if (storage.raw_bytes.len < rows * row_bytes) return false;

    var hf_ref = hf_store.describeTensor(allocator, hf_name) catch |err| switch (err) {
        error.TensorNotFound => return false,
        else => return err,
    };
    defer hf_ref.deinit(allocator);
    var hf_loaded = try hf_store.loadTensorRef(&hf_ref);
    defer hf_loaded.deinit();
    if (hf_loaded.tensor.shape.len != 2) return false;
    if (@as(usize, @intCast(hf_loaded.tensor.shape[0])) != rows or @as(usize, @intCast(hf_loaded.tensor.shape[1])) != cols) return false;

    const gguf_row = try allocator.alloc(f32, cols);
    defer allocator.free(gguf_row);
    const hf_row = try allocator.alloc(f32, cols);
    defer allocator.free(hf_row);

    const candidates = [_]usize{
        0,
        1,
        2,
        rows / 8,
        rows / 4,
        rows / 2,
        (rows * 3) / 4,
        rows - 2,
        rows - 1,
    };
    var sampled: usize = 0;
    for (candidates) |row| {
        if (row >= rows) continue;
        var duplicate = false;
        for (candidates[0..sampled]) |prev| {
            if (prev == row) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) continue;

        const raw = storage.raw_bytes[row * row_bytes ..][0..row_bytes];
        try gguf_mod.quant_codec.dequantizeToFloat32(storage.tensor_type, raw, gguf_row);
        try tensorRowToFloat32(&hf_loaded.tensor, row, cols, hf_row);
        const stats = diffStats(gguf_row, hf_row);
        group.add(gguf_name, stats, cols, true);
        sampled += 1;
        if (verbose) {
            print(
                "gemma4_cross_rmse_tensor: name={s} row={d} source={s} category={s} type={s} elems={d} full_elems={d} sampled=true max_abs={d:.6} mean_abs={d:.6} rmse={d:.6} rel_rmse={d:.6} actual_rms={d:.6} expected_rms={d:.6} max_index={d}\n",
                .{
                    gguf_name,
                    row,
                    hf_name,
                    category_name,
                    tensor_type_name,
                    cols,
                    full_elem_count,
                    stats.max_abs,
                    stats.mean_abs,
                    stats.rmse,
                    stats.rel_rmse,
                    stats.actual_rms,
                    stats.expected_rms,
                    stats.max_index,
                },
            );
        }
    }
    return sampled > 0;
}

fn envFlag(comptime name: [:0]const u8) bool {
    const raw = std.c.getenv(name) orelse return false;
    const value = std.mem.span(raw);
    if (value.len == 0) return false;
    return !(std.mem.eql(u8, value, "0") or
        std.ascii.eqlIgnoreCase(value, "false") or
        std.ascii.eqlIgnoreCase(value, "no") or
        std.ascii.eqlIgnoreCase(value, "off"));
}

fn envUsize(comptime name: [:0]const u8) ?usize {
    const raw = std.c.getenv(name) orelse return null;
    const value = std.mem.span(raw);
    if (value.len == 0) return null;
    return std.fmt.parseInt(usize, value, 10) catch null;
}

fn printGemma4EmbeddingRmseRow(token_id: usize, hf_name: []const u8, tensor_type: []const u8, hidden: usize, stats: DiffStats) void {
    print(
        "gemma4_cross_rmse_tensor: name=token_embd.weight token={d} source={s} category=token_embd type={s} elems={d} sampled=true max_abs={d:.6} mean_abs={d:.6} rmse={d:.6} rel_rmse={d:.6} actual_rms={d:.6} expected_rms={d:.6} max_index={d}\n",
        .{
            token_id,
            hf_name,
            tensor_type,
            hidden,
            stats.max_abs,
            stats.mean_abs,
            stats.rmse,
            stats.rel_rmse,
            stats.actual_rms,
            stats.expected_rms,
            stats.max_index,
        },
    );
}

fn gemma4RmseCategoryForName(name: []const u8) Gemma4RmseCategory {
    if (std.mem.eql(u8, name, "token_embd.weight")) return .token_embd;
    if (std.mem.eql(u8, name, "output_norm.weight")) return .output_norm;
    if (std.mem.indexOf(u8, name, "_norm.weight") != null or std.mem.indexOf(u8, name, "norm.weight") != null) return .norm;
    if (std.mem.indexOf(u8, name, "layer_output_scale.weight") != null) return .layer_scale;
    if (std.mem.indexOf(u8, name, ".attn_q.weight") != null) return .attn_q;
    if (std.mem.indexOf(u8, name, ".attn_k.weight") != null) return .attn_k;
    if (std.mem.indexOf(u8, name, ".attn_v.weight") != null) return .attn_v;
    if (std.mem.indexOf(u8, name, ".attn_output.weight") != null) return .attn_output;
    if (std.mem.indexOf(u8, name, ".ffn_gate.weight") != null) return .ffn_gate;
    if (std.mem.indexOf(u8, name, ".ffn_up.weight") != null) return .ffn_up;
    if (std.mem.indexOf(u8, name, ".ffn_down.weight") != null) return .ffn_down;
    return .other;
}

fn sameShape(lhs: []const i64, rhs: []const i64) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |l, r| {
        if (l != r) return false;
    }
    return true;
}

fn sameGgufDimensionsShape(dimensions: []const u64, shape: []const i64) bool {
    if (dimensions.len != shape.len) return false;
    for (shape, 0..) |dim, idx| {
        if (dim < 0) return false;
        const gguf_dim = dimensions[dimensions.len - 1 - idx];
        if (gguf_dim != @as(u64, @intCast(dim))) return false;
    }
    return true;
}

fn tensorElementCount(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidTensorShape;
        count = try std.math.mul(usize, count, @intCast(dim));
    }
    return count;
}

fn ggufTensorElementCount(dimensions: []const u64) !usize {
    var count: usize = 1;
    for (dimensions) |dim| {
        count = try std.math.mul(usize, count, @intCast(dim));
    }
    return count;
}

const Gemma4Layer0CpuOutputs = struct {
    attn_norm: []f32,
    q: []f32,
    k: []f32,
    v: []f32,
    q_rope: []f32,
    k_rope: []f32,
    v_norm: []f32,
    attn_out: []f32,
    attn_proj: []f32,
    attn_post: []f32,
    attn_residual: []f32,
    ffn_norm: []f32,
    ffn_gate: []f32,
    ffn_up: []f32,
    ffn_gated: []f32,
    ffn_raw: []f32,
    ffn_post: []f32,
    out: []f32,

    fn deinit(self: *Gemma4Layer0CpuOutputs, allocator: std.mem.Allocator) void {
        allocator.free(self.attn_norm);
        allocator.free(self.q);
        allocator.free(self.k);
        allocator.free(self.v);
        allocator.free(self.q_rope);
        allocator.free(self.k_rope);
        allocator.free(self.v_norm);
        allocator.free(self.attn_out);
        allocator.free(self.attn_proj);
        allocator.free(self.attn_post);
        allocator.free(self.attn_residual);
        allocator.free(self.ffn_norm);
        allocator.free(self.ffn_gate);
        allocator.free(self.ffn_up);
        allocator.free(self.ffn_gated);
        allocator.free(self.ffn_raw);
        allocator.free(self.ffn_post);
        allocator.free(self.out);
        self.* = undefined;
    }
};

fn runGemma4CrossLayer0OnStores(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
) !void {
    const rows: usize = 15;
    const hidden: usize = 3840;
    const input = try makeParityInput(allocator, rows * hidden);
    defer allocator.free(input);

    var hf_weights = try loadGemma4Layer0WeightsHostOnly(allocator, hf_store);
    defer hf_weights.deinit(allocator);
    var hf = try computeGemma4Layer0Cpu(allocator, &hf_weights, input, rows, true);
    defer hf.deinit(allocator);

    var gguf_weights = try loadGemma4Layer0WeightsHostOnly(allocator, gguf_store);
    defer gguf_weights.deinit(allocator);
    var gguf = try computeGemma4Layer0Cpu(allocator, &gguf_weights, input, rows, false);
    defer gguf.deinit(allocator);

    printGemma4CrossLayerStage("attn_norm", gguf.attn_norm, hf.attn_norm);
    printGemma4CrossLayerStage("q", gguf.q, hf.q);
    printGemma4CrossLayerStage("k", gguf.k, hf.k);
    printGemma4CrossLayerStage("v", gguf.v, hf.v);
    printGemma4CrossLayerStage("q_rope", gguf.q_rope, hf.q_rope);
    printGemma4CrossLayerStage("k_rope", gguf.k_rope, hf.k_rope);
    printGemma4CrossLayerStage("v_norm", gguf.v_norm, hf.v_norm);
    printGemma4CrossLayerStage("attn_out", gguf.attn_out, hf.attn_out);
    printGemma4CrossLayerStage("attn_proj", gguf.attn_proj, hf.attn_proj);
    printGemma4CrossLayerStage("attn_post", gguf.attn_post, hf.attn_post);
    printGemma4CrossLayerStage("attn_residual", gguf.attn_residual, hf.attn_residual);
    printGemma4CrossLayerStage("ffn_norm", gguf.ffn_norm, hf.ffn_norm);
    printGemma4CrossLayerStage("ffn_gate", gguf.ffn_gate, hf.ffn_gate);
    printGemma4CrossLayerStage("ffn_up", gguf.ffn_up, hf.ffn_up);
    printGemma4CrossLayerStage("ffn_gated", gguf.ffn_gated, hf.ffn_gated);
    printGemma4CrossLayerStage("ffn_raw", gguf.ffn_raw, hf.ffn_raw);
    printGemma4CrossLayerStage("ffn_post", gguf.ffn_post, hf.ffn_post);
    printGemma4CrossLayerStage("out", gguf.out, hf.out);
}

fn printGemma4CrossLayerStage(stage: []const u8, actual: []const f32, expected: []const f32) void {
    if (actual.len != expected.len) {
        print("gemma4_cross_layer0: stage={s} shape_mismatch actual={d} expected={d}\n", .{ stage, actual.len, expected.len });
        return;
    }
    const stats = diffStats(actual, expected);
    print(
        "gemma4_cross_layer0: stage={s} max_abs={d:.6} mean_abs={d:.6} rmse={d:.6} rel_rmse={d:.6} rel_mean_abs={d:.6} actual_rms={d:.6} expected_rms={d:.6} max_index={d} actual={d:.6} expected={d:.6}\n",
        .{
            stage,
            stats.max_abs,
            stats.mean_abs,
            stats.rmse,
            stats.rel_rmse,
            stats.rel_mean_abs,
            stats.actual_rms,
            stats.expected_rms,
            stats.max_index,
            actual[stats.max_index],
            expected[stats.max_index],
        },
    );
}

fn loadGemma4Layer0WeightsHostOnly(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
) !Gemma4Layer0Weights {
    return .{
        .attn_norm = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_norm.weight"),
        .q = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_q.weight"),
        .k = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_k.weight"),
        .v = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_v.weight"),
        .q_norm = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_q_norm.weight"),
        .k_norm = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_k_norm.weight"),
        .attn_output = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.attn_output.weight"),
        .post_attention_norm = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.post_attention_norm.weight"),
        .ffn_norm = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.ffn_norm.weight"),
        .ffn_gate = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.ffn_gate.weight"),
        .ffn_up = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.ffn_up.weight"),
        .ffn_down = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.ffn_down.weight"),
        .post_ffw_norm = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.post_ffw_norm.weight"),
        .layer_output_scale = try loadLayerWeightHostOnly(allocator, tensor_store, "blk.0.layer_output_scale.weight"),
    };
}

fn loadLayerWeightHostOnly(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
    name: []const u8,
) ![]f32 {
    const source_name = try resolveGemma4ParityTensorName(allocator, tensor_store, name);
    defer allocator.free(source_name);
    var tensor_ref = try tensor_store.describeTensor(allocator, source_name);
    defer tensor_ref.deinit(allocator);
    var loaded = try tensor_store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    return try tensorToFloat32Owned(allocator, &loaded.tensor);
}

fn computeGemma4Layer0Cpu(
    allocator: std.mem.Allocator,
    weights: *const Gemma4Layer0Weights,
    input: []const f32,
    rows: usize,
    bf16_linear_inputs: bool,
) !Gemma4Layer0CpuOutputs {
    const hidden: usize = 3840;
    const q_dim: usize = 4096;
    const kv_dim: usize = 2048;
    const num_heads: usize = 16;
    const num_kv_heads: usize = 8;
    const head_dim: usize = 256;
    const inter: usize = 15360;
    const eps: f32 = 0.000001;
    const theta: f32 = 10000.0;
    if (input.len != rows * hidden) return error.InvalidTensorShape;

    var out = Gemma4Layer0CpuOutputs{
        .attn_norm = try allocator.alloc(f32, rows * hidden),
        .q = try allocator.alloc(f32, rows * q_dim),
        .k = try allocator.alloc(f32, rows * kv_dim),
        .v = try allocator.alloc(f32, rows * kv_dim),
        .q_rope = try allocator.alloc(f32, rows * q_dim),
        .k_rope = try allocator.alloc(f32, rows * kv_dim),
        .v_norm = try allocator.alloc(f32, rows * kv_dim),
        .attn_out = try allocator.alloc(f32, rows * q_dim),
        .attn_proj = try allocator.alloc(f32, rows * hidden),
        .attn_post = try allocator.alloc(f32, rows * hidden),
        .attn_residual = try allocator.alloc(f32, rows * hidden),
        .ffn_norm = try allocator.alloc(f32, rows * hidden),
        .ffn_gate = try allocator.alloc(f32, rows * inter),
        .ffn_up = try allocator.alloc(f32, rows * inter),
        .ffn_gated = try allocator.alloc(f32, rows * inter),
        .ffn_raw = try allocator.alloc(f32, rows * hidden),
        .ffn_post = try allocator.alloc(f32, rows * hidden),
        .out = try allocator.alloc(f32, rows * hidden),
    };
    errdefer out.deinit(allocator);

    cpuRmsNormRows(input, weights.attn_norm, out.attn_norm, rows, hidden, eps);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.attn_norm, weights.q, out.q, rows, hidden, q_dim);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.attn_norm, weights.k, out.k, rows, hidden, kv_dim);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.attn_norm, weights.v, out.v, rows, hidden, kv_dim);
    cpuRmsNormHeadsRope(out.q, weights.q_norm, out.q_rope, rows, q_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, @sqrt(@as(f32, @floatFromInt(head_dim))));
    cpuRmsNormHeadsRope(out.k, weights.k_norm, out.k_rope, rows, kv_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, 1.0);
    cpuRmsNormBareRows(out.v, out.v_norm, rows * num_kv_heads, head_dim, eps);
    cpuGqaCausalAttention(out.q_rope, out.k_rope, out.v_norm, out.attn_out, 1, rows, num_heads, num_kv_heads, head_dim);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.attn_out, weights.attn_output, out.attn_proj, rows, q_dim, hidden);
    cpuRmsNormRows(out.attn_proj, weights.post_attention_norm, out.attn_post, rows, hidden, eps);
    cpuAdd(out.attn_post, input, out.attn_residual);
    cpuRmsNormRows(out.attn_residual, weights.ffn_norm, out.ffn_norm, rows, hidden, eps);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.ffn_norm, weights.ffn_gate, out.ffn_gate, rows, hidden, inter);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.ffn_norm, weights.ffn_up, out.ffn_up, rows, hidden, inter);
    cpuSiluMultiply(out.ffn_gate, out.ffn_up, out.ffn_gated);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, out.ffn_gated, weights.ffn_down, out.ffn_raw, rows, inter, hidden);
    cpuRmsNormRows(out.ffn_raw, weights.post_ffw_norm, out.ffn_post, rows, hidden, eps);
    cpuAddScaled(out.ffn_post, out.attn_residual, weights.layer_output_scale[0], out.out);
    return out;
}

fn runGemma4CrossParityOnStores(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
) !void {
    for (gemma4_cross_parity_cases) |case| {
        try runGemma4CrossParityCase(allocator, gguf_store, hf_store, case);
    }
    try runGemma4EmbeddingRowCrossParity(allocator, gguf_store, hf_store);
}

fn runGemma4CrossParityCase(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
    case: Gemma4ParityCase,
) !void {
    var gguf_ref = try gguf_store.describeTensor(allocator, case.name);
    defer gguf_ref.deinit(allocator);
    var gguf_loaded = try gguf_store.loadTensorRef(&gguf_ref);
    defer gguf_loaded.deinit();

    const hf_name = try resolveGemma4ParityTensorName(allocator, hf_store, case.name);
    defer allocator.free(hf_name);
    var hf_ref = try hf_store.describeTensor(allocator, hf_name);
    defer hf_ref.deinit(allocator);
    var hf_loaded = try hf_store.loadTensorRef(&hf_ref);
    defer hf_loaded.deinit();

    if (gguf_loaded.tensor.shape.len != hf_loaded.tensor.shape.len) return error.InvalidTensorShape;
    for (gguf_loaded.tensor.shape, hf_loaded.tensor.shape) |lhs, rhs| {
        if (lhs != rhs) return error.InvalidTensorShape;
    }
    if (gguf_loaded.tensor.shape.len == 1) {
        const gguf_values = try tensorToFloat32Owned(allocator, &gguf_loaded.tensor);
        defer allocator.free(gguf_values);
        const hf_values = try tensorToFloat32Owned(allocator, &hf_loaded.tensor);
        defer allocator.free(hf_values);
        if (gguf_values.len != hf_values.len) return error.InvalidTensorShape;
        const weight_stats = diffStats(gguf_values, hf_values);
        const quant_name = if (gguf_loaded.quantized_storage) |storage| storage.tensor_type.name() else "dense";
        print(
            "gemma4_cross_parity: {s} source={s} type={s} shape=[{d}] weight_max_abs={d:.6} weight_mean_abs={d:.6} weight_rmse={d:.6} weight_rel_rmse={d:.6} weight_max_index={d}\n",
            .{
                case.name,
                hf_name,
                quant_name,
                gguf_values.len,
                weight_stats.max_abs,
                weight_stats.mean_abs,
                weight_stats.rmse,
                weight_stats.rel_rmse,
                weight_stats.max_index,
            },
        );
        return;
    }
    if (gguf_loaded.tensor.shape.len != 2) return error.InvalidTensorShape;
    const out_dim: usize = @intCast(gguf_loaded.tensor.shape[0]);
    const in_dim: usize = @intCast(gguf_loaded.tensor.shape[1]);

    const gguf_values = try tensorToFloat32Owned(allocator, &gguf_loaded.tensor);
    defer allocator.free(gguf_values);
    const hf_values = try tensorToFloat32Owned(allocator, &hf_loaded.tensor);
    defer allocator.free(hf_values);
    if (gguf_values.len != hf_values.len) return error.InvalidTensorShape;

    const weight_stats = diffStats(gguf_values, hf_values);

    const input = try makeParityInput(allocator, in_dim);
    defer allocator.free(input);
    const gguf_out = try allocator.alloc(f32, out_dim);
    defer allocator.free(gguf_out);
    const hf_out = try allocator.alloc(f32, out_dim);
    defer allocator.free(hf_out);
    cpuLinearNoBias(input, gguf_values, gguf_out, in_dim, out_dim);
    cpuLinearNoBias(input, hf_values, hf_out, in_dim, out_dim);
    const linear_stats = diffStats(gguf_out, hf_out);

    const quant_name = if (gguf_loaded.quantized_storage) |storage| storage.tensor_type.name() else "dense";
    print(
        "gemma4_cross_parity: {s} source={s} type={s} shape=[{d},{d}] weight_max_abs={d:.6} weight_mean_abs={d:.6} weight_rmse={d:.6} weight_rel_rmse={d:.6} weight_max_index={d} linear_max_abs={d:.6} linear_mean_abs={d:.6} linear_rmse={d:.6} linear_rel_rmse={d:.6} linear_max_index={d}\n",
        .{
            case.name,
            hf_name,
            quant_name,
            out_dim,
            in_dim,
            weight_stats.max_abs,
            weight_stats.mean_abs,
            weight_stats.rmse,
            weight_stats.rel_rmse,
            weight_stats.max_index,
            linear_stats.max_abs,
            linear_stats.mean_abs,
            linear_stats.rmse,
            linear_stats.rel_rmse,
            linear_stats.max_index,
        },
    );
}

fn runGemma4EmbeddingRowCrossParity(
    allocator: std.mem.Allocator,
    gguf_store: tensor_store_mod.TensorStore,
    hf_store: tensor_store_mod.TensorStore,
) !void {
    const gguf_name = "token_embd.weight";
    const hf_name = try resolveGemma4ParityTensorName(allocator, hf_store, gguf_name);
    defer allocator.free(hf_name);

    var gguf_ref = try gguf_store.describeTensor(allocator, gguf_name);
    defer gguf_ref.deinit(allocator);
    var storage = (try gguf_store.loadQuantizedStorageRef(&gguf_ref)) orelse return error.UnsupportedTensorType;
    defer storage.deinit();
    if (storage.shape.len != 2) return error.InvalidTensorShape;
    const vocab: usize = @intCast(storage.shape[0]);
    const hidden: usize = @intCast(storage.shape[1]);
    const values_per_block = gguf_mod.tensor_types.valuesPerBlock(storage.tensor_type) orelse return error.UnsupportedTensorType;
    const bytes_per_block = gguf_mod.tensor_types.bytesPerBlock(storage.tensor_type) orelse return error.UnsupportedTensorType;
    if (hidden == 0 or hidden % values_per_block != 0) return error.InvalidTensorShape;
    const row_blocks = hidden / values_per_block;
    const row_bytes = row_blocks * bytes_per_block;
    if (storage.raw_bytes.len < vocab * row_bytes) return error.InvalidTensorShape;

    var hf_ref = try hf_store.describeTensor(allocator, hf_name);
    defer hf_ref.deinit(allocator);
    var hf_loaded = try hf_store.loadTensorRef(&hf_ref);
    defer hf_loaded.deinit();
    if (hf_loaded.tensor.shape.len != 2) return error.InvalidTensorShape;
    if (@as(usize, @intCast(hf_loaded.tensor.shape[0])) != vocab or @as(usize, @intCast(hf_loaded.tensor.shape[1])) != hidden) {
        return error.InvalidTensorShape;
    }

    const sampled_ids = [_]usize{ 0, 1, 2, 100, 101, 105, 107, 14054, 45518, 236751, 236761, 236770, 258882 };
    const gguf_row = try allocator.alloc(f32, hidden);
    defer allocator.free(gguf_row);
    const hf_row = try allocator.alloc(f32, hidden);
    defer allocator.free(hf_row);
    const probe = try makeParityInput(allocator, hidden);
    defer allocator.free(probe);

    for (sampled_ids) |token_id| {
        if (token_id >= vocab) continue;
        const raw = storage.raw_bytes[token_id * row_bytes ..][0..row_bytes];
        try gguf_mod.quant_codec.dequantizeToFloat32(storage.tensor_type, raw, gguf_row);
        try tensorRowToFloat32(&hf_loaded.tensor, token_id, hidden, hf_row);
        const stats = diffStats(gguf_row, hf_row);
        const gguf_dot = cpuEmbeddingRowDot(probe, gguf_row, 0, hidden);
        const hf_dot = cpuEmbeddingRowDot(probe, hf_row, 0, hidden);
        print(
            "gemma4_cross_parity: token_embd.row token={d} source={s} type={s} weight_max_abs={d:.6} weight_mean_abs={d:.6} weight_rmse={d:.6} weight_rel_rmse={d:.6} weight_max_index={d} dot_abs={d:.6} gguf_dot={d:.6} hf_dot={d:.6}\n",
            .{
                token_id,
                hf_name,
                storage.tensor_type.name(),
                stats.max_abs,
                stats.mean_abs,
                stats.rmse,
                stats.rel_rmse,
                stats.max_index,
                @abs(gguf_dot - hf_dot),
                gguf_dot,
                hf_dot,
            },
        );
    }
}

fn tensorRowToFloat32(tensor: *const @import("backends/tensor.zig").Tensor, row: usize, dim: usize, output: []f32) !void {
    if (output.len != dim) return error.InvalidTensorShape;
    if (tensor.shape.len != 2) return error.InvalidTensorShape;
    if (row >= @as(usize, @intCast(tensor.shape[0])) or dim != @as(usize, @intCast(tensor.shape[1]))) return error.InvalidTensorShape;
    const row_offset = row * dim;
    switch (tensor.dtype) {
        .f32 => {
            const values = tensor.asFloat32();
            @memcpy(output, values[row_offset..][0..dim]);
        },
        .bf16 => {
            for (0..dim) |col| {
                const byte_offset = (row_offset + col) * @sizeOf(u16);
                const bits = std.mem.readInt(u16, tensor.data[byte_offset..][0..@sizeOf(u16)], .little);
                output[col] = bf16BitsToF32(bits);
            }
        },
        .f16 => {
            for (0..dim) |col| {
                const byte_offset = (row_offset + col) * @sizeOf(u16);
                const bits = std.mem.readInt(u16, tensor.data[byte_offset..][0..@sizeOf(u16)], .little);
                const half: f16 = @bitCast(bits);
                output[col] = @floatCast(half);
            }
        },
        else => return error.UnsupportedTensorType,
    }
}

fn runGemma4ParityOnStore(allocator: std.mem.Allocator, tensor_store: tensor_store_mod.TensorStore) !void {
    for (gemma4_parity_cases) |case| {
        try runGemma4LinearParityCase(allocator, tensor_store, case);
    }
    try runGemma4NormRopeParity(allocator, tensor_store);
    try runGqaParity(allocator);
    try runGemma4Layer0Parity(allocator, tensor_store);
    try runGemma4Layer5AttentionParity(allocator, tensor_store);
    try runGemma4FinalProjectionParity(allocator, tensor_store);
}

fn runGemma4LinearParityCase(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
    case: Gemma4ParityCase,
) !void {
    const source_name = try resolveGemma4ParityTensorName(allocator, tensor_store, case.name);
    defer allocator.free(source_name);
    var tensor_ref = try tensor_store.describeTensor(allocator, source_name);
    defer tensor_ref.deinit(allocator);

    var loaded = try tensor_store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    if (loaded.tensor.shape.len != 2) return error.InvalidTensorShape;

    const out_dim: usize = @intCast(loaded.tensor.shape[0]);
    const in_dim: usize = @intCast(loaded.tensor.shape[1]);
    const rows: usize = 1;
    const input = try makeParityInput(allocator, in_dim);
    defer allocator.free(input);

    const expected = try allocator.alloc(f32, out_dim);
    defer allocator.free(expected);
    const weight_host = try tensorToFloat32Owned(allocator, &loaded.tensor);
    defer allocator.free(weight_host);
    if (loaded.tensor.dtype == .bf16)
        cpuLinearNoBiasBf16Input(input, weight_host, expected, in_dim, out_dim)
    else
        cpuLinearNoBias(input, weight_host, expected, in_dim, out_dim);

    var compute = try cuda_compute.CudaCompute.init(allocator);
    defer compute.deinit();
    const key = try allocator.dupe(u8, case.name);
    var inserted = false;
    errdefer if (!inserted) allocator.free(key);
    try compute.insertWeightFromLoaded(key, &loaded);
    inserted = true;
    const cb = compute.computeBackend();

    const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const weight_ct = try cb.getWeight(case.name);
    const output_ct = try cb.linearNoBias(input_ct, weight_ct, rows, in_dim, out_dim);
    defer cb.free(output_ct);
    const actual = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(actual);
    if (actual.len != expected.len) return error.InvalidTensorShape;

    const stats = diffStats(actual, expected);
    const tolerance = toleranceForLoadedParityCase(&loaded);
    const quant_name = if (loaded.quantized_storage) |storage| storage.tensor_type.name() else "F32";
    print(
        "gemma4_parity: {s} source={s} type={s} dtype={s} shape=[{d},{d}] max_abs={d:.6} mean_abs={d:.6} max_index={d}\n",
        .{ case.name, source_name, quant_name, @tagName(loaded.tensor.dtype), out_dim, in_dim, stats.max_abs, stats.mean_abs, stats.max_index },
    );
    if (stats.max_abs > tolerance) return error.CudaParityMismatch;
}

fn runGemma4NormRopeParity(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
) !void {
    const cases = [_]NormRopeParityCase{
        .{
            .weight_name = "blk.0.attn_q_norm.weight",
            .layer = 0,
            .proj = "q",
            .rows = 2,
            .total_dim = 4096,
            .head_dim = 256,
            .rope_dim = 256,
            .theta = 10000.0,
            .scale = @sqrt(@as(f32, 256.0)),
        },
        .{
            .weight_name = "blk.0.attn_k_norm.weight",
            .layer = 0,
            .proj = "k",
            .rows = 2,
            .total_dim = 2048,
            .head_dim = 256,
            .rope_dim = 256,
            .theta = 10000.0,
            .scale = 1.0,
        },
        .{
            .weight_name = "blk.5.attn_q_norm.weight",
            .layer = 5,
            .proj = "q",
            .rows = 2,
            .total_dim = 8192,
            .head_dim = 512,
            .rope_dim = 512,
            .theta = 1000000.0,
            .scale = @sqrt(@as(f32, 512.0)),
        },
        .{
            .weight_name = "blk.5.attn_k_norm.weight",
            .layer = 5,
            .proj = "k",
            .rows = 2,
            .total_dim = 512,
            .head_dim = 512,
            .rope_dim = 512,
            .theta = 1000000.0,
            .scale = 1.0,
        },
    };

    for (cases) |case| {
        try runNormRopeParityCase(allocator, tensor_store, case);
    }
}

fn runNormRopeParityCase(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
    case: NormRopeParityCase,
) !void {
    const source_name = try resolveGemma4ParityTensorName(allocator, tensor_store, case.weight_name);
    defer allocator.free(source_name);
    var tensor_ref = try tensor_store.describeTensor(allocator, source_name);
    defer tensor_ref.deinit(allocator);

    var loaded = try tensor_store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    if (loaded.tensor.elementCount() != case.head_dim) return error.InvalidTensorShape;
    const weight_host = try tensorToFloat32Owned(allocator, &loaded.tensor);
    defer allocator.free(weight_host);

    const count = case.rows * case.total_dim;
    const input = try makeParityInput(allocator, count);
    defer allocator.free(input);

    const expected = try allocator.alloc(f32, count);
    defer allocator.free(expected);
    cpuRmsNormHeadsRope(
        input,
        weight_host,
        expected,
        case.rows,
        case.total_dim,
        case.head_dim,
        case.rope_dim,
        0.000001,
        case.theta,
        1.0,
        0,
        case.rows,
        false,
        case.scale,
    );

    var compute = try cuda_compute.CudaCompute.init(allocator);
    defer compute.deinit();
    const key = try allocator.dupe(u8, case.weight_name);
    var inserted = false;
    errdefer if (!inserted) allocator.free(key);
    try compute.insertWeightFromLoaded(key, &loaded);
    inserted = true;
    const cb = compute.computeBackend();

    const input_shape = [_]i32{ @intCast(case.rows), @intCast(case.total_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const weight_ct = try cb.getWeight(case.weight_name);
    const output_ct = (try cb.rmsNormHeadsRope(
        input_ct,
        weight_ct,
        case.rows,
        case.total_dim,
        case.head_dim,
        case.rope_dim,
        0.000001,
        case.theta,
        1.0,
        0,
        case.rows,
        false,
        case.scale,
    )) orelse return error.CudaKernelUnavailable;
    defer cb.free(output_ct);

    const actual = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(actual);
    if (actual.len != expected.len) return error.InvalidTensorShape;
    const stats = diffStats(actual, expected);
    print(
        "gemma4_parity: rms_norm_heads_rope layer={d} proj={s} source={s} shape=[{d},{d}] max_abs={d:.6} mean_abs={d:.6} max_index={d}\n",
        .{ case.layer, case.proj, source_name, case.rows, case.total_dim, stats.max_abs, stats.mean_abs, stats.max_index },
    );
    if (stats.max_abs > toleranceForLoadedElementwiseCase(&loaded)) return error.CudaParityMismatch;
}

fn runGqaParity(allocator: std.mem.Allocator) !void {
    const batch: usize = 1;
    const seq_len: usize = 3;
    const num_heads: usize = 4;
    const num_kv_heads: usize = 2;
    const head_dim: usize = 8;
    const q_count = batch * seq_len * num_heads * head_dim;
    const kv_count = batch * seq_len * num_kv_heads * head_dim;
    const q = try makeParityInput(allocator, q_count);
    defer allocator.free(q);
    const k = try makeParityInput(allocator, kv_count);
    defer allocator.free(k);
    const v = try allocator.alloc(f32, kv_count);
    defer allocator.free(v);
    for (v, 0..) |*value, idx| {
        const centered: i32 = @as(i32, @intCast((idx * 7 + 11) % 199)) - 99;
        value.* = @as(f32, @floatFromInt(centered)) / 99.0;
    }

    const expected = try allocator.alloc(f32, q_count);
    defer allocator.free(expected);
    cpuGqaCausalAttention(q, k, v, expected, batch, seq_len, num_heads, num_kv_heads, head_dim);

    var compute = try cuda_compute.CudaCompute.init(allocator);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const q_shape = [_]i32{ @intCast(batch * seq_len), @intCast(num_heads * head_dim) };
    const kv_shape = [_]i32{ @intCast(batch * seq_len), @intCast(num_kv_heads * head_dim) };
    const q_ct = try cb.fromFloat32Shape(q, &q_shape);
    defer cb.free(q_ct);
    const k_ct = try cb.fromFloat32Shape(k, &kv_shape);
    defer cb.free(k_ct);
    const v_ct = try cb.fromFloat32Shape(v, &kv_shape);
    defer cb.free(v_ct);
    const output_ct = try cb.gqaCausalAttention(q_ct, k_ct, v_ct, null, batch, seq_len, num_heads, num_kv_heads, head_dim);
    defer cb.free(output_ct);
    const actual = try cb.toFloat32(output_ct, allocator);
    defer allocator.free(actual);
    if (actual.len != expected.len) return error.InvalidTensorShape;
    const stats = diffStats(actual, expected);
    print(
        "gemma4_parity: gqa_causal_attention shape=[{d},{d},{d},{d}] max_abs={d:.6} mean_abs={d:.6} max_index={d}\n",
        .{ batch, seq_len, num_heads, head_dim, stats.max_abs, stats.mean_abs, stats.max_index },
    );
    if (stats.max_abs > 0.001) return error.CudaParityMismatch;
}

const Gemma4Layer0Weights = struct {
    attn_norm: []f32,
    q: []f32,
    k: []f32,
    v: []f32,
    q_norm: []f32,
    k_norm: []f32,
    attn_output: []f32,
    post_attention_norm: []f32,
    ffn_norm: []f32,
    ffn_gate: []f32,
    ffn_up: []f32,
    ffn_down: []f32,
    post_ffw_norm: []f32,
    layer_output_scale: []f32,

    fn deinit(self: *Gemma4Layer0Weights, allocator: std.mem.Allocator) void {
        allocator.free(self.attn_norm);
        allocator.free(self.q);
        allocator.free(self.k);
        allocator.free(self.v);
        allocator.free(self.q_norm);
        allocator.free(self.k_norm);
        allocator.free(self.attn_output);
        allocator.free(self.post_attention_norm);
        allocator.free(self.ffn_norm);
        allocator.free(self.ffn_gate);
        allocator.free(self.ffn_up);
        allocator.free(self.ffn_down);
        allocator.free(self.post_ffw_norm);
        allocator.free(self.layer_output_scale);
        self.* = undefined;
    }
};

fn runGemma4Layer0Parity(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
) !void {
    const rows: usize = 3;
    const hidden: usize = 3840;
    const q_dim: usize = 4096;
    const kv_dim: usize = 2048;
    const num_heads: usize = 16;
    const num_kv_heads: usize = 8;
    const head_dim: usize = 256;
    const inter: usize = 15360;
    const eps: f32 = 0.000001;
    const theta: f32 = 10000.0;
    const bf16_linear_inputs = tensor_store.kind() == .safetensors;
    const elem_tolerance = elementwiseStageTolerance(tensor_store, 0.001);
    const linear_tolerance = linearStageTolerance(tensor_store, 0.001);
    const ffn_linear_tolerance = linearStageTolerance(tensor_store, 0.005);
    const activation_tolerance = activationStageTolerance(tensor_store, 0.005);
    const ffn_down_tolerance = ffnDownStageTolerance(tensor_store, 0.005);
    const ffn_post_tolerance = ffnPostStageTolerance(tensor_store, 0.001);

    var compute = try cuda_compute.CudaCompute.init(allocator);
    defer compute.deinit();
    var weights = try loadGemma4Layer0Weights(allocator, tensor_store, &compute);
    defer weights.deinit(allocator);
    const cb = compute.computeBackend();

    const input = try makeParityInput(allocator, rows * hidden);
    defer allocator.free(input);
    const input_shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const expected_attn_norm = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_attn_norm);
    cpuRmsNormRows(input, weights.attn_norm, expected_attn_norm, rows, hidden, eps);
    const attn_norm_w = try cb.getWeight("blk.0.attn_norm.weight");
    const attn_norm_ct = try cb.rmsNorm(input_ct, attn_norm_w, hidden, eps);
    defer cb.free(attn_norm_ct);
    try compareCudaStage(allocator, &cb, "layer0.attn_norm", attn_norm_ct, expected_attn_norm, elem_tolerance);

    const expected_q = try allocator.alloc(f32, rows * q_dim);
    defer allocator.free(expected_q);
    const expected_k = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_k);
    const expected_v = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_v);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn_norm, weights.q, expected_q, rows, hidden, q_dim);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn_norm, weights.k, expected_k, rows, hidden, kv_dim);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn_norm, weights.v, expected_v, rows, hidden, kv_dim);
    const q_w = try cb.getWeight("blk.0.attn_q.weight");
    const k_w = try cb.getWeight("blk.0.attn_k.weight");
    const v_w = try cb.getWeight("blk.0.attn_v.weight");
    const qkv_ct = try cb.linearNoBiasQkv(attn_norm_ct, q_w, k_w, v_w, rows, hidden, q_dim, kv_dim);
    const q_ct = qkv_ct.first;
    defer cb.free(q_ct);
    const k_ct = qkv_ct.second;
    defer cb.free(k_ct);
    const v_ct = qkv_ct.third;
    defer cb.free(v_ct);
    try compareCudaStage(allocator, &cb, "layer0.q", q_ct, expected_q, linear_tolerance);
    try compareCudaStage(allocator, &cb, "layer0.k", k_ct, expected_k, linear_tolerance);
    try compareCudaStage(allocator, &cb, "layer0.v", v_ct, expected_v, linear_tolerance);

    const expected_q_rope = try allocator.alloc(f32, rows * q_dim);
    defer allocator.free(expected_q_rope);
    const expected_k_rope = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_k_rope);
    cpuRmsNormHeadsRope(expected_q, weights.q_norm, expected_q_rope, rows, q_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, @sqrt(@as(f32, @floatFromInt(head_dim))));
    cpuRmsNormHeadsRope(expected_k, weights.k_norm, expected_k_rope, rows, kv_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, 1.0);
    const q_norm_w = try cb.getWeight("blk.0.attn_q_norm.weight");
    const k_norm_w = try cb.getWeight("blk.0.attn_k_norm.weight");
    const q_rope_ct = (try cb.rmsNormHeadsRope(q_ct, q_norm_w, rows, q_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, @sqrt(@as(f32, @floatFromInt(head_dim))))) orelse return error.CudaKernelUnavailable;
    defer cb.free(q_rope_ct);
    const k_rope_ct = (try cb.rmsNormHeadsRope(k_ct, k_norm_w, rows, kv_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, 1.0)) orelse return error.CudaKernelUnavailable;
    defer cb.free(k_rope_ct);
    try compareCudaStage(allocator, &cb, "layer0.q_rope", q_rope_ct, expected_q_rope, 0.001);
    try compareCudaStage(allocator, &cb, "layer0.k_rope", k_rope_ct, expected_k_rope, 0.001);

    const expected_v_norm = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_v_norm);
    cpuRmsNormBareRows(expected_v, expected_v_norm, rows * num_kv_heads, head_dim, eps);
    const v_flat_ct = (try cb.reshape2d(v_ct, rows * num_kv_heads, head_dim)) orelse return error.ReshapeFailed;
    defer cb.free(v_flat_ct);
    const v_norm_flat_ct = (try cb.rmsNormBare(v_flat_ct, head_dim, eps)) orelse return error.CudaKernelUnavailable;
    defer cb.free(v_norm_flat_ct);
    const v_norm_ct = (try cb.reshape2d(v_norm_flat_ct, rows, kv_dim)) orelse return error.ReshapeFailed;
    defer cb.free(v_norm_ct);
    try compareCudaStage(allocator, &cb, "layer0.v_norm", v_norm_ct, expected_v_norm, 0.001);

    const expected_attn = try allocator.alloc(f32, rows * q_dim);
    defer allocator.free(expected_attn);
    cpuGqaCausalAttention(expected_q_rope, expected_k_rope, expected_v_norm, expected_attn, 1, rows, num_heads, num_kv_heads, head_dim);
    const attn_ct = try cb.gqaCausalAttention(q_rope_ct, k_rope_ct, v_norm_ct, null, 1, rows, num_heads, num_kv_heads, head_dim);
    defer cb.free(attn_ct);
    try compareCudaStage(allocator, &cb, "layer0.attn_out", attn_ct, expected_attn, 0.001);

    const expected_attn_proj = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_attn_proj);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn, weights.attn_output, expected_attn_proj, rows, q_dim, hidden);
    const attn_out_w = try cb.getWeight("blk.0.attn_output.weight");
    const attn_proj_ct = try cb.linearNoBias(attn_ct, attn_out_w, rows, q_dim, hidden);
    defer cb.free(attn_proj_ct);
    try compareCudaStage(allocator, &cb, "layer0.attn_proj", attn_proj_ct, expected_attn_proj, linear_tolerance);

    const expected_attn_post = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_attn_post);
    cpuRmsNormRows(expected_attn_proj, weights.post_attention_norm, expected_attn_post, rows, hidden, eps);
    const post_attn_w = try cb.getWeight("blk.0.post_attention_norm.weight");
    const attn_post_ct = try cb.rmsNorm(attn_proj_ct, post_attn_w, hidden, eps);
    defer cb.free(attn_post_ct);
    try compareCudaStage(allocator, &cb, "layer0.attn_post", attn_post_ct, expected_attn_post, elem_tolerance);

    const expected_sa = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_sa);
    cpuAdd(expected_attn_post, input, expected_sa);
    const sa_ct = try cb.add(attn_post_ct, input_ct);
    defer cb.free(sa_ct);
    try compareCudaStage(allocator, &cb, "layer0.attn_residual", sa_ct, expected_sa, elem_tolerance);

    const expected_ffn_norm = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_ffn_norm);
    cpuRmsNormRows(expected_sa, weights.ffn_norm, expected_ffn_norm, rows, hidden, eps);
    const ffn_norm_w = try cb.getWeight("blk.0.ffn_norm.weight");
    const ffn_norm_ct = try cb.rmsNorm(sa_ct, ffn_norm_w, hidden, eps);
    defer cb.free(ffn_norm_ct);
    try compareCudaStage(allocator, &cb, "layer0.ffn_norm", ffn_norm_ct, expected_ffn_norm, elem_tolerance);

    const expected_gate = try allocator.alloc(f32, rows * inter);
    defer allocator.free(expected_gate);
    const expected_up = try allocator.alloc(f32, rows * inter);
    defer allocator.free(expected_up);
    const expected_gated = try allocator.alloc(f32, rows * inter);
    defer allocator.free(expected_gated);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_ffn_norm, weights.ffn_gate, expected_gate, rows, hidden, inter);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_ffn_norm, weights.ffn_up, expected_up, rows, hidden, inter);
    cpuSiluMultiply(expected_gate, expected_up, expected_gated);
    const gate_w = try cb.getWeight("blk.0.ffn_gate.weight");
    const up_w = try cb.getWeight("blk.0.ffn_up.weight");
    const gate_up_ct = try cb.linearNoBiasPair(ffn_norm_ct, gate_w, up_w, rows, hidden, inter);
    const gate_ct = gate_up_ct.first;
    defer cb.free(gate_ct);
    const up_ct = gate_up_ct.second;
    defer cb.free(up_ct);
    try compareCudaStage(allocator, &cb, "layer0.ffn_gate", gate_ct, expected_gate, ffn_linear_tolerance);
    try compareCudaStage(allocator, &cb, "layer0.ffn_up", up_ct, expected_up, ffn_linear_tolerance);
    const gated_ct = (try cb.activationMultiply(gate_ct, up_ct, .silu)) orelse return error.CudaKernelUnavailable;
    defer cb.free(gated_ct);
    try compareCudaStage(allocator, &cb, "layer0.ffn_gated", gated_ct, expected_gated, activation_tolerance);

    const expected_ffn_raw = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_ffn_raw);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_gated, weights.ffn_down, expected_ffn_raw, rows, inter, hidden);
    const down_w = try cb.getWeight("blk.0.ffn_down.weight");
    const ffn_raw_ct = try cb.linearNoBias(gated_ct, down_w, rows, inter, hidden);
    defer cb.free(ffn_raw_ct);
    try compareCudaStage(allocator, &cb, "layer0.ffn_raw", ffn_raw_ct, expected_ffn_raw, ffn_down_tolerance);

    const expected_ffn_post = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_ffn_post);
    cpuRmsNormRows(expected_ffn_raw, weights.post_ffw_norm, expected_ffn_post, rows, hidden, eps);
    const post_ffw_w = try cb.getWeight("blk.0.post_ffw_norm.weight");
    const ffn_post_ct = try cb.rmsNorm(ffn_raw_ct, post_ffw_w, hidden, eps);
    defer cb.free(ffn_post_ct);
    try compareCudaStage(allocator, &cb, "layer0.ffn_post", ffn_post_ct, expected_ffn_post, ffn_post_tolerance);

    const expected_out = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_out);
    cpuAddScaled(expected_ffn_post, expected_sa, weights.layer_output_scale[0], expected_out);
    const scale_w = try cb.getWeight("blk.0.layer_output_scale.weight");
    const out_ct = if (try cb.addMultiplyScalarTensor(ffn_post_ct, sa_ct, scale_w)) |fused|
        fused
    else blk: {
        const scaled = try cb.multiply(ffn_post_ct, scale_w);
        defer cb.free(scaled);
        break :blk try cb.add(scaled, sa_ct);
    };
    defer cb.free(out_ct);
    try compareCudaStage(allocator, &cb, "layer0.out", out_ct, expected_out, ffn_post_tolerance);

    print("gemma4_parity: layer0_decoder_block ok\n", .{});
}

fn runGemma4Layer5AttentionParity(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
) !void {
    const rows: usize = 3;
    const hidden: usize = 3840;
    const q_dim: usize = 8192;
    const kv_dim: usize = 512;
    const num_heads: usize = 16;
    const num_kv_heads: usize = 1;
    const head_dim: usize = 512;
    const eps: f32 = 0.000001;
    const theta: f32 = 1000000.0;
    const bf16_linear_inputs = tensor_store.kind() == .safetensors;
    const elem_tolerance = elementwiseStageTolerance(tensor_store, 0.001);
    const linear_tolerance = linearStageTolerance(tensor_store, 0.005);

    var compute = try cuda_compute.CudaCompute.init(allocator);
    defer compute.deinit();
    const attn_norm = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.attn_norm.weight");
    defer allocator.free(attn_norm);
    const q_w_host = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.attn_q.weight");
    defer allocator.free(q_w_host);
    const k_w_host = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.attn_k.weight");
    defer allocator.free(k_w_host);
    const q_norm = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.attn_q_norm.weight");
    defer allocator.free(q_norm);
    const k_norm = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.attn_k_norm.weight");
    defer allocator.free(k_norm);
    const attn_output = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.attn_output.weight");
    defer allocator.free(attn_output);
    const post_attention_norm = try loadLayerWeight(allocator, tensor_store, &compute, "blk.5.post_attention_norm.weight");
    defer allocator.free(post_attention_norm);
    const cb = compute.computeBackend();

    const input = try makeParityInput(allocator, rows * hidden);
    defer allocator.free(input);
    const input_shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const expected_attn_norm = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_attn_norm);
    cpuRmsNormRows(input, attn_norm, expected_attn_norm, rows, hidden, eps);
    const attn_norm_w = try cb.getWeight("blk.5.attn_norm.weight");
    const attn_norm_ct = try cb.rmsNorm(input_ct, attn_norm_w, hidden, eps);
    defer cb.free(attn_norm_ct);
    try compareCudaStage(allocator, &cb, "layer5.attn_norm", attn_norm_ct, expected_attn_norm, elem_tolerance);

    const expected_q = try allocator.alloc(f32, rows * q_dim);
    defer allocator.free(expected_q);
    const expected_k = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_k);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn_norm, q_w_host, expected_q, rows, hidden, q_dim);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn_norm, k_w_host, expected_k, rows, hidden, kv_dim);
    const q_w = try cb.getWeight("blk.5.attn_q.weight");
    const k_w = try cb.getWeight("blk.5.attn_k.weight");
    const q_ct = try cb.linearNoBias(attn_norm_ct, q_w, rows, hidden, q_dim);
    defer cb.free(q_ct);
    const k_ct = try cb.linearNoBias(attn_norm_ct, k_w, rows, hidden, kv_dim);
    defer cb.free(k_ct);
    try compareCudaStage(allocator, &cb, "layer5.q", q_ct, expected_q, linear_tolerance);
    try compareCudaStage(allocator, &cb, "layer5.k", k_ct, expected_k, linear_tolerance);

    const expected_q_rope = try allocator.alloc(f32, rows * q_dim);
    defer allocator.free(expected_q_rope);
    const expected_k_rope = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_k_rope);
    cpuRmsNormHeadsRope(expected_q, q_norm, expected_q_rope, rows, q_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, @sqrt(@as(f32, @floatFromInt(head_dim))));
    cpuRmsNormHeadsRope(expected_k, k_norm, expected_k_rope, rows, kv_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, 1.0);
    const q_norm_w = try cb.getWeight("blk.5.attn_q_norm.weight");
    const k_norm_w = try cb.getWeight("blk.5.attn_k_norm.weight");
    const q_rope_ct = (try cb.rmsNormHeadsRope(q_ct, q_norm_w, rows, q_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, @sqrt(@as(f32, @floatFromInt(head_dim))))) orelse return error.CudaKernelUnavailable;
    defer cb.free(q_rope_ct);
    const k_rope_ct = (try cb.rmsNormHeadsRope(k_ct, k_norm_w, rows, kv_dim, head_dim, head_dim, eps, theta, 1.0, 0, rows, false, 1.0)) orelse return error.CudaKernelUnavailable;
    defer cb.free(k_rope_ct);
    try compareCudaStage(allocator, &cb, "layer5.q_rope", q_rope_ct, expected_q_rope, 0.005);
    try compareCudaStage(allocator, &cb, "layer5.k_rope", k_rope_ct, expected_k_rope, 0.001);

    const expected_v_norm = try allocator.alloc(f32, rows * kv_dim);
    defer allocator.free(expected_v_norm);
    cpuRmsNormBareRows(expected_k, expected_v_norm, rows * num_kv_heads, head_dim, eps);
    const v_flat_ct = (try cb.reshape2d(k_ct, rows * num_kv_heads, head_dim)) orelse return error.ReshapeFailed;
    defer cb.free(v_flat_ct);
    const v_norm_flat_ct = (try cb.rmsNormBare(v_flat_ct, head_dim, eps)) orelse return error.CudaKernelUnavailable;
    defer cb.free(v_norm_flat_ct);
    const v_norm_ct = (try cb.reshape2d(v_norm_flat_ct, rows, kv_dim)) orelse return error.ReshapeFailed;
    defer cb.free(v_norm_ct);
    try compareCudaStage(allocator, &cb, "layer5.v_norm_from_k", v_norm_ct, expected_v_norm, 0.001);

    const expected_attn = try allocator.alloc(f32, rows * q_dim);
    defer allocator.free(expected_attn);
    cpuGqaCausalAttention(expected_q_rope, expected_k_rope, expected_v_norm, expected_attn, 1, rows, num_heads, num_kv_heads, head_dim);
    const attn_ct = try cb.gqaCausalAttention(q_rope_ct, k_rope_ct, v_norm_ct, null, 1, rows, num_heads, num_kv_heads, head_dim);
    defer cb.free(attn_ct);
    try compareCudaStage(allocator, &cb, "layer5.attn_out", attn_ct, expected_attn, 0.001);

    const expected_attn_proj = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_attn_proj);
    cpuLinearNoBiasRowsForCudaDense(bf16_linear_inputs, expected_attn, attn_output, expected_attn_proj, rows, q_dim, hidden);
    const attn_out_w = try cb.getWeight("blk.5.attn_output.weight");
    const attn_proj_ct = try cb.linearNoBias(attn_ct, attn_out_w, rows, q_dim, hidden);
    defer cb.free(attn_proj_ct);
    try compareCudaStage(allocator, &cb, "layer5.attn_proj", attn_proj_ct, expected_attn_proj, linear_tolerance);

    const expected_attn_post = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(expected_attn_post);
    cpuRmsNormRows(expected_attn_proj, post_attention_norm, expected_attn_post, rows, hidden, eps);
    const post_attn_w = try cb.getWeight("blk.5.post_attention_norm.weight");
    const attn_post_ct = try cb.rmsNorm(attn_proj_ct, post_attn_w, hidden, eps);
    defer cb.free(attn_post_ct);
    try compareCudaStage(allocator, &cb, "layer5.attn_post", attn_post_ct, expected_attn_post, elem_tolerance);

    print("gemma4_parity: layer5_attention_omitted_v ok\n", .{});
}

fn runGemma4FinalProjectionParity(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
) !void {
    const hidden: usize = 3840;
    const vocab: usize = 262144;
    const eps: f32 = 0.000001;
    const sampled_ids = [_]usize{ 0, 1, 2, 50, 100, 101, 106, 236761, 258882, 258883 };

    var compute = try cuda_compute.CudaCompute.init(allocator);
    defer compute.deinit();
    const norm = try loadLayerWeight(allocator, tensor_store, &compute, "output_norm.weight");
    defer allocator.free(norm);

    const embed_source_name = try resolveGemma4ParityTensorName(allocator, tensor_store, "token_embd.weight");
    defer allocator.free(embed_source_name);
    var embed_ref = try tensor_store.describeTensor(allocator, embed_source_name);
    defer embed_ref.deinit(allocator);
    var embed_loaded = try tensor_store.loadTensorRef(&embed_ref);
    defer embed_loaded.deinit();
    if (embed_loaded.tensor.shape.len != 2) return error.InvalidTensorShape;
    if (@as(usize, @intCast(embed_loaded.tensor.shape[0])) != vocab) return error.InvalidTensorShape;
    if (@as(usize, @intCast(embed_loaded.tensor.shape[1])) != hidden) return error.InvalidTensorShape;
    const embed_host = if (embed_loaded.tensor.dtype == .bf16)
        @as(?[]f32, null)
    else
        try tensorToFloat32Owned(allocator, &embed_loaded.tensor);
    defer if (embed_host) |host| allocator.free(host);
    const embed_key = try allocator.dupe(u8, "token_embd.weight");
    var inserted = false;
    errdefer if (!inserted) allocator.free(embed_key);
    try compute.insertWeightFromLoaded(embed_key, &embed_loaded);
    inserted = true;

    const cb = compute.computeBackend();
    const input = try makeParityInput(allocator, hidden);
    defer allocator.free(input);
    const expected_norm = try allocator.alloc(f32, hidden);
    defer allocator.free(expected_norm);
    cpuRmsNormRows(input, norm, expected_norm, 1, hidden, eps);

    const input_shape = [_]i32{ 1, @intCast(hidden) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);
    const norm_w = try cb.getWeight("output_norm.weight");
    const norm_ct = try cb.rmsNorm(input_ct, norm_w, hidden, eps);
    defer cb.free(norm_ct);
    try compareCudaStage(allocator, &cb, "final.norm", norm_ct, expected_norm, 0.005);

    const embed_w = try cb.getWeight("token_embd.weight");
    const logits_ct = try cb.linearNoBias(norm_ct, embed_w, 1, hidden, vocab);
    defer cb.free(logits_ct);
    const actual_logits = try cb.toFloat32(logits_ct, allocator);
    defer allocator.free(actual_logits);
    if (actual_logits.len != vocab) return error.InvalidTensorShape;

    var max_abs: f32 = 0;
    var mean_abs: f64 = 0;
    var max_token: usize = 0;
    var max_expected: f32 = 0;
    var max_actual: f32 = 0;
    for (sampled_ids) |token_id| {
        const expected = if (embed_loaded.tensor.dtype == .bf16)
            cpuEmbeddingRowDotBf16Input(expected_norm, embed_loaded.tensor.data, token_id, hidden)
        else
            cpuEmbeddingRowDot(expected_norm, embed_host orelse return error.UnsupportedTensorType, token_id, hidden);
        const actual = actual_logits[token_id];
        const diff = @abs(actual - expected);
        mean_abs += diff;
        if (diff > max_abs) {
            max_abs = diff;
            max_token = token_id;
            max_expected = expected;
            max_actual = actual;
        }
    }
    mean_abs /= @as(f64, @floatFromInt(sampled_ids.len));
    print(
        "gemma4_parity: final.tied_lm_head sampled={d} max_abs={d:.6} mean_abs={d:.6} token={d}\n",
        .{ sampled_ids.len, max_abs, mean_abs, max_token },
    );
    const tolerance: f32 = if (embed_loaded.quantized) 0.05 else 0.02;
    if (max_abs > tolerance) {
        print(
            "gemma4_parity: first_divergence stage=final.tied_lm_head got={d:.6} expected={d:.6}\n",
            .{ max_actual, max_expected },
        );
        return error.CudaParityMismatch;
    }
}

fn loadGemma4Layer0Weights(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
    compute: *cuda_compute.CudaCompute,
) !Gemma4Layer0Weights {
    return .{
        .attn_norm = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_norm.weight"),
        .q = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_q.weight"),
        .k = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_k.weight"),
        .v = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_v.weight"),
        .q_norm = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_q_norm.weight"),
        .k_norm = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_k_norm.weight"),
        .attn_output = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.attn_output.weight"),
        .post_attention_norm = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.post_attention_norm.weight"),
        .ffn_norm = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.ffn_norm.weight"),
        .ffn_gate = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.ffn_gate.weight"),
        .ffn_up = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.ffn_up.weight"),
        .ffn_down = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.ffn_down.weight"),
        .post_ffw_norm = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.post_ffw_norm.weight"),
        .layer_output_scale = try loadLayerWeight(allocator, tensor_store, compute, "blk.0.layer_output_scale.weight"),
    };
}

fn loadLayerWeight(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
    compute: *cuda_compute.CudaCompute,
    name: []const u8,
) ![]f32 {
    const source_name = try resolveGemma4ParityTensorName(allocator, tensor_store, name);
    defer allocator.free(source_name);
    var tensor_ref = try tensor_store.describeTensor(allocator, source_name);
    defer tensor_ref.deinit(allocator);
    var loaded = try tensor_store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    const host = try tensorToFloat32Owned(allocator, &loaded.tensor);
    errdefer allocator.free(host);
    const key = try allocator.dupe(u8, name);
    var inserted = false;
    errdefer if (!inserted) allocator.free(key);
    try compute.insertWeightFromLoaded(key, &loaded);
    inserted = true;
    return host;
}

fn tensorToFloat32Owned(allocator: std.mem.Allocator, tensor: *const @import("backends/tensor.zig").Tensor) ![]f32 {
    if (tensor.dtype == .f32) return try allocator.dupe(f32, tensor.asFloat32());
    if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
        var converted = try weight_source_mod.convertToF32(allocator, tensor);
        defer converted.deinit();
        return try allocator.dupe(f32, converted.asFloat32());
    }
    return error.UnsupportedTensorType;
}

fn resolveGemma4ParityTensorName(
    allocator: std.mem.Allocator,
    tensor_store: tensor_store_mod.TensorStore,
    gguf_name: []const u8,
) ![]u8 {
    if (tensor_store.kind() != .safetensors) return try allocator.dupe(u8, gguf_name);
    if (std.mem.eql(u8, gguf_name, "output_norm.weight")) return try allocator.dupe(u8, "model.language_model.norm.weight");
    if (std.mem.eql(u8, gguf_name, "token_embd.weight")) return try allocator.dupe(u8, "model.language_model.embed_tokens.weight");
    if (!std.mem.startsWith(u8, gguf_name, "blk.")) return try allocator.dupe(u8, gguf_name);

    const rest = gguf_name["blk.".len..];
    const dot = std.mem.indexOfScalar(u8, rest, '.') orelse return error.InvalidTensorName;
    const layer = rest[0..dot];
    const suffix = rest[dot + 1 ..];
    const hf_suffix = gemma4HfLayerSuffixForGguf(suffix) orelse return error.TensorNotFound;
    return try std.fmt.allocPrint(allocator, "model.language_model.layers.{s}.{s}", .{ layer, hf_suffix });
}

fn gemma4HfLayerSuffixForGguf(suffix: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, suffix, "attn_norm.weight")) return "input_layernorm.weight";
    if (std.mem.eql(u8, suffix, "attn_q.weight")) return "self_attn.q_proj.weight";
    if (std.mem.eql(u8, suffix, "attn_k.weight")) return "self_attn.k_proj.weight";
    if (std.mem.eql(u8, suffix, "attn_v.weight")) return "self_attn.v_proj.weight";
    if (std.mem.eql(u8, suffix, "attn_output.weight")) return "self_attn.o_proj.weight";
    if (std.mem.eql(u8, suffix, "attn_q_norm.weight")) return "self_attn.q_norm.weight";
    if (std.mem.eql(u8, suffix, "attn_k_norm.weight")) return "self_attn.k_norm.weight";
    if (std.mem.eql(u8, suffix, "post_attention_norm.weight")) return "post_attention_layernorm.weight";
    if (std.mem.eql(u8, suffix, "ffn_norm.weight")) return "pre_feedforward_layernorm.weight";
    if (std.mem.eql(u8, suffix, "ffn_gate.weight")) return "mlp.gate_proj.weight";
    if (std.mem.eql(u8, suffix, "ffn_up.weight")) return "mlp.up_proj.weight";
    if (std.mem.eql(u8, suffix, "ffn_down.weight")) return "mlp.down_proj.weight";
    if (std.mem.eql(u8, suffix, "post_ffw_norm.weight")) return "post_feedforward_layernorm.weight";
    if (std.mem.eql(u8, suffix, "layer_output_scale.weight")) return "layer_scalar";
    return null;
}

fn compareCudaStage(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    stage: []const u8,
    tensor: ops.CT,
    expected: []const f32,
    tolerance: f32,
) !void {
    const actual = try cb.toFloat32(tensor, allocator);
    defer allocator.free(actual);
    if (actual.len != expected.len) return error.InvalidTensorShape;
    const stats = diffStats(actual, expected);
    print(
        "gemma4_parity: {s} max_abs={d:.6} mean_abs={d:.6} max_index={d}\n",
        .{ stage, stats.max_abs, stats.mean_abs, stats.max_index },
    );
    if (stats.max_abs > tolerance) {
        print(
            "gemma4_parity: first_divergence stage={s} got={d:.6} expected={d:.6}\n",
            .{ stage, actual[stats.max_index], expected[stats.max_index] },
        );
        return error.CudaParityMismatch;
    }
}

fn makeParityInput(allocator: std.mem.Allocator, in_dim: usize) ![]f32 {
    const input = try allocator.alloc(f32, in_dim);
    for (input, 0..) |*value, idx| {
        const centered: i32 = @as(i32, @intCast(idx % 251)) - 125;
        const sign: f32 = if ((idx / 251) % 2 == 0) 1.0 else -1.0;
        value.* = sign * @as(f32, @floatFromInt(centered)) / 125.0;
    }
    return input;
}

fn cpuLinearNoBias(input: []const f32, weight: []const f32, output: []f32, in_dim: usize, out_dim: usize) void {
    for (0..out_dim) |row| {
        var sum: f32 = 0;
        const w_row = weight[row * in_dim ..][0..in_dim];
        for (0..in_dim) |col| {
            sum += input[col] * w_row[col];
        }
        output[row] = sum;
    }
}

fn cpuLinearNoBiasBf16Input(input: []const f32, weight: []const f32, output: []f32, in_dim: usize, out_dim: usize) void {
    for (0..out_dim) |row| {
        var sum: f32 = 0;
        const w_row = weight[row * in_dim ..][0..in_dim];
        for (0..in_dim) |col| {
            sum += roundF32ToBf16(input[col]) * w_row[col];
        }
        output[row] = sum;
    }
}

fn cpuLinearNoBiasRows(input: []const f32, weight: []const f32, output: []f32, rows: usize, in_dim: usize, out_dim: usize) void {
    for (0..rows) |row| {
        const in_row = input[row * in_dim ..][0..in_dim];
        const out_row = output[row * out_dim ..][0..out_dim];
        cpuLinearNoBias(in_row, weight, out_row, in_dim, out_dim);
    }
}

fn cpuLinearNoBiasRowsForCudaDense(
    bf16_linear_inputs: bool,
    input: []const f32,
    weight: []const f32,
    output: []f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) void {
    for (0..rows) |row| {
        const in_row = input[row * in_dim ..][0..in_dim];
        const out_row = output[row * out_dim ..][0..out_dim];
        if (bf16_linear_inputs) {
            cpuLinearNoBiasBf16Input(in_row, weight, out_row, in_dim, out_dim);
        } else {
            cpuLinearNoBias(in_row, weight, out_row, in_dim, out_dim);
        }
    }
}

fn roundF32ToBf16(value: f32) f32 {
    const bits: u32 = @bitCast(value);
    const rounded = bits + 0x8000;
    return @bitCast(rounded & 0xffff0000);
}

fn cpuEmbeddingRowDotBf16Input(input: []const f32, bf16_weight_bytes: []const u8, row: usize, dim: usize) f32 {
    var sum: f32 = 0;
    const row_offset = row * dim;
    for (0..dim) |col| {
        const bits = std.mem.readInt(u16, bf16_weight_bytes[(row_offset + col) * @sizeOf(u16) ..][0..@sizeOf(u16)], .little);
        const weight = bf16BitsToF32(bits);
        sum += roundF32ToBf16(input[col]) * weight;
    }
    return sum;
}

fn cpuEmbeddingRowDot(input: []const f32, weight: []const f32, row: usize, dim: usize) f32 {
    var sum: f32 = 0;
    const row_offset = row * dim;
    for (0..dim) |col| {
        sum += input[col] * weight[row_offset + col];
    }
    return sum;
}

fn bf16BitsToF32(bits: u16) f32 {
    return @bitCast(@as(u32, bits) << 16);
}

fn cpuRmsNormRows(input: []const f32, weight: []const f32, output: []f32, rows: usize, dim: usize, eps: f32) void {
    for (0..rows) |row| {
        const src = input[row * dim ..][0..dim];
        const dst = output[row * dim ..][0..dim];
        var sumsq: f32 = 0;
        for (src) |value| sumsq += value * value;
        const scale = 1.0 / @sqrt(sumsq / @as(f32, @floatFromInt(dim)) + eps);
        for (src, dst, 0..) |value, *out, idx| out.* = value * scale * weight[idx];
    }
}

fn cpuRmsNormBareRows(input: []const f32, output: []f32, rows: usize, dim: usize, eps: f32) void {
    for (0..rows) |row| {
        const src = input[row * dim ..][0..dim];
        const dst = output[row * dim ..][0..dim];
        var sumsq: f32 = 0;
        for (src) |value| sumsq += value * value;
        const scale = 1.0 / @sqrt(sumsq / @as(f32, @floatFromInt(dim)) + eps);
        for (src, dst) |value, *out| out.* = value * scale;
    }
}

fn cpuAdd(a: []const f32, b: []const f32, output: []f32) void {
    for (a, b, output) |av, bv, *out| out.* = av + bv;
}

fn cpuAddScaled(a: []const f32, b: []const f32, scale: f32, output: []f32) void {
    for (a, b, output) |av, bv, *out| out.* = av * scale + bv;
}

fn cpuSiluMultiply(gate: []const f32, up: []const f32, output: []f32) void {
    for (gate, up, output) |g, u, *out| out.* = (g / (1.0 + @exp(-g))) * u;
}

fn cpuRmsNormHeadsRope(
    input: []const f32,
    weight: []const f32,
    output: []f32,
    rows: usize,
    total_dim: usize,
    head_dim: usize,
    rope_dim: usize,
    eps: f32,
    theta: f32,
    freq_scale: f32,
    position_offset: usize,
    seq_len: usize,
    consecutive_pairs: bool,
    scale: f32,
) void {
    const total_chunks = rows * (total_dim / head_dim);
    const chunks_per_position = total_chunks / seq_len;
    for (0..total_chunks) |chunk| {
        const base = chunk * head_dim;
        var sumsq: f32 = 0;
        for (0..head_dim) |i| {
            const x = input[base + i];
            sumsq += x * x;
        }
        const norm_scale = 1.0 / @sqrt(sumsq / @as(f32, @floatFromInt(head_dim)) + eps);
        for (0..head_dim) |d| {
            var value = input[base + d] * norm_scale * weight[d];
            if (d < rope_dim and rope_dim >= 2) {
                var idx0: usize = 0;
                var idx1: usize = 0;
                var pair_index: usize = 0;
                var second = false;
                if (consecutive_pairs) {
                    pair_index = d / 2;
                    idx0 = pair_index * 2;
                    idx1 = idx0 + 1;
                    second = (d & 1) != 0;
                } else {
                    const half = rope_dim / 2;
                    if (d < half) {
                        pair_index = d;
                        idx0 = d;
                        idx1 = d + half;
                    } else {
                        pair_index = d - half;
                        idx0 = d - half;
                        idx1 = d;
                        second = true;
                    }
                }
                if (idx1 < head_dim) {
                    const token_pos = (chunk / chunks_per_position) % seq_len;
                    const position = position_offset + token_pos;
                    const exponent = @as(f32, @floatFromInt(2 * pair_index)) / @as(f32, @floatFromInt(rope_dim));
                    const angle = @as(f32, @floatFromInt(position)) * freq_scale * (1.0 / std.math.pow(f32, theta, exponent));
                    const s = @sin(angle);
                    const c = @cos(angle);
                    const x0 = input[base + idx0] * norm_scale * weight[idx0];
                    const x1 = input[base + idx1] * norm_scale * weight[idx1];
                    value = if (second) x0 * s + x1 * c else x0 * c - x1 * s;
                }
            }
            output[base + d] = value * scale;
        }
    }
}

fn cpuGqaCausalAttention(
    q: []const f32,
    k: []const f32,
    v: []const f32,
    output: []f32,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) void {
    const heads_per_kv = num_heads / num_kv_heads;
    const q_row_dim = num_heads * head_dim;
    const kv_row_dim = num_kv_heads * head_dim;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    for (0..batch) |b| {
        for (0..seq_len) |qi| {
            for (0..num_heads) |h| {
                const kv_head = h / heads_per_kv;
                var max_score: f32 = -std.math.inf(f32);
                var scores: [16]f32 = undefined;
                for (0..seq_len) |ki| {
                    if (ki > qi) {
                        scores[ki] = -std.math.inf(f32);
                        continue;
                    }
                    var dot: f32 = 0;
                    const q_base = (b * seq_len + qi) * q_row_dim + h * head_dim;
                    const k_base = (b * seq_len + ki) * kv_row_dim + kv_head * head_dim;
                    for (0..head_dim) |d| dot += q[q_base + d] * k[k_base + d];
                    const score = dot * scale;
                    scores[ki] = score;
                    max_score = @max(max_score, score);
                }
                var denom: f32 = 0;
                for (0..seq_len) |ki| {
                    if (ki > qi) continue;
                    const e = @exp(scores[ki] - max_score);
                    scores[ki] = e;
                    denom += e;
                }
                const out_base = (b * seq_len + qi) * q_row_dim + h * head_dim;
                for (0..head_dim) |d| {
                    var sum: f32 = 0;
                    for (0..seq_len) |ki| {
                        if (ki > qi) continue;
                        const v_base = (b * seq_len + ki) * kv_row_dim + kv_head * head_dim;
                        sum += (scores[ki] / denom) * v[v_base + d];
                    }
                    output[out_base + d] = sum;
                }
            }
        }
    }
}

fn diffStats(actual: []const f32, expected: []const f32) DiffStats {
    var stats: DiffStats = .{};
    for (actual, expected, 0..) |got, want, idx| {
        const diff_f32 = got - want;
        const diff = @abs(diff_f32);
        const diff_f64: f64 = @floatCast(diff_f32);
        const got_f64: f64 = @floatCast(got);
        const want_f64: f64 = @floatCast(want);
        stats.sum_abs += diff;
        stats.sum_sq += diff_f64 * diff_f64;
        stats.actual_sq_sum += got_f64 * got_f64;
        stats.expected_sq_sum += want_f64 * want_f64;
        stats.expected_abs_sum += @abs(want_f64);
        if (diff > stats.max_abs) {
            stats.max_abs = diff;
            stats.max_index = idx;
        }
    }
    const count: f64 = @floatFromInt(actual.len);
    stats.mean_abs = stats.sum_abs / count;
    stats.rmse = @sqrt(stats.sum_sq / count);
    stats.actual_rms = @sqrt(stats.actual_sq_sum / count);
    stats.expected_rms = @sqrt(stats.expected_sq_sum / count);
    const expected_l2 = @sqrt(stats.expected_sq_sum);
    const expected_mean_abs = stats.expected_abs_sum / count;
    stats.rel_rmse = if (expected_l2 > 1e-12) @sqrt(stats.sum_sq) / expected_l2 else 0;
    stats.rel_mean_abs = if (expected_mean_abs > 1e-12) stats.mean_abs / expected_mean_abs else 0;
    return stats;
}

fn toleranceForParityCase(quantized: bool) f32 {
    return if (quantized) 0.05 else 0.001;
}

fn toleranceForLoadedParityCase(loaded: *const weight_source_mod.LoadedWeight) f32 {
    if (loaded.quantized) return 0.05;
    return switch (loaded.tensor.dtype) {
        .bf16, .f16 => 0.25,
        else => 0.001,
    };
}

fn toleranceForLoadedElementwiseCase(loaded: *const weight_source_mod.LoadedWeight) f32 {
    return switch (loaded.tensor.dtype) {
        .bf16, .f16 => 0.005,
        else => 0.001,
    };
}

fn elementwiseStageTolerance(tensor_store: tensor_store_mod.TensorStore, base: f32) f32 {
    return if (tensor_store.kind() == .safetensors) @max(base, 0.005) else base;
}

fn linearStageTolerance(tensor_store: tensor_store_mod.TensorStore, base: f32) f32 {
    return if (tensor_store.kind() == .safetensors) @max(base, 0.02) else base;
}

fn activationStageTolerance(tensor_store: tensor_store_mod.TensorStore, base: f32) f32 {
    return if (tensor_store.kind() == .safetensors) @max(base, 0.25) else base;
}

fn ffnDownStageTolerance(tensor_store: tensor_store_mod.TensorStore, base: f32) f32 {
    return if (tensor_store.kind() == .safetensors) @max(base, 0.5) else base;
}

fn ffnPostStageTolerance(tensor_store: tensor_store_mod.TensorStore, base: f32) f32 {
    return if (tensor_store.kind() == .safetensors) @max(base, 0.25) else base;
}
