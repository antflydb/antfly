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
const validation = @import("inference_internal").finetune.gliner2_run_validation;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var out_dir: ?[]const u8 = null;
    var require_loss_decrease = false;
    var min_supervised_tokens_per_second: ?f64 = null;
    var max_avg_step_wall_ms: ?f64 = null;
    var max_total_execute_ms: ?f64 = null;
    var max_peak_resident_bytes: ?usize = null;
    var max_metal_eager_arena_peak_bytes: ?u64 = null;
    var max_metal_eager_arena_spill_bytes: ?u64 = null;
    var max_metal_chunk_local_output_peak_bytes: ?u64 = null;
    var max_metal_chunk_local_output_spill_bytes: ?u64 = null;
    var max_metal_chunk_local_output_unconsumed_hints: ?u64 = null;
    var min_metal_chunk_local_output_consumed_hints: ?u64 = null;
    var min_examples: ?usize = null;
    var min_steps: ?usize = null;
    var min_entity_labels: ?usize = null;
    var min_supervised_tokens: ?usize = null;
    var min_entity_tokens: ?usize = null;
    var max_graph_command_dispatch_count: ?u64 = null;
    var max_graph_host_output_count: ?u64 = null;
    var max_metal_frame_gpu_ms: ?f64 = null;
    var max_metal_last_frame_compute_encoder_count: ?u64 = null;
    var min_metal_frame_chunk_boundary_count: ?u64 = null;
    var min_metal_frame_chunk_promoted_value_count: ?u64 = null;
    var min_metal_frame_chunk_swept_value_count: ?u64 = null;
    var min_graph_runtime_region_dispatch_count: ?u64 = null;
    var max_graph_runtime_region_fallback_count: ?u64 = null;
    var min_graph_runtime_region_elided_node_count: ?u64 = null;
    var min_metal_deberta_ffn_forward_region_count: ?u64 = null;
    var min_metal_deberta_encoder_lora_layer_region_count: ?u64 = null;
    var min_metal_deberta_encoder_lora_residual_layernorm_region_count: ?u64 = null;
    var max_metal_deberta_encoder_lora_layer_scaffold_count: ?u64 = null;
    var max_metal_deberta_encoder_lora_layer_fallback_count: ?u64 = null;
    var min_metal_deberta_attention_flash_call_count: ?u64 = null;
    var max_metal_deberta_attention_gemm_fallback_count: ?u64 = null;
    var min_metal_deberta_encoder_layer_success_count: ?u64 = null;
    var min_metal_deberta_ffn_fused_call_count: ?u64 = null;
    var max_metal_deberta_ffn_fused_fallback_count: ?u64 = null;
    var max_runtime_frame_ineligible_missing_model_metadata: ?u64 = null;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, arg, "--require-loss-decrease")) {
            require_loss_decrease = true;
        } else if (std.mem.eql(u8, arg, "--min-supervised-tokens-per-second")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_supervised_tokens_per_second = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-avg-step-wall-ms")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_avg_step_wall_ms = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-total-execute-ms")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_total_execute_ms = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-peak-resident-bytes")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_peak_resident_bytes = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-eager-arena-peak-bytes")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_eager_arena_peak_bytes = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-eager-arena-spill-bytes")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_eager_arena_spill_bytes = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-chunk-local-output-peak-bytes")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_chunk_local_output_peak_bytes = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-chunk-local-output-spill-bytes")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_chunk_local_output_spill_bytes = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-chunk-local-output-unconsumed-hints")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_chunk_local_output_unconsumed_hints = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-chunk-local-output-consumed-hints")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_chunk_local_output_consumed_hints = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-examples")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_examples = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-steps")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_steps = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-entity-labels")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_entity_labels = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-supervised-tokens")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_supervised_tokens = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-entity-tokens")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_entity_tokens = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-graph-command-dispatch-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_graph_command_dispatch_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-graph-host-output-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_graph_host_output_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-frame-gpu-ms")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_frame_gpu_ms = try std.fmt.parseFloat(f64, value);
        } else if (std.mem.eql(u8, arg, "--max-metal-last-frame-compute-encoder-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_last_frame_compute_encoder_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-frame-chunk-boundary-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_frame_chunk_boundary_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-frame-chunk-promoted-value-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_frame_chunk_promoted_value_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-frame-chunk-swept-value-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_frame_chunk_swept_value_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-graph-runtime-region-dispatch-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_graph_runtime_region_dispatch_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-graph-runtime-region-fallback-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_graph_runtime_region_fallback_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-graph-runtime-region-elided-node-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_graph_runtime_region_elided_node_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-deberta-ffn-forward-region-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_deberta_ffn_forward_region_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-deberta-encoder-lora-layer-region-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_deberta_encoder_lora_layer_region_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-deberta-encoder-lora-residual-layernorm-region-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_deberta_encoder_lora_residual_layernorm_region_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-deberta-encoder-lora-layer-scaffold-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_deberta_encoder_lora_layer_scaffold_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-deberta-encoder-lora-layer-fallback-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_deberta_encoder_lora_layer_fallback_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-deberta-attention-flash-call-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_deberta_attention_flash_call_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-deberta-attention-gemm-fallback-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_deberta_attention_gemm_fallback_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-deberta-encoder-layer-success-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_deberta_encoder_layer_success_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--min-metal-deberta-ffn-fused-call-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            min_metal_deberta_ffn_fused_call_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-metal-deberta-ffn-fused-fallback-count")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_metal_deberta_ffn_fused_fallback_count = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, arg, "--max-runtime-frame-ineligible-missing-model-metadata")) {
            const value = args.next() orelse {
                std.debug.print("error: missing value for {s}\n", .{arg});
                printUsage();
                return error.InvalidArguments;
            };
            max_runtime_frame_ineligible_missing_model_metadata = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (out_dir == null) {
            out_dir = arg;
        } else {
            std.debug.print("error: unknown argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArguments;
        }
    }

    const dir = out_dir orelse {
        printUsage();
        return error.InvalidArguments;
    };

    var summary = try validation.validateRun(allocator, dir, .{
        .require_loss_decrease = require_loss_decrease,
        .min_supervised_tokens_per_second = min_supervised_tokens_per_second,
        .max_avg_step_wall_ms = max_avg_step_wall_ms,
        .max_total_execute_ms = max_total_execute_ms,
        .max_peak_resident_bytes = max_peak_resident_bytes,
        .max_metal_eager_arena_peak_bytes = max_metal_eager_arena_peak_bytes,
        .max_metal_eager_arena_spill_bytes = max_metal_eager_arena_spill_bytes,
        .max_metal_chunk_local_output_peak_bytes = max_metal_chunk_local_output_peak_bytes,
        .max_metal_chunk_local_output_spill_bytes = max_metal_chunk_local_output_spill_bytes,
        .max_metal_chunk_local_output_unconsumed_hints = max_metal_chunk_local_output_unconsumed_hints,
        .min_metal_chunk_local_output_consumed_hints = min_metal_chunk_local_output_consumed_hints,
        .min_examples = min_examples,
        .min_steps = min_steps,
        .min_entity_labels = min_entity_labels,
        .min_supervised_tokens = min_supervised_tokens,
        .min_entity_tokens = min_entity_tokens,
        .max_graph_command_dispatch_count = max_graph_command_dispatch_count,
        .max_graph_host_output_count = max_graph_host_output_count,
        .max_metal_frame_gpu_ms = max_metal_frame_gpu_ms,
        .max_metal_last_frame_compute_encoder_count = max_metal_last_frame_compute_encoder_count,
        .min_metal_frame_chunk_boundary_count = min_metal_frame_chunk_boundary_count,
        .min_metal_frame_chunk_promoted_value_count = min_metal_frame_chunk_promoted_value_count,
        .min_metal_frame_chunk_swept_value_count = min_metal_frame_chunk_swept_value_count,
        .min_graph_runtime_region_dispatch_count = min_graph_runtime_region_dispatch_count,
        .max_graph_runtime_region_fallback_count = max_graph_runtime_region_fallback_count,
        .min_graph_runtime_region_elided_node_count = min_graph_runtime_region_elided_node_count,
        .min_metal_deberta_ffn_forward_region_count = min_metal_deberta_ffn_forward_region_count,
        .min_metal_deberta_encoder_lora_layer_region_count = min_metal_deberta_encoder_lora_layer_region_count,
        .min_metal_deberta_encoder_lora_residual_layernorm_region_count = min_metal_deberta_encoder_lora_residual_layernorm_region_count,
        .max_metal_deberta_encoder_lora_layer_scaffold_count = max_metal_deberta_encoder_lora_layer_scaffold_count,
        .max_metal_deberta_encoder_lora_layer_fallback_count = max_metal_deberta_encoder_lora_layer_fallback_count,
        .min_metal_deberta_attention_flash_call_count = min_metal_deberta_attention_flash_call_count,
        .max_metal_deberta_attention_gemm_fallback_count = max_metal_deberta_attention_gemm_fallback_count,
        .min_metal_deberta_encoder_layer_success_count = min_metal_deberta_encoder_layer_success_count,
        .min_metal_deberta_ffn_fused_call_count = min_metal_deberta_ffn_fused_call_count,
        .max_metal_deberta_ffn_fused_fallback_count = max_metal_deberta_ffn_fused_fallback_count,
        .max_runtime_frame_ineligible_missing_model_metadata = max_runtime_frame_ineligible_missing_model_metadata,
    });
    defer validation.freeRunValidationSummary(allocator, &summary);

    const io = init.io;
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn printUsage() void {
    std.debug.print(
        \\usage: validate-gliner2-autodiff-run <out_dir> [--require-loss-decrease] [--min-supervised-tokens-per-second <f64>] [--max-avg-step-wall-ms <f64>] [--max-total-execute-ms <f64>] [--max-peak-resident-bytes <n>] [--max-metal-eager-arena-peak-bytes <n>] [--max-metal-eager-arena-spill-bytes <n>] [--max-metal-chunk-local-output-peak-bytes <n>] [--max-metal-chunk-local-output-spill-bytes <n>] [--max-metal-chunk-local-output-unconsumed-hints <n>] [--min-metal-chunk-local-output-consumed-hints <n>] [--min-examples <n>] [--min-steps <n>] [--min-entity-labels <n>] [--min-supervised-tokens <n>] [--min-entity-tokens <n>] [--max-graph-command-dispatch-count <n>] [--max-graph-host-output-count <n>] [--max-metal-frame-gpu-ms <f64>] [--max-metal-last-frame-compute-encoder-count <n>] [--min-metal-frame-chunk-boundary-count <n>] [--min-metal-frame-chunk-promoted-value-count <n>] [--min-metal-frame-chunk-swept-value-count <n>] [--min-graph-runtime-region-dispatch-count <n>] [--max-graph-runtime-region-fallback-count <n>] [--min-graph-runtime-region-elided-node-count <n>] [--min-metal-deberta-ffn-forward-region-count <n>] [--min-metal-deberta-encoder-lora-layer-region-count <n>] [--min-metal-deberta-encoder-lora-residual-layernorm-region-count <n>] [--max-metal-deberta-encoder-lora-layer-scaffold-count <n>] [--max-metal-deberta-encoder-lora-layer-fallback-count <n>] [--min-metal-deberta-attention-flash-call-count <n>] [--max-metal-deberta-attention-gemm-fallback-count <n>] [--min-metal-deberta-encoder-layer-success-count <n>] [--min-metal-deberta-ffn-fused-call-count <n>] [--max-metal-deberta-ffn-fused-fallback-count <n>] [--max-runtime-frame-ineligible-missing-model-metadata <n>]
        \\example: validate-gliner2-autodiff-run /tmp/gliner2-run --require-loss-decrease --min-supervised-tokens-per-second 10 --max-avg-step-wall-ms 1000 --max-total-execute-ms 50000 --max-peak-resident-bytes 2000000000 --min-examples 100 --min-steps 100 --min-entity-labels 2 --min-supervised-tokens 1000 --min-entity-tokens 100
        \\
        \\Validates a train-gliner2-autodiff output directory containing:
        \\  training_manifest.json
        \\  training_metrics.jsonl
        \\  one or more saved LoRA parameter .bin files
        \\  adapter_model.safetensors + adapter_config.json
        \\  task_head.safetensors
        \\
    , .{});
}
