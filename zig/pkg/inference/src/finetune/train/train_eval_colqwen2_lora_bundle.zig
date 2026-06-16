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
const finetune = @import("../colqwen2.zig");
const graph_bridge = @import("../graph_bridge.zig");
const build_options = @import("build_options");
const run_contract = @import("../../run/contract.zig");
const artifact_writer = @import("../../run/artifact_writer.zig");
const ops_mod = @import("../../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const pjrt_mod = if (build_options.enable_pjrt) @import("pjrt") else struct {
    pub const pjrt = struct {
        pub const Client = void;
    };
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var argv: std.ArrayListUnmanaged([]const u8) = .empty;
    defer argv.deinit(allocator);
    while (args.next()) |arg| try argv.append(allocator, arg);
    try runFromArgs(allocator, init.io, argv.items);
}

pub fn runFromArgs(allocator: std.mem.Allocator, io: std.Io, argv: []const []const u8) !void {
    if (argv.len < 4) return usageError();

    const base_model_dir = argv[0];
    const adapter_model_dir = argv[1];
    const prepared_inputs_path = argv[2];
    const out_dir = argv[3];

    // Defaults for options
    var learning_rate: f32 = 0.001;
    var max_examples: usize = 32;
    var epochs: usize = 1;
    var layer_name: ?[]const u8 = null;
    var max_grad_norm: f32 = 1.0;
    var grad_accum_steps: u32 = 1;
    var llrd_decay: f32 = 1.0;
    var use_schedule_free: bool = false;

    // Parse remaining positional args (legacy positional support) then flags.
    // We handle both: legacy positional order and new --flag style.
    var positional_count: usize = 0;
    var i: usize = 4;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--lr") or std.mem.eql(u8, arg, "--learning-rate")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            learning_rate = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--max-examples")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            max_examples = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--epochs")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            epochs = try std.fmt.parseUnsigned(usize, val, 10);
        } else if (std.mem.eql(u8, arg, "--layer-name") or std.mem.eql(u8, arg, "--layer")) {
            i += 1;
            if (i >= argv.len) return usageError();
            layer_name = argv[i];
        } else if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            max_grad_norm = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            grad_accum_steps = try std.fmt.parseUnsigned(u32, val, 10);
        } else if (std.mem.eql(u8, arg, "--llrd-decay")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            llrd_decay = try std.fmt.parseFloat(f32, val);
        } else if (std.mem.eql(u8, arg, "--schedule-free")) {
            use_schedule_free = true;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            if (!std.mem.eql(u8, val, "native") and !std.mem.eql(u8, val, "native") and !std.mem.eql(u8, val, "auto")) return usageError();
        } else {
            // Legacy positional: lr, max_examples, epochs, layer_name
            switch (positional_count) {
                0 => learning_rate = try std.fmt.parseFloat(f32, arg),
                1 => max_examples = try std.fmt.parseUnsigned(usize, arg, 10),
                2 => epochs = try std.fmt.parseUnsigned(usize, arg, 10),
                3 => layer_name = arg,
                else => return usageError(),
            }
            positional_count += 1;
        }
    }

    const backend_ptr: ?*const ComputeBackend = null;
    std.debug.print("backend: native\n", .{});

    var prepared = try finetune.loadPreparedInputsSummary(allocator, prepared_inputs_path);
    defer finetune.freePreparedInputsSummary(allocator, &prepared);

    var bundle = try finetune.loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();

    // Initialize PJRT client if available.
    const PjrtClientT = if (build_options.enable_pjrt) ?pjrt_mod.pjrt.Client else void;
    var pjrt_client_storage: PjrtClientT = if (comptime build_options.enable_pjrt) null else {};
    if (comptime build_options.enable_pjrt) {
        pjrt_client_storage = pjrt_mod.pjrt.Client.initFromEnv(allocator) catch |err| blk: {
            std.log.warn("PJRT client init failed ({s}); LoRA gradients will use CPU", .{@errorName(err)});
            break :blk null;
        };
    }
    defer if (comptime build_options.enable_pjrt) {
        if (pjrt_client_storage) |*client| client.deinit();
    };

    // Pre-compile PJRT LoRA gradient steps (one per layer, amortized over all epochs/examples).
    const PjrtStepsT = if (build_options.enable_pjrt) ?[]?graph_bridge.LoRAPjrtTrainStep else void;
    var pjrt_lora_steps: PjrtStepsT = if (comptime build_options.enable_pjrt) null else {};
    if (comptime build_options.enable_pjrt) {
        if (pjrt_client_storage) |*pjrt_client| {
            const steps = try allocator.alloc(?graph_bridge.LoRAPjrtTrainStep, bundle.layers.len);
            @memset(steps, null);
            var compiled_count: usize = 0;
            for (bundle.layers, 0..) |*layer, li| {
                var layer_graph = graph_bridge.LoRALinearGraph.init(
                    allocator,
                    3,
                    layer.input_dim,
                    layer.output_dim,
                    layer.rank,
                    bundle.lora_alpha,
                ) catch continue;
                steps[li] = graph_bridge.compileLoRALinearPjrtStep(allocator, &layer_graph, pjrt_client) catch blk: {
                    layer_graph.deinit();
                    break :blk null;
                };
                if (steps[li] != null) {
                    layer_graph.deinit();
                    compiled_count += 1;
                }
            }
            std.log.info("PJRT: compiled {d}/{d} LoRA layers", .{ compiled_count, bundle.layers.len });
            pjrt_lora_steps = steps;
        }
    }
    defer if (comptime build_options.enable_pjrt) {
        if (pjrt_lora_steps) |steps| {
            for (steps) |*step_opt| if (step_opt.*) |*step| step.deinit();
            allocator.free(steps);
        }
    };

    const before = try finetune.evaluatePreparedExamples(allocator, &bundle, prepared.examples, .{
        .max_examples = max_examples,
        .layer_name = layer_name,
    });
    const epoch_history = try allocator.alloc(finetune.TrainEpochSummary, epochs);
    defer allocator.free(epoch_history);
    for (0..epochs) |epoch_idx| {
        epoch_history[epoch_idx] = try finetune.trainPreparedExamplesEpoch(allocator, &bundle, prepared.examples, .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
            .layer_name = layer_name,
            .max_grad_norm = max_grad_norm,
            .grad_accum_steps = grad_accum_steps,
            .llrd_decay = llrd_decay,
            .use_schedule_free = use_schedule_free,
            .compute_backend = backend_ptr,
            .pjrt_lora_steps = if (comptime build_options.enable_pjrt) pjrt_lora_steps else {},
        });
        const ep = &epoch_history[epoch_idx];
        std.log.info("colqwen2 train: epoch={d}/{d} loss={d:.4} examples={d} updates={d}", .{ epoch_idx + 1, epochs, ep.average_loss, ep.examples_seen, ep.updates_applied });
    }
    const after = try finetune.evaluatePreparedExamples(allocator, &bundle, prepared.examples, .{
        .max_examples = max_examples,
        .layer_name = layer_name,
    });

    try finetune.saveLoRABundle(&bundle, out_dir);
    std.log.info("colqwen2 checkpoint: saved={s}", .{out_dir});

    const training_config_path = try std.fs.path.join(allocator, &.{ out_dir, "training_config.json" });
    defer allocator.free(training_config_path);
    try artifact_writer.writeJsonFile(allocator, training_config_path, .{
        .contract_version = run_contract.training_config_version,
        .artifact_family_version = finetune.artifact_family_version,
        .task = "colqwen2_lora_train_eval",
        .inputs = .{
            .base_model_dir = base_model_dir,
            .adapter_model_dir = adapter_model_dir,
            .prepared_inputs_path = prepared_inputs_path,
        },
        .training = .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
            .epochs = epochs,
            .layer_name = layer_name,
            .max_grad_norm = max_grad_norm,
            .grad_accum_steps = grad_accum_steps,
            .llrd_decay = llrd_decay,
            .use_schedule_free = use_schedule_free,
        },
        .backend_policy = .{
            .selected = "native",
            .preferred = "native",
        },
        .distributed = .{
            .enabled = false,
            .backend = "native",
            .rank = 0,
            .world_size = 1,
            .primary_rank = 0,
        },
    });

    const report_path = try std.fs.path.join(allocator, &.{ out_dir, "train_eval_report.json" });
    defer allocator.free(report_path);
    const report_payload = .{
        .artifact_family_version = finetune.artifact_family_version,
        .prepared_inputs_path = prepared_inputs_path,
        .saved_adapter_checkpoint = finetune.adapter_checkpoint_file_name,
        .learning_rate = learning_rate,
        .max_examples = max_examples,
        .epochs = epochs,
        .layer_name = layer_name,
        .max_grad_norm = max_grad_norm,
        .grad_accum_steps = grad_accum_steps,
        .llrd_decay = llrd_decay,
        .use_schedule_free = use_schedule_free,
        .before = before,
        .epoch_history = epoch_history,
        .after = after,
    };
    try artifact_writer.writeJsonFile(allocator, report_path, report_payload);
    const training_report_path = try std.fs.path.join(allocator, &.{ out_dir, "training_report.json" });
    defer allocator.free(training_report_path);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = finetune.artifact_family_version,
        .task = "colqwen2_lora_train_eval",
        .backend_policy = .{
            .selected = "native",
            .preferred = "native",
        },
        .distributed = .{
            .enabled = false,
            .backend = "native",
            .rank = 0,
            .world_size = 1,
            .primary_rank = 0,
        },
        .report = report_payload,
    });

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(.{
        .before = before,
        .epoch_history = epoch_history,
        .after = after,
    }, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: train-eval-colqwen2-lora-bundle <base_model_dir> <adapter_model_dir> <prepared_inputs_json> <out_dir> [options]
        \\
        \\Positional (legacy):
        \\  [learning_rate] [max_examples] [epochs] [layer_name|@colqwen2_focus_top3]
        \\
        \\Flags:
        \\  --lr, --learning-rate <f32>   Learning rate (default: 0.001)
        \\  --max-examples <usize>        Max examples per epoch (default: 32)
        \\  --epochs <usize>              Number of epochs (default: 1)
        \\  --layer-name, --layer <str>   Scope to a layer name or @colqwen2_focus_top3
        \\  --max-grad-norm <f32>         Gradient norm clipping threshold (default: 1.0, 0=disabled)
        \\  --grad-accum <u32>            Gradient accumulation steps (default: 1)
        \\  --llrd-decay <f32>            Layer-wise LR decay factor (default: 1.0=disabled)
        \\  --schedule-free               Enable schedule-free mode (default: false)
        \\  --backend auto|native           Compute backend for gradient math (default: auto)
        \\
        \\example: train-eval-colqwen2-lora-bundle /tmp/base /tmp/lora /tmp/inputs.json /tmp/out \
        \\           --lr 0.0003 --max-examples 64 --epochs 3 --max-grad-norm 1.0 --grad-accum 4 \
        \\           --llrd-decay 0.9 --layer @colqwen2_focus_top3 --backend native
        \\
    , .{});
    return error.InvalidArguments;
}
