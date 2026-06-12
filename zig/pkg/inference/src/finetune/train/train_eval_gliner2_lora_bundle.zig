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
const gliner2 = @import("../gliner2.zig");
const gliner2_boundary = @import("../gliner2_boundary.zig");
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
    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    var learning_rate: f32 = 1e-4;
    var max_examples: usize = 0;
    var epochs: usize = 1;
    var layer_name: ?[]const u8 = null;
    var max_grad_norm: f32 = 1.0;
    var grad_accum_steps: u32 = 1;
    var llrd_decay: f32 = 1.0;
    var use_schedule_free: bool = false;

    var i: usize = 0;
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
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 5) return usageError();
    const base_model_dir = positional.items[0];
    const adapter_model_dir = positional.items[1];
    const train_cache_path = positional.items[2];
    const eval_cache_path = positional.items[3];
    const out_dir = positional.items[4];

    const backend_ptr: ?*const ComputeBackend = null;
    std.debug.print("backend: native\n", .{});

    var train_summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, train_cache_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &train_summary);
    var eval_summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, eval_cache_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &eval_summary);

    var bundle = try gliner2.loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
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
                    1,
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

    var summary = try gliner2.trainEvalLoRABundleCached(
        allocator,
        &bundle,
        &train_summary,
        &eval_summary,
        out_dir,
        .{
            .learning_rate = learning_rate,
            .max_examples = max_examples,
            .layer_name = layer_name,
            .max_grad_norm = max_grad_norm,
            .grad_accum_steps = grad_accum_steps,
            .llrd_decay = llrd_decay,
            .use_schedule_free = use_schedule_free,
            .compute_backend = backend_ptr,
            .pjrt_lora_steps = if (comptime build_options.enable_pjrt) pjrt_lora_steps else {},
        },
        epochs,
    );
    defer summary.deinit(allocator);

    const training_config_path = try std.fs.path.join(allocator, &.{ out_dir, "training_config.json" });
    defer allocator.free(training_config_path);
    try artifact_writer.writeJsonFile(allocator, training_config_path, .{
        .contract_version = run_contract.training_config_version,
        .artifact_family_version = gliner2.artifact_family_version,
        .task = "gliner2_lora_train_eval",
        .inputs = .{
            .base_model_dir = base_model_dir,
            .adapter_model_dir = adapter_model_dir,
            .train_cache_path = train_cache_path,
            .eval_cache_path = eval_cache_path,
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
            .world_size = 1,
        },
    });

    const report_path = try std.fs.path.join(allocator, &.{ out_dir, "train_eval_lora_report.json" });
    defer allocator.free(report_path);
    try artifact_writer.writeJsonFile(allocator, report_path, summary);
    const training_report_path = try std.fs.path.join(allocator, &.{ out_dir, "training_report.json" });
    defer allocator.free(training_report_path);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = gliner2.artifact_family_version,
        .task = "gliner2_lora_train_eval",
        .backend_policy = .{
            .selected = "native",
            .preferred = "native",
        },
        .distributed = .{
            .enabled = false,
            .backend = "native",
            .world_size = 1,
        },
        .report = summary,
    });

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: train-eval-gliner2-lora-bundle <base_model_dir> <adapter_model_dir> <train_cache.json> <eval_cache.json> <out_dir> [options]
        \\
        \\Flags:
        \\  --lr, --learning-rate <f32>   Learning rate (default: 0.0001)
        \\  --max-examples <usize>        Max training examples (default: 0 = all)
        \\  --epochs <usize>              Number of epochs (default: 1)
        \\  --layer-name, --layer <str>   Scope to a specific layer name
        \\  --max-grad-norm <f32>         Gradient norm clipping threshold (default: 1.0, 0=disabled)
        \\  --grad-accum <u32>            Gradient accumulation steps (default: 1)
        \\  --llrd-decay <f32>            Layer-wise LR decay factor (default: 1.0=disabled)
        \\  --schedule-free               Use Schedule-Free AdamW optimizer (default: off)
        \\  --backend auto|native           Compute backend for gradient math (default: auto)
        \\
        \\Inputs:
        \\  train_cache.json / eval_cache.json are produced by prepare-gliner2-top-layer-boundary-cache
        \\
        \\example: train-eval-gliner2-lora-bundle /tmp/gliner2 /tmp/lora /tmp/train_cache.json \
        \\           /tmp/eval_cache.json /tmp/out \
        \\           --lr 0.0001 --epochs 3 --max-grad-norm 1.0 --grad-accum 4 --llrd-decay 0.9 --backend native
        \\
    , .{});
    return error.InvalidArguments;
}
