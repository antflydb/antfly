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
const finetune = @import("../layoutlmv3.zig");
const document_data = @import("../document_data.zig");
const build_options = @import("build_options");
const run_contract = @import("../../run/contract.zig");
const artifact_writer = @import("../../run/artifact_writer.zig");

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

    var max_grad_norm: f32 = 1.0;
    var llrd_decay: f32 = 1.0;
    var grad_accum_steps: u32 = 1;
    var use_schedule_free: bool = false;

    var i: usize = 0;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--max-grad-norm")) {
            i += 1;
            if (i >= argv.len) return error.MissingValue;
            const value = argv[i];
            max_grad_norm = try std.fmt.parseFloat(f32, value);
        } else if (std.mem.eql(u8, arg, "--llrd-decay")) {
            i += 1;
            if (i >= argv.len) return error.MissingValue;
            const value = argv[i];
            llrd_decay = try std.fmt.parseFloat(f32, value);
        } else if (std.mem.eql(u8, arg, "--grad-accum")) {
            i += 1;
            if (i >= argv.len) return error.MissingValue;
            const value = argv[i];
            grad_accum_steps = try std.fmt.parseUnsigned(u32, value, 10);
        } else if (std.mem.eql(u8, arg, "--schedule-free")) {
            use_schedule_free = true;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= argv.len) return usageError();
            const val = argv[i];
            if (!std.mem.eql(u8, val, "native") and !std.mem.eql(u8, val, "auto")) return usageError();
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len < 5) return usageError();
    const base_model_dir = positional.items[0];
    const adapter_model_dir = positional.items[1];
    const train_input = positional.items[2];
    const val_input = positional.items[3];
    const out_dir = positional.items[4];
    const max_train_examples_arg = if (positional.items.len >= 6) positional.items[5] else "128";
    const learning_rate_arg = if (positional.items.len >= 7) positional.items[6] else "0.001";
    const max_val_examples_arg = if (positional.items.len >= 8) positional.items[7] else "64";
    const epochs_arg = if (positional.items.len >= 9) positional.items[8] else "4";
    const layer_name: ?[]const u8 = if (positional.items.len >= 10) positional.items[9] else null;

    const max_train_examples = try std.fmt.parseUnsigned(usize, max_train_examples_arg, 10);
    const learning_rate = try std.fmt.parseFloat(f32, learning_rate_arg);
    const max_val_examples = try std.fmt.parseUnsigned(usize, max_val_examples_arg, 10);
    const epochs = try std.fmt.parseUnsigned(usize, epochs_arg, 10);

    std.debug.print("backend: native\n", .{});

    var train_loaded = try document_data.loadExamples(allocator, train_input, "train");
    defer train_loaded.deinit();
    var val_loaded = try document_data.loadExamples(allocator, val_input, "val");
    defer val_loaded.deinit();

    const train_examples = try document_data.filterSequenceExamples(allocator, train_loaded.dataset_root, train_loaded.examples);
    defer document_data.freeSequenceExamples(allocator, train_examples);
    const val_examples = try document_data.filterSequenceExamples(allocator, val_loaded.dataset_root, val_loaded.examples);
    defer document_data.freeSequenceExamples(allocator, val_examples);

    const label_vocab = try buildCombinedLabelVocab(allocator, train_loaded.examples, val_loaded.examples);
    defer {
        for (label_vocab) |label| allocator.free(label);
        allocator.free(label_vocab);
    }
    try requireSequenceLabelVocab(label_vocab);

    var bundle = try finetune.loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();
    var summary = try finetune.trainEvalSequenceLoRABundle(allocator, &bundle, train_examples, val_examples, label_vocab, out_dir, .{
        .max_train_examples = max_train_examples,
        .max_val_examples = max_val_examples,
        .epochs = epochs,
        .learning_rate = learning_rate,
        .layer_name = layer_name,
        .max_grad_norm = max_grad_norm,
        .llrd_decay = llrd_decay,
        .grad_accum_steps = grad_accum_steps,
        .use_schedule_free = use_schedule_free,
        .compute_backend = null,
    });
    defer finetune.freeSequenceTrainEvalSummary(allocator, &summary);

    const training_config_path = try std.fs.path.join(allocator, &.{ out_dir, "training_config.json" });
    defer allocator.free(training_config_path);
    try artifact_writer.writeJsonFile(allocator, training_config_path, .{
        .contract_version = run_contract.training_config_version,
        .artifact_family_version = finetune.artifact_family_version,
        .task = "layoutlmv3_lora_sequence_train_eval",
        .inputs = .{
            .base_model_dir = base_model_dir,
            .adapter_model_dir = adapter_model_dir,
            .train_input = train_input,
            .val_input = val_input,
            .label_vocab = label_vocab,
        },
        .training = .{
            .max_train_examples = max_train_examples,
            .max_val_examples = max_val_examples,
            .learning_rate = learning_rate,
            .epochs = epochs,
            .layer_name = layer_name,
            .max_grad_norm = max_grad_norm,
            .llrd_decay = llrd_decay,
            .grad_accum_steps = grad_accum_steps,
            .use_schedule_free = use_schedule_free,
        },
        .backend_policy = .{
            .selected = "native",
            .preferred = "native",
        },
    });

    const report_path = try std.fs.path.join(allocator, &.{ out_dir, "sequence_train_eval_report.json" });
    defer allocator.free(report_path);
    const report_payload = .{
        .artifact_family_version = finetune.artifact_family_version,
        .train_input = train_input,
        .val_input = val_input,
        .label_vocab = label_vocab,
        .summary = summary,
    };
    try artifact_writer.writeJsonFile(allocator, report_path, report_payload);
    const training_report_path = try std.fs.path.join(allocator, &.{ out_dir, "training_report.json" });
    defer allocator.free(training_report_path);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = finetune.artifact_family_version,
        .task = "layoutlmv3_lora_sequence_train_eval",
        .backend_policy = .{
            .selected = "native",
            .preferred = "native",
        },
        .report = report_payload,
    });

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn buildCombinedLabelVocab(
    allocator: std.mem.Allocator,
    train_examples: []const document_data.PageExample,
    val_examples: []const document_data.PageExample,
) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    for (train_examples) |ex| {
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
        }
    }
    for (val_examples) |ex| {
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
        }
    }

    var keys = try allocator.alloc([]const u8, labels.count());
    errdefer allocator.free(keys);
    var it = labels.iterator();
    var idx: usize = 0;
    while (it.next()) |entry| : (idx += 1) {
        keys[idx] = try allocator.dupe(u8, entry.key_ptr.*);
    }
    std.mem.sort([]const u8, keys, {}, struct {
        fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);
    return keys;
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: train-eval-layoutlmv3-lora-sequence <base_model_dir> <adapter_model_dir> <train_jsonl_or_dir> <val_jsonl_or_dir> <out_dir> [max_train_examples] [learning_rate] [max_val_examples] [epochs] [layer_name|@layoutlmv3_token_top1|@layoutlmv3_token_top3|@layoutlmv3_sequence_top3] [--max-grad-norm F] [--llrd-decay F] [--grad-accum N] [--schedule-free] [--backend auto|native]
        \\example: train-eval-layoutlmv3-lora-sequence /tmp/layoutlmv3_base /tmp/layoutlmv3_lora /tmp/train.jsonl /tmp/val.jsonl /tmp/layoutlmv3_seq 128 0.001 64 4 @layoutlmv3_sequence_top3 --max-grad-norm 1.0 --llrd-decay 0.9 --grad-accum 4 --schedule-free --backend native
        \\
    , .{});
    return error.InvalidArguments;
}

fn requireSequenceLabelVocab(label_vocab: []const []const u8) !void {
    if (label_vocab.len == 0) {
        std.debug.print("error: no non-empty sequence labels were found in the provided train/val inputs\n", .{});
        return error.EmptyLabelVocabulary;
    }
    if (label_vocab.len < 2) {
        std.debug.print("error: sequence training requires at least 2 distinct labels, found {d}\n", .{label_vocab.len});
        return error.InsufficientLabelVocabulary;
    }
}
