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
const termite = @import("termite_internal");
const finetune = termite.finetune.gemma4;
const peft = termite.finetune.peft;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const out_dir = args.next() orelse return usageError();
    var rank: usize = 16;
    var alpha: f32 = 32.0;
    var rank_set = false;
    var alpha_set = false;
    var rank_alpha_flag_seen = false;
    var base_model_name_or_path: ?[]const u8 = null;
    var layer_name: ?[]const u8 = null;
    var target_preset: ?peft.TargetPreset = null;
    var target_modules: ?[]const []const u8 = null;
    defer if (target_modules) |modules| allocator.free(modules);
    var use_dora = false;
    var init_lora_weights: ?[]const u8 = null;
    var eva_stats_path: ?[]const u8 = null;
    var lora_ga_stats_path: ?[]const u8 = null;

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--rank")) {
            if (rank_set) return usageError();
            rank = try std.fmt.parseUnsigned(usize, args.next() orelse return usageError(), 10);
            rank_set = true;
            rank_alpha_flag_seen = true;
        } else if (std.mem.eql(u8, arg, "--alpha")) {
            if (alpha_set) return usageError();
            alpha = try std.fmt.parseFloat(f32, args.next() orelse return usageError());
            alpha_set = true;
            rank_alpha_flag_seen = true;
        } else if (std.mem.eql(u8, arg, "--layer-name") or std.mem.eql(u8, arg, "--layer")) {
            layer_name = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--target-preset")) {
            const preset_name = args.next() orelse return usageError();
            target_preset = peft.parseTargetPreset(preset_name) orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--target-modules")) {
            if (target_modules != null) return usageError();
            target_modules = try parseTargetModules(allocator, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--use-dora")) {
            use_dora = true;
        } else if (std.mem.eql(u8, arg, "--init-lora-weights")) {
            init_lora_weights = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--eva-stats")) {
            eva_stats_path = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--lora-ga-stats")) {
            lora_ga_stats_path = args.next() orelse return usageError();
        } else if (!rank_alpha_flag_seen and !rank_set) {
            rank = try std.fmt.parseUnsigned(usize, arg, 10);
            rank_set = true;
        } else if (!rank_alpha_flag_seen and !alpha_set) {
            alpha = try std.fmt.parseFloat(f32, arg);
            alpha_set = true;
        } else if (base_model_name_or_path == null) {
            base_model_name_or_path = arg;
        } else {
            return usageError();
        }
    }

    if (target_modules != null and target_preset != null) return usageError();
    const effective_target_preset = if (target_modules == null) target_preset orelse .all_linear else null;

    var summary = try finetune.bootstrapLoRABundle(allocator, model_dir, out_dir, .{
        .rank = rank,
        .alpha = alpha,
        .base_model_name_or_path = base_model_name_or_path,
        .layer_name = layer_name,
        .target_modules = target_modules,
        .target_preset = effective_target_preset,
        .use_dora = use_dora,
        .init_lora_weights = init_lora_weights,
        .eva_stats_path = eva_stats_path,
        .lora_ga_stats_path = lora_ga_stats_path,
    });
    defer finetune.freeBootstrapSummary(allocator, &summary);

    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: bootstrap-gemma4-lora <model_dir> <out_dir> [rank] [alpha] [base_model_name_or_path]
        \\       [--rank <n>] [--alpha <float>]
        \\       [--layer-name <substring>] [--target-preset all-linear|attention-only|mlp-only|moe-experts]
        \\       [--target-modules q_proj,k_proj,v_proj,o_proj,gate_proj,up_proj,down_proj]
        \\       [--use-dora] [--init-lora-weights default|pissa|eva|lora-ga|loftq|loftq-nf4]
        \\       [--eva-stats <safetensors>] [--lora-ga-stats <safetensors>]
        \\example: bootstrap-gemma4-lora /tmp/gemma4-base /tmp/gemma4-lora 16 32 google/gemma-4 --target-preset all-linear
        \\EVA stats tensors are named <base_tensor>.eva_activation_covariance with shape [in,in].
        \\LoRA-GA stats tensors are named <base_tensor>.lora_ga_gradient with shape [out,in].
        \\
    , .{});
    return error.InvalidArguments;
}

fn parseTargetModules(allocator: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var modules = std.ArrayList([]const u8).empty;
    errdefer modules.deinit(allocator);
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        try modules.append(allocator, item);
    }
    if (modules.items.len == 0) return error.InvalidArguments;
    return modules.toOwnedSlice(allocator);
}
