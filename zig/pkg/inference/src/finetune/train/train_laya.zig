// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const job = @import("inference_internal").finetune.laya_job;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const file = args.next() orelse return usage();
    if (std.mem.eql(u8, file, "--help") or std.mem.eql(u8, file, "-h")) return help();
    if (args.next() != null) return usage();
    const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, file, init.gpa, .limited(1024 * 1024));
    defer init.gpa.free(bytes);
    const parsed = try std.json.parseFromSlice(job.Config, init.gpa, bytes, .{});
    defer parsed.deinit();
    try job.execute(init.gpa, init.io, parsed.value);
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}
fn help() void {
    std.debug.print(
        \\usage: antfly inference finetune train laya <job.json>
        \\Native full finetuning of prepared ModernBERT Laya checkpoints on cpu or metal.
        \\The job specifies absolute model_dir, train_file, eval_file, and a new output_dir.
        \\JSONL records contain id, group_id, text, kind, instruction, labels, target,
        \\and optional descriptions. kind is choice, score, or noul; targets sum to one.
        \\objective is rlcd (default) or soft_ce. calibration_file is an optional third split.
        \\Resume with resume_from pointing to latest.safetensors and a new output directory.
        \\Completed runs export a serving checkpoint to output_dir/model and report.json.
        \\
    , .{});
}
