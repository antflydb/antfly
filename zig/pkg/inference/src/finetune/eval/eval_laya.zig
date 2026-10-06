// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const evaluate = @import("inference_internal").finetune.laya_evaluate;

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    var options = evaluate.Options{ .model_dir = "", .records_file = "" };
    var positional: usize = 0;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) return help();
        if (std.mem.eql(u8, arg, "--backend")) {
            const value = args.next() orelse return usage();
            options.backend = std.meta.stringToEnum(@TypeOf(options.backend), value) orelse return usage();
        } else if (std.mem.eql(u8, arg, "--chunk")) {
            const value = args.next() orelse return usage();
            options.chunk = std.fmt.parseInt(usize, value, 10) catch return usage();
            if (options.chunk == 0 or options.chunk > 512) return usage();
        } else if (std.mem.eql(u8, arg, "--truncate-state")) {
            options.truncate_state = true;
        } else if (std.mem.eql(u8, arg, "--predictions")) {
            options.predictions_file = args.next() orelse return usage();
        } else if (std.mem.eql(u8, arg, "--top-k-recall")) {
            const value = args.next() orelse return usage();
            options.top_k_recall = std.fmt.parseInt(usize, value, 10) catch return usage();
        } else if (positional == 0) {
            options.model_dir = arg;
            positional += 1;
        } else if (positional == 1) {
            options.records_file = arg;
            positional += 1;
        } else return usage();
    }
    if (positional != 2) return usage();
    const report = try evaluate.run(init.gpa, init.io, options);
    defer evaluate.deinitReport(init.gpa, report);
    var buffer: [4096]u8 = undefined;
    var w = std.Io.File.stdout().writer(init.io, &buffer);
    try std.json.Stringify.value(report, .{ .whitespace = .indent_2 }, &w.interface);
    try w.interface.writeByte('\n');
    try w.interface.flush();
}
fn usage() error{InvalidArguments} {
    help();
    return error.InvalidArguments;
}
fn help() void {
    std.debug.print(
        \\usage: antfly inference finetune eval laya <model_dir> <records.jsonl> [--backend metal|native] [--top-k-recall N] [--predictions out.jsonl] [--truncate-state] [--chunk N]
        \\Scores a prepared Laya checkpoint on native training records through the
        \\serving pipeline (packed or unpacked per the model config, with its
        \\calibration). Prints accuracy, soft CE, ECE, and ordinal MAE as JSON.
        \\--top-k-recall N also reports the fraction of choice decisions whose
        \\gold label is among the N highest probabilities (bounds two-stage
        \\choice's stage 2, LAYA.md roadmap 2b, run against a model with
        \\packing.two_stage unset to measure stage 1 alone).
        \\--predictions writes each decision's id, labels, probabilities and target
        \\as JSON lines to a new file (scripts/laya/typed_decisions_bench.py
        \\compares models on them).
        \\--truncate-state cuts the end of a state that does not fit the model's
        \\max_len instead of failing, as upstream Laya and OpenDecider do; serving
        \\never truncates.
        \\--chunk N sets the tasks per pipeline call (default 64, at most 512); a
        \\packed model's case always stays in one call.
        \\
    , .{});
}
