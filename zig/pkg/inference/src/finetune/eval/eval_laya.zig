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
        \\usage: antfly inference finetune eval laya <model_dir> <records.jsonl> [--backend metal|native]
        \\Scores a prepared Laya checkpoint on native training records through the
        \\serving pipeline (packed or unpacked per the model config, with its
        \\calibration). Prints accuracy, soft CE, ECE, and ordinal MAE as JSON.
        \\
    , .{});
}
