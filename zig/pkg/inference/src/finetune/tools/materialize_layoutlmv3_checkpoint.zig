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
const inference = @import("inference_internal");
const compat = inference.io.compat;
const finetune = inference.finetune.layoutlmv3;
const peft = inference.finetune.peft;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var eval_program: ?[]const u8 = null;
    var positional = std.ArrayListUnmanaged([]const u8).empty;
    defer positional.deinit(allocator);

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--eval")) {
            eval_program = args.next() orelse return usageError();
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            return usageError();
        } else {
            try positional.append(allocator, arg);
        }
    }

    if (positional.items.len != 4 and positional.items.len != 5) return usageError();
    const base_model_dir = positional.items[0];
    const adapter_model_dir = positional.items[1];
    const task = positional.items[2];
    const out_dir = positional.items[3];
    const report_path: ?[]const u8 = if (positional.items.len == 5) positional.items[4] else null;

    if (eval_program) |program| {
        var eval_before = try peft.runEvalCapture(allocator, init.io, program, "before");
        defer eval_before.deinit(allocator);
        if (!eval_before.success) {
            const report = MaterializeEvalFailureReport{
                .eval_program = program,
                .eval_before = eval_before,
                .blocked_export = true,
            };
            if (report_path) |path| try writeJsonReport(allocator, path, report);
            try printJson(init, report);
            return error.EvalBeforeFailed;
        }

        var summary = try finetune.materializeMergedModel(allocator, base_model_dir, adapter_model_dir, task, out_dir);
        defer finetune.freeMaterializeSummary(allocator, &summary);

        var eval_after = try peft.runEvalCapture(allocator, init.io, program, "after");
        defer eval_after.deinit(allocator);

        const report = MaterializeEvalReport{
            .eval_program = program,
            .eval_before = eval_before,
            .materialize = summary,
            .eval_after = eval_after,
            .export_succeeded = true,
        };
        if (report_path) |path| try writeJsonReport(allocator, path, report);
        try printJson(init, report);
        if (!eval_after.success) return error.EvalAfterFailed;
        return;
    }

    var summary = try finetune.materializeMergedModel(allocator, base_model_dir, adapter_model_dir, task, out_dir);
    defer finetune.freeMaterializeSummary(allocator, &summary);

    if (report_path) |path| try writeJsonReport(allocator, path, summary);

    try printJson(init, summary);
}

const MaterializeEvalReport = struct {
    eval_program: []const u8,
    eval_before: peft.EvalRun,
    materialize: finetune.MaterializeSummary,
    eval_after: peft.EvalRun,
    export_succeeded: bool,
};

const MaterializeEvalFailureReport = struct {
    eval_program: []const u8,
    eval_before: peft.EvalRun,
    blocked_export: bool,
};

fn writeJsonReport(allocator: std.mem.Allocator, path: []const u8, value: anytype) !void {
    const io = compat.io();
    var file = try compat.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    const rendered = try std.json.Stringify.valueAlloc(allocator, value, .{ .whitespace = .indent_2 });
    defer allocator.free(rendered);
    try file.writeStreamingAll(io, rendered);
    try file.writeStreamingAll(io, "\n");
}

fn printJson(init: std.process.Init, value: anytype) !void {
    const stdout = std.Io.File.stdout();
    var buf: [4096]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(value, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: materialize-layoutlmv3-checkpoint <base_model_dir> <adapter_model_dir> <sequence|token> <out_dir> [report_path]
        \\       materialize-layoutlmv3-checkpoint [--eval <program>] <base_model_dir> <adapter_model_dir> <sequence|token> <out_dir> [report_path]
        \\example: materialize-layoutlmv3-checkpoint /tmp/layoutlmv3-base /tmp/layoutlmv3-lora sequence /tmp/layoutlmv3-materialized /tmp/materialize_report.json
        \\
        \\When --eval is provided, the program is run before and after export.
        \\
    , .{});
    return error.InvalidArguments;
}
