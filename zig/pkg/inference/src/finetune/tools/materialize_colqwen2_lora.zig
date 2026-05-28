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
const finetune = termite.finetune.colqwen2;
const peft = termite.finetune.peft;

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

    if (positional.items.len != 3) return usageError();
    const base_model_dir = positional.items[0];
    const adapter_model_dir = positional.items[1];
    const out_dir = positional.items[2];

    if (eval_program) |program| {
        var eval_before = try peft.runEvalCapture(allocator, init.io, program, "before");
        defer eval_before.deinit(allocator);
        if (!eval_before.success) {
            const report = MaterializeEvalFailureReport{
                .eval_program = program,
                .eval_before = eval_before,
                .blocked_export = true,
            };
            try printJson(init, report);
            return error.EvalBeforeFailed;
        }

        var summary = try finetune.materializeMergedModel(allocator, base_model_dir, adapter_model_dir, out_dir);
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
        try printJson(init, report);
        if (!eval_after.success) return error.EvalAfterFailed;
        return;
    }

    var summary = try finetune.materializeMergedModel(allocator, base_model_dir, adapter_model_dir, out_dir);
    defer finetune.freeMaterializeSummary(allocator, &summary);

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
        \\usage: materialize-colqwen2-lora <base_model_dir> <adapter_model_dir> <out_dir>
        \\       materialize-colqwen2-lora [--eval <program>] <base_model_dir> <adapter_model_dir> <out_dir>
        \\example: materialize-colqwen2-lora /tmp/colqwen2-base /tmp/colqwen2-lora /tmp/colqwen2-merged
        \\
        \\When --eval is provided, the program is run before and after export.
        \\
    , .{});
    return error.InvalidArguments;
}
