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
const compat = @import("../../io/compat.zig");
const finetune = @import("../layoutlmv3.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const base_model_dir = args.next() orelse return usageError();
    const adapter_model_dir = args.next() orelse return usageError();
    const out_dir = args.next() orelse return usageError();
    const learning_rate_arg = args.next() orelse "0.001";
    const input_rows_arg = args.next() orelse "4";
    const layer_name = args.next();

    const learning_rate = try std.fmt.parseFloat(f32, learning_rate_arg);
    const input_rows = try std.fmt.parseUnsigned(usize, input_rows_arg, 10);

    var bundle = try finetune.loadLoRABundle(allocator, base_model_dir, adapter_model_dir);
    defer bundle.deinit();
    var step = try finetune.trainLoRABundleOneStep(allocator, &bundle, .{
        .input_rows = input_rows,
        .learning_rate = learning_rate,
        .layer_name = layer_name,
    });
    defer finetune.freeLoRAOneStepSummary(allocator, &step);

    try finetune.saveLoRABundle(&bundle, out_dir);

    const report_path = try std.fs.path.join(allocator, &.{ out_dir, "one_step_report.json" });
    defer allocator.free(report_path);
    const rendered = try std.json.Stringify.valueAlloc(allocator, .{
        .artifact_family_version = finetune.artifact_family_version,
        .summary = step,
        .saved_adapter_checkpoint = finetune.adapter_checkpoint_file_name,
    }, .{ .whitespace = .indent_2 });
    defer allocator.free(rendered);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = report_path, .data = rendered });

    const stdout = std.Io.File.stdout();
    var buf: [2048]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(step, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: train-layoutlmv3-lora-one-step <base_model_dir> <adapter_model_dir> <out_dir> [learning_rate] [input_rows] [layer_name|@layoutlmv3_token_top1|@layoutlmv3_token_top3|@layoutlmv3_sequence_top3]
        \\example: train-layoutlmv3-lora-one-step /tmp/layoutlmv3_base /tmp/layoutlmv3_lora /tmp/layoutlmv3_step 0.001 4 @layoutlmv3_sequence_top3
        \\
    , .{});
    return error.InvalidArguments;
}
