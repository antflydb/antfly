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
const gliner2_boundary = termite.finetune.gliner2_boundary;
const text_encoder_boundary = termite.finetune.text_encoder_boundary;
const run_contract = termite.run.contract;
const artifact_writer = termite.run.artifact_writer;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usage();
    const train_summary_path = args.next() orelse return usage();
    const eval_summary_path = args.next() orelse return usage();
    const out_dir = args.next() orelse return usage();
    const backend = parseBackend(args.next() orelse "native") orelse return error.InvalidBackend;
    const learning_rate = try std.fmt.parseFloat(f32, args.next() orelse "0.001");
    const epochs = try std.fmt.parseUnsigned(usize, args.next() orelse "1", 10);

    var train_summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, train_summary_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &train_summary);
    var eval_summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, eval_summary_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &eval_summary);

    var summary = try gliner2_boundary.trainEvalBoundaryTaskHead(
        allocator,
        model_dir,
        &train_summary,
        &eval_summary,
        backend,
        out_dir,
        .{ .learning_rate = learning_rate, .epochs = epochs },
    );
    defer gliner2_boundary.freeTaskHeadTrainEvalSummary(allocator, &summary);

    const training_config_path = try std.fs.path.join(allocator, &.{ out_dir, "training_config.json" });
    defer allocator.free(training_config_path);
    try artifact_writer.writeJsonFile(allocator, training_config_path, .{
        .contract_version = run_contract.training_config_version,
        .artifact_family_version = summary.artifact_family_version,
        .task = "gliner2_boundary_task_head_train_eval",
        .inputs = .{
            .model_dir = model_dir,
            .train_summary_path = train_summary_path,
            .eval_summary_path = eval_summary_path,
        },
        .training = .{
            .backend = @tagName(backend),
            .learning_rate = learning_rate,
            .epochs = epochs,
        },
    });
    const training_report_path = try std.fs.path.join(allocator, &.{ out_dir, "training_report.json" });
    defer allocator.free(training_report_path);
    try artifact_writer.writeJsonFile(allocator, training_report_path, .{
        .contract_version = run_contract.training_report_version,
        .artifact_family_version = summary.artifact_family_version,
        .task = "gliner2_boundary_task_head_train_eval",
        .backend_policy = .{
            .selected = @tagName(backend),
            .preferred = @tagName(backend),
        },
        .report = summary,
    });

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) ?text_encoder_boundary.BackendChoice {
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return null;
}

fn usage() error{InvalidArguments}!void {
    std.debug.print(
        \\usage: train-eval-gliner2-top-layer-boundary-task-head <model_dir> <train_boundary_summary.json> <eval_boundary_summary.json> <out_dir> [backend] [learning_rate] [epochs]
        \\example: train-eval-gliner2-top-layer-boundary-task-head /tmp/gliner2_base /tmp/gliner2_train_boundary.json /tmp/gliner2_eval_boundary.json /tmp/gliner2_boundary_task_head native 0.001 2
        \\
    , .{});
    return error.InvalidArguments;
}
