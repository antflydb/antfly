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
const inference = @import("inference_internal");
const gliner2_boundary = inference.finetune.gliner2_boundary;
const reranker = inference.finetune.reranker;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const train_summary_path = args.next() orelse return usageError();
    const eval_summary_path = args.next() orelse return usageError();
    const out_dir = args.next() orelse return usageError();
    const backend_arg = args.next() orelse "native";
    const learning_rate_arg = args.next() orelse "0.001";
    const epochs_arg = args.next() orelse "1";

    const backend = try parseBackend(backend_arg);
    const learning_rate = try std.fmt.parseFloat(f32, learning_rate_arg);
    const epochs = try std.fmt.parseUnsigned(usize, epochs_arg, 10);

    var train_summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, train_summary_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &train_summary);
    var eval_summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, eval_summary_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &eval_summary);

    try std.Io.Dir.cwd().createDirPath(init.io, out_dir);

    var summary = try gliner2_boundary.trainEvalBoundaryHead(
        allocator,
        model_dir,
        &train_summary,
        &eval_summary,
        backend,
        out_dir,
        .{ .learning_rate = learning_rate, .epochs = epochs },
    );
    defer gliner2_boundary.freeTrainEvalSummary(allocator, &summary);

    const report_path = try std.fs.path.join(allocator, &.{ out_dir, "train_eval_boundary_head_report.json" });
    defer allocator.free(report_path);
    const rendered = try std.json.Stringify.valueAlloc(allocator, summary, .{ .whitespace = .indent_2 });
    defer allocator.free(rendered);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = report_path, .data = rendered });

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(summary, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) !reranker.BackendChoice {
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return error.InvalidBackend;
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: train-eval-gliner2-top-layer-boundary-head <model_dir> <train_boundary_summary.json> <eval_boundary_summary.json> <out_dir> [backend] [learning_rate] [epochs]
        \\example: train-eval-gliner2-top-layer-boundary-head /tmp/gliner2_base /tmp/gliner2_train_boundary.json /tmp/gliner2_eval_boundary.json /tmp/gliner2_boundary_head native 0.001 2
        \\
    , .{});
    return error.InvalidArguments;
}
