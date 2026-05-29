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
const gliner2_boundary = @import("../gliner2_boundary.zig");
const text_encoder_boundary = @import("../text_encoder_boundary.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usage();
    const summary_path = args.next() orelse return usage();
    const backend = parseBackend(args.next() orelse "native") orelse return error.InvalidBackend;
    const task_head_input = args.next();

    var summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, summary_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &summary);

    var maybe_head: ?gliner2_boundary.BoundaryTaskHead = null;
    defer if (maybe_head) |*head| gliner2_boundary.freeBoundaryTaskHead(allocator, head);
    if (task_head_input) |input| {
        const head_path = try gliner2_boundary.resolveBoundaryTaskHeadPath(allocator, input);
        defer allocator.free(head_path);
        maybe_head = try gliner2_boundary.loadBoundaryTaskHead(allocator, head_path);
    }

    var eval = try gliner2_boundary.evaluateCachedBoundarySummaryWithTaskHead(
        allocator,
        model_dir,
        &summary,
        backend,
        if (maybe_head) |*head| head else null,
    );
    defer gliner2_boundary.freeEvalSummary(allocator, &eval);

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(eval, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) ?text_encoder_boundary.BackendChoice {
    if (std.mem.eql(u8, value, "blas")) return .native;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    return null;
}

fn usage() error{InvalidArguments}!void {
    std.debug.print(
        \\usage: eval-gliner2-top-layer-boundary-task-head <model_dir> <boundary_summary.json> [backend] [boundary_task_head.json|boundary_task_head_dir]
        \\example: eval-gliner2-top-layer-boundary-task-head /tmp/gliner2_base /tmp/gliner2_boundary.json native /tmp/gliner2_boundary_task_head
        \\
    , .{});
    return error.InvalidArguments;
}
