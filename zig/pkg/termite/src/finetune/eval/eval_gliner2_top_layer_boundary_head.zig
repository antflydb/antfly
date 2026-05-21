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
const reranker = @import("../reranker.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    const model_dir = args.next() orelse return usageError();
    const summary_path = args.next() orelse return usageError();
    const backend_arg = args.next() orelse "blas";
    const boundary_head_input = args.next();
    const backend = try parseBackend(backend_arg);

    var summary = try gliner2_boundary.loadCachedBoundarySummary(allocator, summary_path);
    defer gliner2_boundary.freeCachedBoundarySummary(allocator, &summary);

    var maybe_head: ?gliner2_boundary.BoundaryHead = null;
    defer if (maybe_head) |*head| gliner2_boundary.freeBoundaryHead(allocator, head);
    if (boundary_head_input) |input| {
        const head_path = try gliner2_boundary.resolveBoundaryHeadPath(allocator, input);
        defer allocator.free(head_path);
        maybe_head = try gliner2_boundary.loadBoundaryHead(allocator, head_path);
    }

    const eval = try gliner2_boundary.evaluateCachedBoundarySummaryWithHead(
        allocator,
        model_dir,
        &summary,
        backend,
        if (maybe_head) |*head| head else null,
    );
    defer {
        allocator.free(eval.artifact_family_version);
        allocator.free(eval.model_dir);
        allocator.free(eval.requested_backend);
    }

    const stdout = std.Io.File.stdout();
    var buf: [8192]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try std.json.Stringify.value(eval, .{ .whitespace = .indent_2 }, &writer.interface);
    try writer.interface.writeByte('\n');
    try writer.interface.flush();
}

fn parseBackend(value: []const u8) !reranker.BackendChoice {
    return reranker.parseBackendChoice(value) orelse error.InvalidBackend;
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: eval-gliner2-top-layer-boundary-head <model_dir> <boundary_summary.json> [backend] [boundary_head.json|boundary_head_dir]
        \\example: eval-gliner2-top-layer-boundary-head /tmp/gliner2_base /tmp/gliner2_boundary.json blas /tmp/gliner2_boundary_head
        \\
    , .{});
    return error.InvalidArguments;
}
