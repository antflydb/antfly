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
const platform = @import("antfly_platform");
const compat = inference.io.compat;
const c_file = inference.util.c_file;

const Row = struct {
    kind: []const u8,
    path: []const u8,
    rank: ?usize = null,
    shared_block_size: ?usize = null,
    teacher_temperature: ?f64 = null,
    before_average_loss: ?f64 = null,
    after_average_loss: ?f64 = null,
    last_epoch_average_loss: ?f64 = null,
    teacher_supervised_tokens: ?f64 = null,
    trained_adapter_bytes: ?u64 = null,
    compressed_base_checkpoint_bytes: ?u64 = null,
    compressed_base_ratio: ?f64 = null,
    train_supervised_tokens_per_second: ?f64 = null,
};

const Comparison = struct {
    rows: []Row = &.{},
};

const Criteria = struct {
    max_loss_ratio: f64 = 1.10,
    max_adapter_ratio: f64 = 1.25,
    max_compressed_base_ratio: f64 = 0.75,
    min_teacher_tokens: u64 = 1,
    min_throughput: f64 = 0,
};

const Evaluated = struct {
    status: []const u8,
    reasons: []const []const u8,
    score: ?f64,
    loss_ratio_vs_baseline: ?f64,
    adapter_size_ratio_vs_baseline: ?f64,
    row: Row,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, allocator);
    defer args.deinit();
    _ = args.next();

    var comparison_path: []const u8 = args.next() orelse return usageError();
    var out_dir: []const u8 = args.next() orelse return usageError();
    var criteria = Criteria{};

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--max-loss-ratio")) {
            criteria.max_loss_ratio = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--max-adapter-ratio")) {
            criteria.max_adapter_ratio = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--max-compressed-base-ratio")) {
            criteria.max_compressed_base_ratio = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else if (std.mem.eql(u8, arg, "--min-teacher-tokens")) {
            criteria.min_teacher_tokens = try std.fmt.parseUnsigned(u64, args.next() orelse return usageError(), 10);
        } else if (std.mem.eql(u8, arg, "--min-throughput")) {
            criteria.min_throughput = try std.fmt.parseFloat(f64, args.next() orelse return usageError());
        } else {
            return usageError();
        }
    }

    const invocation_cwd = invocationCwd();
    comparison_path = try resolveCliPath(allocator, invocation_cwd, comparison_path);
    defer allocator.free(comparison_path);
    out_dir = try resolveCliPath(allocator, invocation_cwd, out_dir);
    defer allocator.free(out_dir);

    try compat.cwd().createDirPath(compat.io(), out_dir);
    const decision_json = try std.fs.path.join(allocator, &.{ out_dir, "recursive_lora_sweep_decision.json" });
    defer allocator.free(decision_json);
    const decision_md = try std.fs.path.join(allocator, &.{ out_dir, "recursive_lora_sweep_decision.md" });
    defer allocator.free(decision_md);

    try analyze(allocator, comparison_path, decision_json, decision_md, criteria);

    const stdout = std.Io.File.stdout();
    var buf: [2048]u8 = undefined;
    var writer = stdout.writer(init.io, &buf);
    try writer.interface.print(
        \\recursive_sweep_decision_complete: true
        \\comparison: {s}
        \\decision_json: {s}
        \\decision_md: {s}
        \\max_loss_ratio: {d}
        \\max_adapter_ratio: {d}
        \\max_compressed_base_ratio: {d}
        \\min_teacher_tokens: {d}
        \\min_throughput: {d}
        \\
    , .{
        comparison_path,
        decision_json,
        decision_md,
        criteria.max_loss_ratio,
        criteria.max_adapter_ratio,
        criteria.max_compressed_base_ratio,
        criteria.min_teacher_tokens,
        criteria.min_throughput,
    });
    try writer.interface.flush();
}

fn analyze(
    allocator: std.mem.Allocator,
    comparison_path: []const u8,
    decision_json: []const u8,
    decision_md: []const u8,
    criteria: Criteria,
) !void {
    const bytes = try c_file.readFile(allocator, comparison_path);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(Comparison, allocator, bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    var evaluated = std.ArrayListUnmanaged(Evaluated).empty;
    defer {
        for (evaluated.items) |item| allocator.free(item.reasons);
        evaluated.deinit(allocator);
    }

    for (parsed.value.rows) |row| {
        if (!std.mem.eql(u8, row.kind, "recursive")) continue;
        const baseline = findBaseline(parsed.value.rows, row.rank);
        try evaluated.append(allocator, try evaluateRow(allocator, row, baseline, criteria));
    }

    var recommendation: ?Evaluated = null;
    for (evaluated.items) |item| {
        if (!std.mem.eql(u8, item.status, "pass")) continue;
        if (recommendation == null or betterRecommendation(item, recommendation.?)) recommendation = item;
    }
    const status: []const u8 = if (recommendation != null) "promote" else "hold";

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    try std.json.Stringify.value(.{
        .task = "gemma4_recursive_lora_sweep_decision",
        .comparison_path = comparison_path,
        .criteria = criteria,
        .status = status,
        .recommendation = recommendation,
        .evaluated = evaluated.items,
    }, .{ .whitespace = .indent_2 }, &out.writer);
    try out.writer.writeByte('\n');
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = decision_json, .data = out.written() });

    try writeMarkdown(allocator, decision_md, criteria, status, recommendation, evaluated.items);
}

fn evaluateRow(allocator: std.mem.Allocator, row: Row, baseline: ?Row, criteria: Criteria) !Evaluated {
    var reasons = std.ArrayListUnmanaged([]const u8).empty;
    errdefer reasons.deinit(allocator);

    var loss_ratio: ?f64 = null;
    var adapter_ratio: ?f64 = null;
    if (baseline) |base| {
        const base_loss = base.after_average_loss orelse 0;
        const rec_loss = row.after_average_loss orelse 0;
        const base_size = @as(f64, @floatFromInt(base.trained_adapter_bytes orelse 0));
        const rec_size = @as(f64, @floatFromInt(row.trained_adapter_bytes orelse 0));
        if (base_loss > 0) loss_ratio = rec_loss / base_loss;
        if (base_size > 0) adapter_ratio = rec_size / base_size;
        if (loss_ratio == null or loss_ratio.? > criteria.max_loss_ratio) try reasons.append(allocator, "loss ratio above threshold");
        if (adapter_ratio == null or adapter_ratio.? > criteria.max_adapter_ratio) try reasons.append(allocator, "adapter size ratio above threshold");
    } else {
        try reasons.append(allocator, "missing same-rank baseline");
    }

    const compressed_base_ratio = row.compressed_base_ratio orelse 0;
    if (compressed_base_ratio <= 0 or compressed_base_ratio > criteria.max_compressed_base_ratio) try reasons.append(allocator, "compressed base ratio above threshold");
    if (@as(u64, @intFromFloat(row.teacher_supervised_tokens orelse 0)) < criteria.min_teacher_tokens) try reasons.append(allocator, "teacher coverage below threshold");
    const throughput = row.train_supervised_tokens_per_second orelse 0;
    if (criteria.min_throughput > 0 and throughput < criteria.min_throughput) try reasons.append(allocator, "throughput below threshold");

    return .{
        .status = if (reasons.items.len == 0) "pass" else "fail",
        .reasons = try reasons.toOwnedSlice(allocator),
        .score = if (loss_ratio != null and adapter_ratio != null) loss_ratio.? + 0.05 * adapter_ratio.? else null,
        .loss_ratio_vs_baseline = loss_ratio,
        .adapter_size_ratio_vs_baseline = adapter_ratio,
        .row = row,
    };
}

fn writeMarkdown(
    allocator: std.mem.Allocator,
    decision_md: []const u8,
    criteria: Criteria,
    status: []const u8,
    recommendation: ?Evaluated,
    evaluated: []const Evaluated,
) !void {
    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("# Gemma4 Recursive LoRA Sweep Decision\n\n");
    try writer.print("Status: **{s}**\n\n", .{status});
    try writer.writeAll("## Criteria\n\n");
    try writer.print("- Max loss ratio: {d}\n", .{criteria.max_loss_ratio});
    try writer.print("- Max adapter ratio: {d}\n", .{criteria.max_adapter_ratio});
    try writer.print("- Max compressed base ratio: {d}\n", .{criteria.max_compressed_base_ratio});
    try writer.print("- Min teacher tokens: {d}\n", .{criteria.min_teacher_tokens});
    try writer.print("- Min throughput: {d}\n\n", .{criteria.min_throughput});

    try writer.writeAll("## Recommendation\n\n");
    if (recommendation) |item| {
        const row = item.row;
        try writer.print("- Path: `{s}`\n", .{row.path});
        try writeNamedOptional(writer, "Rank", row.rank);
        try writeNamedOptional(writer, "Shared block size", row.shared_block_size);
        try writeNamedOptional(writer, "Teacher temperature", row.teacher_temperature);
        try writeNamedOptional(writer, "After loss", row.after_average_loss);
        try writeNamedOptional(writer, "Loss ratio", item.loss_ratio_vs_baseline);
        try writeNamedOptional(writer, "Adapter size ratio", item.adapter_size_ratio_vs_baseline);
        try writeNamedOptional(writer, "Compressed base ratio", row.compressed_base_ratio);
        try writer.writeByte('\n');
    } else {
        try writer.writeAll("No recursive variant passed the criteria.\n\n");
    }

    try writer.writeAll("## Rows\n\n");
    try writer.writeAll("| Status | Rank | Share | Temp | Loss Ratio | Adapter Ratio | Base Ratio | Reasons |\n");
    try writer.writeAll("|---|---:|---:|---:|---:|---:|---:|---|\n");
    for (evaluated) |item| {
        const row = item.row;
        try writer.print("| {s} | ", .{item.status});
        try writeOptional(writer, row.rank);
        try writer.writeAll(" | ");
        try writeOptional(writer, row.shared_block_size);
        try writer.writeAll(" | ");
        try writeOptional(writer, row.teacher_temperature);
        try writer.writeAll(" | ");
        try writeOptional(writer, item.loss_ratio_vs_baseline);
        try writer.writeAll(" | ");
        try writeOptional(writer, item.adapter_size_ratio_vs_baseline);
        try writer.writeAll(" | ");
        try writeOptional(writer, row.compressed_base_ratio);
        try writer.writeAll(" | ");
        for (item.reasons, 0..) |reason, idx| {
            if (idx != 0) try writer.writeAll(", ");
            try writer.writeAll(reason);
        }
        try writer.writeAll(" |\n");
    }
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = decision_md, .data = out.written() });
}

fn writeNamedOptional(writer: anytype, name: []const u8, value: anytype) !void {
    try writer.print("- {s}: ", .{name});
    try writeOptional(writer, value);
    try writer.writeByte('\n');
}

fn writeOptional(writer: anytype, value: anytype) !void {
    if (value) |unwrapped| {
        try writer.print("{any}", .{unwrapped});
    } else {
        try writer.writeAll("null");
    }
}

fn findBaseline(rows: []const Row, rank: ?usize) ?Row {
    const wanted = rank orelse return null;
    for (rows) |row| {
        if (std.mem.eql(u8, row.kind, "baseline") and row.rank != null and row.rank.? == wanted) return row;
    }
    return null;
}

fn betterRecommendation(candidate: Evaluated, current: Evaluated) bool {
    const candidate_score = candidate.score orelse 1e9;
    const current_score = current.score orelse 1e9;
    if (candidate_score != current_score) return candidate_score < current_score;
    return (candidate.row.trained_adapter_bytes orelse 0) < (current.row.trained_adapter_bytes orelse 0);
}

fn invocationCwd() ?[]const u8 {
    return platform.env.getenv("ANTFLY_WORKFLOW_CWD");
}

fn resolveCliPath(allocator: std.mem.Allocator, invocation_cwd: ?[]const u8, path: []const u8) ![]const u8 {
    if (path.len == 0 or std.fs.path.isAbsolute(path)) return allocator.dupe(u8, path);
    const cwd = invocation_cwd orelse return allocator.dupe(u8, path);
    return std.fs.path.join(allocator, &.{ cwd, path });
}

fn usageError() error{InvalidArguments} {
    std.debug.print(
        \\usage: analyze-gemma4-recursive-lora-sweep <comparison_json> <out_dir> [options]
        \\
        \\Options:
        \\  --max-loss-ratio FLOAT
        \\  --max-adapter-ratio FLOAT
        \\  --max-compressed-base-ratio FLOAT
        \\  --min-teacher-tokens N
        \\  --min-throughput FLOAT
        \\
    , .{});
    return error.InvalidArguments;
}
