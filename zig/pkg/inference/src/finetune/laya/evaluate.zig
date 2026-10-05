// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Serving-path evaluation of a Laya checkpoint on native training records
//! (models/laya/LAYA.md, "Training methodology"). Decisions come from the
//! same pipeline as extraction, packed or unpacked by the model's config, and
//! include its calibration temperatures.
const std = @import("std");
const data = @import("data.zig");
const model = @import("../../models/laya.zig");
const pipeline = @import("../../pipelines/laya.zig");
const factory = @import("../../architectures/session_factory.zig");
const hf = @import("inference_hf_tokenizer");
const files = @import("../../util/c_file.zig");

pub const Metrics = struct {
    decisions: usize = 0,
    accuracy: f64 = 0,
    soft_ce: f64 = 0,
    /// Expected calibration error over 15 equal-width confidence bins, where
    /// confidence is the top probability and a decision is correct when its
    /// top label is the target's top label.
    ece: f64 = 0,
    ordinal_mae: ?f64 = null,
};

pub const Report = struct {
    format: []const u8 = "antfly-laya-eval/v1",
    model_dir: []const u8,
    records_file: []const u8,
    records_sha256: []const u8,
    packing: model.PackingMode,
    backend: []const u8,
    overall: Metrics,
    choice: ?Metrics,
    score: ?Metrics,
    noul: ?Metrics,
    prompt_tokens: usize,
    seconds: f64,
    /// Set only when `Options.top_k_recall` is given: the fraction of
    /// `choice` decisions whose gold label is among the `top_k` highest
    /// probabilities. Two-stage choice (LAYA.md, roadmap 2b) can only pick
    /// the right answer in stage 2 when the shortlist contains it, so this
    /// bounds what stage 2 can achieve on this model's distribution (run it
    /// against a model with `two_stage` disabled to measure stage 1 alone).
    top_k_recall: ?f64 = null,
};

const Scored = struct { kind: model.QuestionType, probabilities: []const f32, target: []const f32 };

/// Fraction of `choice` items whose gold label (the target's argmax) is
/// among the `k` highest probabilities. 0 (not 1, to fail loud) if there are
/// no `choice` items.
fn topKRecall(items: []const Scored, k: usize) f64 {
    var hits: f64 = 0;
    var total: f64 = 0;
    for (items) |item| {
        if (item.kind != .choice) continue;
        total += 1;
        var gold: usize = 0;
        for (item.target, 0..) |t, i| if (t > item.target[gold]) {
            gold = i;
        };
        var rank: usize = 0;
        for (item.probabilities, 0..) |p, i| {
            if (i != gold and p > item.probabilities[gold]) rank += 1;
        }
        if (rank < k) hits += 1;
    }
    return if (total > 0) hits / total else 0;
}

pub fn metrics(items: []const Scored) Metrics {
    var out = Metrics{ .decisions = items.len };
    if (items.len == 0) return out;
    var bins: [15]struct { count: f64 = 0, confidence: f64 = 0, correct: f64 = 0 } = @splat(.{});
    var ordinals: usize = 0;
    var mae: f64 = 0;
    for (items) |item| {
        var winner: usize = 0;
        var gold: usize = 0;
        var expected: f64 = 0;
        var target_expected: f64 = 0;
        for (item.probabilities, item.target, 0..) |p, t, k| {
            if (p > item.probabilities[winner]) winner = k;
            if (t > item.target[gold]) gold = k;
            out.soft_ce -= t * @log(@max(@as(f64, p), 1e-12));
            expected += @as(f64, @floatFromInt(k)) * p;
            target_expected += @as(f64, @floatFromInt(k)) * t;
        }
        const correct: f64 = @floatFromInt(@intFromBool(winner == gold));
        out.accuracy += correct;
        const confidence: f64 = item.probabilities[winner];
        const bin = @min(14, @as(usize, @intFromFloat(confidence * 15)));
        bins[bin].count += 1;
        bins[bin].confidence += confidence;
        bins[bin].correct += correct;
        if (item.kind == .score) {
            ordinals += 1;
            mae += @abs(expected - target_expected);
        }
    }
    const n: f64 = @floatFromInt(items.len);
    for (bins) |bin| if (bin.count > 0) {
        out.ece += bin.count / n * @abs(bin.confidence / bin.count - bin.correct / bin.count);
    };
    out.accuracy /= n;
    out.soft_ce /= n;
    if (ordinals > 0) out.ordinal_mae = mae / @as(f64, @floatFromInt(ordinals));
    return out;
}

pub const Options = struct {
    model_dir: []const u8,
    records_file: []const u8,
    backend: enum { native, metal } = .metal,
    /// Tasks per pipeline call; packed models group each case into one row.
    chunk: usize = 64,
    /// When set, also report the fraction of `choice` decisions whose gold
    /// label is among this many highest probabilities (`Report.top_k_recall`).
    top_k_recall: ?usize = null,
    /// When set, also write one JSON line per decision (`Prediction`) to this
    /// new file, for paired comparisons between models.
    predictions_file: ?[]const u8 = null,
    /// Cut overlong states to fit instead of failing (`Config.truncate_state`).
    truncate_state: bool = false,
};

/// One scored decision in `Options.predictions_file`.
pub const Prediction = struct { id: []const u8, kind: model.QuestionType, labels: []const []const u8, probabilities: []const f32, target: []const f32 };

pub fn run(gpa: std.mem.Allocator, io: std.Io, options: Options) !Report {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var session = switch (options.backend) {
        .native => try factory.createNativeSession(gpa, options.model_dir),
        .metal => try factory.createMetalSession(gpa, options.model_dir),
    };
    defer session.close();
    var cfg = factory.getLayaConfig(session) orelse return error.InvalidLayaConfig;
    cfg.truncate_state = options.truncate_state;
    const tokenizer = try hf.HfTokenizer.loadFromBytes(gpa, try files.readFileFromDir(a, options.model_dir, "tokenizer.json"));
    const tok = tokenizer.tokenizer();
    defer tok.deinitTokenizer();
    const bytes = try files.readFileMax(a, options.records_file, data.max_file_bytes);
    var tasks: std.ArrayListUnmanaged(pipeline.Task) = .empty;
    var targets: std.ArrayListUnmanaged([]const f32) = .empty;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0) continue;
        const record = (try std.json.parseFromSlice(data.Record, a, line, .{ .allocate = .alloc_always })).value;
        try data.validate(record);
        const descriptions = record.descriptions orelse blk: {
            const empty = try a.alloc([]const u8, record.labels.len);
            @memset(empty, "");
            break :blk empty;
        };
        try tasks.append(a, .{ .text = record.text, .question = .{ .name = record.id, .kind = record.kind, .instruction = record.instruction, .labels = record.labels, .descriptions = descriptions } });
        try targets.append(a, record.target);
    }
    if (tasks.items.len == 0) return error.EmptyLayaDataset;
    const scored = try a.alloc(Scored, tasks.items.len);
    var prompt_tokens: usize = 0;
    const began = @import("antfly_platform").time.monotonicNs();
    var start: usize = 0;
    while (start < tasks.items.len) {
        // Keep a case's questions in one call so packed models share its state.
        var end = @min(start + options.chunk, tasks.items.len);
        while (end < tasks.items.len and std.mem.eql(u8, tasks.items[end].text, tasks.items[end - 1].text)) end += 1;
        var request = std.heap.ArenaAllocator.init(gpa);
        defer request.deinit();
        const result = try pipeline.executeWithScratch(request.allocator(), gpa, session, tok, cfg, tasks.items[start..end], null, null);
        prompt_tokens += result.prompt_tokens;
        for (result.decisions, scored[start..end], tasks.items[start..end], targets.items[start..end]) |decision, *dst, task, target| {
            dst.* = .{ .kind = task.question.kind, .probabilities = try a.dupe(f32, decision.probabilities), .target = target };
        }
        start = end;
    }
    const seconds = @as(f64, @floatFromInt(@import("antfly_platform").time.monotonicNs() - began)) / 1e9;
    var by_kind: [3]?Metrics = .{ null, null, null };
    for (0..3) |kind| {
        var subset: std.ArrayListUnmanaged(Scored) = .empty;
        for (scored) |item| if (@intFromEnum(item.kind) == kind) try subset.append(a, item);
        if (subset.items.len > 0) by_kind[kind] = metrics(subset.items);
    }
    if (options.predictions_file) |output| {
        const file = try std.Io.Dir.cwd().createFile(io, output, .{ .exclusive = true });
        defer file.close(io);
        var buffer: [16384]u8 = undefined;
        var w = file.writer(io, &buffer);
        for (tasks.items, scored) |task, item| {
            try std.json.Stringify.value(Prediction{ .id = task.question.name, .kind = item.kind, .labels = task.question.labels, .probabilities = item.probabilities, .target = item.target }, .{}, &w.interface);
            try w.interface.writeByte('\n');
        }
        try w.interface.flush();
    }
    return .{
        .model_dir = try gpa.dupe(u8, options.model_dir),
        .records_file = try gpa.dupe(u8, options.records_file),
        .records_sha256 = try gpa.dupe(u8, &std.fmt.bytesToHex(data.digest(bytes), .lower)),
        .packing = cfg.packing.mode,
        .backend = @tagName(session.backend()),
        .overall = metrics(scored),
        .choice = by_kind[0],
        .score = by_kind[1],
        .noul = by_kind[2],
        .prompt_tokens = prompt_tokens,
        .seconds = seconds,
        .top_k_recall = if (options.top_k_recall) |k| topKRecall(scored, k) else null,
    };
}

pub fn deinitReport(gpa: std.mem.Allocator, report: Report) void {
    gpa.free(report.model_dir);
    gpa.free(report.records_file);
    gpa.free(report.records_sha256);
}

test "laya evaluation metrics: accuracy, soft CE, ECE, and ordinal MAE" {
    const items = [_]Scored{
        .{ .kind = .choice, .probabilities = &.{ 0.9, 0.1 }, .target = &.{ 1, 0 } },
        .{ .kind = .choice, .probabilities = &.{ 0.6, 0.4 }, .target = &.{ 0, 1 } },
        .{ .kind = .score, .probabilities = &.{ 0, 0.5, 0.5 }, .target = &.{ 0, 0, 1 } },
    };
    const m = metrics(&items);
    try std.testing.expectEqual(@as(usize, 3), m.decisions);
    // The ordinal tie resolves to the first maximum (label 1), so it is wrong.
    try std.testing.expectApproxEqAbs(@as(f64, 1.0 / 3.0), m.accuracy, 1e-9);
    try std.testing.expectApproxEqAbs((-@log(0.9) - @log(0.4) - @log(0.5)) / 3.0, m.soft_ce, 1e-6);
    // Bins: 0.9 (correct), 0.6 (wrong), 0.5 (wrong).
    try std.testing.expectApproxEqAbs((0.1 + 0.6 + 0.5) / 3.0, m.ece, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), m.ordinal_mae.?, 1e-9);
}

test "laya top-k recall counts only choice items and ignores gold's own rank ties" {
    const items = [_]Scored{
        // Gold (index 2) is the top pick: within any k >= 1.
        .{ .kind = .choice, .probabilities = &.{ 0.1, 0.2, 0.7 }, .target = &.{ 0, 0, 1 } },
        // Gold (index 0) is third: needs k >= 3.
        .{ .kind = .choice, .probabilities = &.{ 0.1, 0.5, 0.4 }, .target = &.{ 1, 0, 0 } },
        // Non-choice kinds are excluded from the denominator.
        .{ .kind = .score, .probabilities = &.{ 0.9, 0.1 }, .target = &.{ 1, 0 } },
    };
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), topKRecall(&items, 1), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), topKRecall(&items, 2), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 1), topKRecall(&items, 3), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, 0), topKRecall(items[2..2], 1), 1e-9);
}
