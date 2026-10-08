// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Fitted raw cosine thresholds. No probability or confidence transformation.
const std = @import("std");
const scoring = @import("embedding_decisions.zig");
const prototypes = @import("embedding_prototypes.zig");
const file = @import("../util/c_file.zig");
const receipt = @import("../registry/managed_receipt.zig");
pub const Policy = struct { options: scoring.Options, thresholds: ?[]const f64 = null };
const V = std.json.Value;
fn field(value: V, key: []const u8) !V {
    return if (value == .object) value.object.get(key) orelse error.InvalidEmbeddingCalibration else error.InvalidEmbeddingCalibration;
}
fn string(value: V) ![]const u8 {
    return if (value == .string) value.string else error.InvalidEmbeddingCalibration;
}
fn number(value: V) !f64 {
    const x: f64 = switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        else => return error.InvalidEmbeddingCalibration,
    };
    if (!std.math.isFinite(x)) return error.InvalidEmbeddingCalibration;
    return x;
}
fn count(value: V, key: []const u8) !usize {
    const raw = try field(value, key);
    if (raw != .integer or raw.integer < 0) return error.InvalidEmbeddingCalibration;
    return std.math.cast(usize, raw.integer) orelse error.InvalidEmbeddingCalibration;
}
fn equal(value: V, expected: []const u8) !void {
    if (!std.mem.eql(u8, try string(value), expected)) return error.EmbeddingCalibrationMismatch;
}
fn lower(correct: usize, n: usize) f64 {
    if (n == 0) return 0;
    const z: f64 = 1.959963984540054;
    const total: f64 = @floatFromInt(n);
    const p = @as(f64, @floatFromInt(correct)) / total;
    return (p + z * z / (2 * total) - z * @sqrt(p * (1 - p) / total + z * z / (4 * total * total))) / (1 + z * z / total);
}

pub fn parse(a: std.mem.Allocator, raw: V, identity: []const u8, question: anytype, options: scoring.Options, mode: []const u8) !Policy {
    if (try count(raw, "version") != 1) return error.InvalidEmbeddingCalibration;
    try equal(try field(raw, "method"), "heldout-abstention-v1");
    const qualified = try field(raw, "qualified");
    if (qualified != .bool or !qualified.bool) return error.UnqualifiedEmbeddingCalibration;
    const binding = try field(raw, "binding");
    try equal(try field(binding, "model_identity"), identity);
    try equal(try field(binding, "renderer_version"), scoring.renderer_version);
    try equal(try field(binding, "task_type"), options.task_type);
    try equal(try field(binding, "mode"), mode);
    if (try count(binding, "dimensions") != options.dimensions) return error.EmbeddingCalibrationMismatch;
    const expected = try prototypes.prototypeSetHash(a, question, options, mode);
    try equal(try field(binding, "prototype_set_hash"), &expected);
    const labels = try field(binding, "labels");
    if (labels != .array or labels.array.items.len != question.labels.len) return error.EmbeddingCalibrationMismatch;
    for (labels.array.items, question.labels) |label, want| try equal(label, want);
    const metrics = try field(raw, "metrics");
    const fit = try field(metrics, "fit");
    const validation = try field(metrics, "validation");
    const holdout = try field(metrics, "holdout");
    if (try count(fit, "count") < 30 or try count(validation, "count") < 30 or try count(holdout, "count") < 100) return error.UnqualifiedEmbeddingCalibration;
    const fit_hash = try string(try field(fit, "dataset_sha256"));
    const val_hash = try string(try field(validation, "dataset_sha256"));
    const holdout_hash = try string(try field(holdout, "dataset_sha256"));
    if (fit_hash.len != 64 or val_hash.len != 64 or holdout_hash.len != 64 or std.mem.eql(u8, fit_hash, val_hash) or std.mem.eql(u8, fit_hash, holdout_hash) or std.mem.eql(u8, val_hash, holdout_hash)) return error.UnqualifiedEmbeddingCalibration;
    const targets = try field(raw, "targets");
    const precision = try number(try field(targets, "precision_lower_95"));
    const coverage = try number(try field(targets, "minimum_coverage_or_f1"));
    if (precision <= 0 or precision > 1 or coverage <= 0 or coverage > 1) return error.InvalidEmbeddingCalibration;
    const thresholds = try field(raw, "thresholds");
    if (std.mem.eql(u8, mode, "single")) {
        const selected = try count(holdout, "selected");
        const correct = try count(holdout, "correct");
        const total = try count(holdout, "count");
        if (correct > selected or selected > total or lower(correct, selected) < precision or @as(f64, @floatFromInt(selected)) / @as(f64, @floatFromInt(total)) < coverage) return error.UnqualifiedEmbeddingCalibration;
        var configured = options;
        configured.min_similarity = try number(try field(thresholds, "min_similarity"));
        configured.min_margin = try number(try field(thresholds, "min_margin"));
        if (configured.min_similarity.? < -1 or configured.min_similarity.? > 1 or configured.min_margin.? < 0 or configured.min_margin.? > 2) return error.InvalidEmbeddingCalibration;
        return .{ .options = configured };
    }
    const values = try field(thresholds, "similarity_thresholds");
    if (values != .object or values.object.count() != question.labels.len) return error.InvalidEmbeddingCalibration;
    const results = try field(holdout, "labels");
    const output = try a.alloc(f64, question.labels.len);
    errdefer a.free(output);
    for (question.labels, output) |label, *threshold| {
        threshold.* = try number(try field(values, label));
        if (threshold.* < -1 or threshold.* > 1) return error.InvalidEmbeddingCalibration;
        const result = try field(results, label);
        const tp = try count(result, "tp");
        const fp = try count(result, "fp");
        const fn_count = try count(result, "fn");
        const total = try count(holdout, "count");
        if (tp > total or fp > total - tp or fn_count > total - tp - fp or tp + fn_count < 30 or total - tp - fn_count < 30) return error.UnqualifiedEmbeddingCalibration;
        const f1 = 2 * @as(f64, @floatFromInt(tp)) / @as(f64, @floatFromInt(2 * tp + fp + fn_count));
        if (!std.math.isFinite(f1) or f1 < coverage or lower(tp, tp + fp) < precision) return error.UnqualifiedEmbeddingCalibration;
    }
    return .{ .options = options, .thresholds = output };
}

pub fn load(a: std.mem.Allocator, io: std.Io, directory: []const u8, identity: []const u8, question: anytype, options: scoring.Options, mode: []const u8) !Policy {
    const id = options.calibration_id orelse return .{ .options = options };
    const relative = try std.fmt.allocPrint(a, "calibrations/{s}.json", .{id});
    defer a.free(relative);
    const path = receipt.resolveContainedArtifactPath(a, io, directory, relative) catch |err| switch (err) {
        error.FileNotFound => return error.InvalidEmbeddingCalibration,
        else => return err,
    };
    defer a.free(path);
    const bytes = try file.readFileMax(a, path, 256 * 1024);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(V, a, bytes, .{ .duplicate_field_behavior = .@"error" });
    defer parsed.deinit();
    return parse(a, parsed.value, identity, question, options, mode);
}

test "embeddinggemma2 unqualified calibration cannot supply thresholds" {
    const question = @import("decide.zig").Question{ .name = "route", .kind = .choice, .instructions = "route", .labels = &.{ "a", "b" }, .descriptions = &.{ "A", "B" } };
    const raw = try std.json.parseFromSlice(V, std.testing.allocator, "{\"version\":1,\"method\":\"heldout-abstention-v1\",\"qualified\":false}", .{});
    defer raw.deinit();
    try std.testing.expectError(error.UnqualifiedEmbeddingCalibration, parse(std.testing.allocator, raw.value, "identity", question, .{}, "single"));
}

test "embeddinggemma2 calibration binds prototypes and independently checks heldout metrics" {
    const a = std.testing.allocator;
    const question = @import("decide.zig").Question{ .name = "route", .kind = .choice, .instructions = "route", .labels = &.{ "a", "b" }, .descriptions = &.{ "A", "B" } };
    const identity: [64]u8 = @splat('a');
    const fit_hash: [64]u8 = @splat('b');
    const val_hash: [64]u8 = @splat('c');
    const holdout_hash: [64]u8 = @splat('d');
    const proto = try prototypes.prototypeSetHash(a, question, .{}, "single");
    // Synthetic counts exercise artifact validation; they do not qualify a
    // real deployment or assert any model quality.
    const bytes = try std.json.Stringify.valueAlloc(a, .{
        .version = 1,
        .method = "heldout-abstention-v1",
        .qualified = true,
        .binding = .{ .model_identity = identity[0..], .renderer_version = scoring.renderer_version, .task_type = "CLUSTERING", .dimensions = 768, .prototype_set_hash = proto[0..], .labels = question.labels, .mode = "single" },
        .metrics = .{ .fit = .{ .count = 30, .dataset_sha256 = fit_hash[0..] }, .validation = .{ .count = 30, .dataset_sha256 = val_hash[0..] }, .holdout = .{ .count = 100, .selected = 100, .correct = 100, .dataset_sha256 = holdout_hash[0..] } },
        .targets = .{ .precision_lower_95 = 0.9, .minimum_coverage_or_f1 = 0.8 },
        .thresholds = .{ .min_similarity = 0.5, .min_margin = 0.1 },
    }, .{});
    defer a.free(bytes);
    var raw = try std.json.parseFromSlice(V, a, bytes, .{});
    defer raw.deinit();
    const policy = try parse(a, raw.value, &identity, question, .{}, "single");
    try std.testing.expectEqual(@as(?f64, 0.5), policy.options.min_similarity);
    try std.testing.expectError(error.EmbeddingCalibrationMismatch, parse(a, raw.value, &identity, question, .{ .dimensions = 128 }, "single"));
    var changed = question;
    changed.instructions = "different route";
    try std.testing.expectError(error.EmbeddingCalibrationMismatch, parse(a, raw.value, &identity, changed, .{}, "single"));
    raw.value.object.getPtr("metrics").?.object.getPtr("holdout").?.object.getPtr("correct").?.* = .{ .integer = 80 };
    try std.testing.expectError(error.UnqualifiedEmbeddingCalibration, parse(a, raw.value, &identity, question, .{}, "single"));
}
