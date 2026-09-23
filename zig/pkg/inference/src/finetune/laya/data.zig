// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const pipeline = @import("../../pipelines/laya.zig");
const model = @import("../../models/laya.zig");
const objective = @import("objective.zig");
const training = @import("training.zig");
const Tokenizer = @import("inference_tokenizer").Tokenizer;
const files = @import("../../util/c_file.zig");

pub const Record = struct {
    id: []const u8,
    group_id: []const u8,
    text: []const u8,
    kind: model.QuestionType,
    instruction: []const u8,
    labels: []const []const u8,
    descriptions: ?[]const []const u8 = null,
    target: []const f32,
};
pub const Dataset = struct {
    records: []const Record,
    examples: []const training.Example,
    sha256: [32]u8,
};

pub fn digest(bytes: []const u8) [32]u8 {
    var out: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &out, .{});
    return out;
}

pub fn validate(r: Record) !void {
    if (r.id.len == 0 or r.id.len > 1024 or r.group_id.len == 0 or r.group_id.len > 1024 or r.text.len == 0 or r.text.len > 1024 * 1024 or r.instruction.len == 0 or r.instruction.len > 65536 or r.labels.len != r.target.len)
        return error.InvalidLayaTrainingRecord;
    try objective.validateTarget(r.kind, r.target);
    if (r.descriptions) |descs| if (descs.len != r.labels.len) return error.InvalidLayaTrainingRecord;
    for (r.labels, 0..) |label, i| {
        if (label.len == 0 or label.len > 65536) return error.InvalidLayaTrainingRecord;
        for (r.labels[0..i]) |prior| if (std.mem.eql(u8, label, prior)) return error.InvalidLayaTrainingRecord;
    }
    if (r.kind == .noul and (!std.mem.eql(u8, r.labels[0], "false") or !std.mem.eql(u8, r.labels[1], "true"))) return error.InvalidLayaTrainingRecord;
}

/// The dataset and tokenized sequences belong to the caller's bounded arena.
pub fn load(a: std.mem.Allocator, path: []const u8, tok: Tokenizer, cfg: model.Config) !Dataset {
    const bytes = try files.readFileMax(a, path, 64 * 1024 * 1024);
    var records: std.ArrayListUnmanaged(Record) = .empty;
    var examples: std.ArrayListUnmanaged(training.Example) = .empty;
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    var lines = std.mem.splitScalar(u8, bytes, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0) continue;
        if (records.items.len >= 100000) return error.LayaDatasetLimitExceeded;
        const parsed = try std.json.parseFromSlice(Record, a, line, .{ .allocate = .alloc_always });
        const record = parsed.value;
        try validate(record);
        const entry = try ids.getOrPut(a, record.id);
        if (entry.found_existing) return error.DuplicateLayaTrainingId;
        const descriptions = record.descriptions orelse blk: {
            const defaults = try a.alloc([]const u8, record.labels.len);
            @memset(defaults, "");
            break :blk defaults;
        };
        const sequence = try pipeline.prepare(a, tok, cfg, .{ .text = record.text, .question = .{ .name = record.id, .kind = record.kind, .instruction = record.instruction, .labels = record.labels, .descriptions = descriptions } });
        try records.append(a, record);
        try examples.append(a, .{ .ids = sequence.ids, .markers = sequence.markers, .kind = record.kind, .target = record.target });
    }
    if (records.items.len == 0) return error.EmptyLayaDataset;
    return .{ .records = try records.toOwnedSlice(a), .examples = try examples.toOwnedSlice(a), .sha256 = digest(bytes) };
}

/// Cases stay together across all questions. Also catch renamed copies of
/// source text and token-identical examples, even with different IDs/groups.
pub fn disjoint(a: std.mem.Allocator, left: Dataset, right: Dataset) !void {
    var groups: std.StringHashMapUnmanaged(void) = .empty;
    defer groups.deinit(a);
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    defer ids.deinit(a);
    var texts: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer texts.deinit(a);
    var tokens: std.AutoHashMapUnmanaged([32]u8, void) = .empty;
    defer tokens.deinit(a);
    for (left.records, left.examples) |record, example| {
        try groups.put(a, record.group_id, {});
        try ids.put(a, record.id, {});
        try texts.put(a, digest(record.text), {});
        try tokens.put(a, digest(std.mem.sliceAsBytes(example.ids)), {});
    }
    for (right.records, right.examples) |record, example| {
        if (groups.contains(record.group_id) or ids.contains(record.id) or texts.contains(digest(record.text)) or tokens.contains(digest(std.mem.sliceAsBytes(example.ids))))
            return error.LayaDatasetOverlap;
    }
}

test "laya training records reject mismatched targets and boolean order" {
    var record = Record{ .id = "case1/decision1", .group_id = "case1", .text = "hello", .kind = .noul, .instruction = "is this a greeting?", .labels = &.{ "false", "true" }, .target = &.{ 0.1, 0.9 } };
    try validate(record);
    record.labels = &.{ "true", "false" };
    try std.testing.expectError(error.InvalidLayaTrainingRecord, validate(record));
    record.kind = .choice;
    record.labels = &.{ "a", "a" };
    try std.testing.expectError(error.InvalidLayaTrainingRecord, validate(record));
}

test "laya training split separation catches renamed text and token duplicates" {
    const r = Record{ .id = "a", .group_id = "g", .text = "text", .kind = .choice, .instruction = "choose", .labels = &.{ "a", "b" }, .target = &.{ 1, 0 } };
    const e = training.Example{ .ids = &.{ 1, 2 }, .markers = &.{ 0, 1 }, .kind = .choice, .target = &.{ 1, 0 } };
    const left = Dataset{ .records = &.{r}, .examples = &.{e}, .sha256 = [_]u8{0} ** 32 };
    var changed = r;
    changed.id = "b";
    changed.group_id = "h";
    var other = e;
    other.ids = &.{ 3, 4 };
    var right = Dataset{ .records = &.{changed}, .examples = &.{other}, .sha256 = [_]u8{0} ** 32 };
    try std.testing.expectError(error.LayaDatasetOverlap, disjoint(std.testing.allocator, left, right));
    changed.text = "different text";
    right.records = &.{changed};
    try disjoint(std.testing.allocator, left, right);
    right.examples = &.{e};
    try std.testing.expectError(error.LayaDatasetOverlap, disjoint(std.testing.allocator, left, right));
}
