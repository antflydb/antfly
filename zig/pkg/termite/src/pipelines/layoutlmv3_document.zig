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
const backends = @import("../backends/backends.zig");
const c_file = @import("../util/c_file.zig");
const compat = @import("../io/compat.zig");
const document_prep = @import("document_preprocessing.zig");
const layoutlmv3 = @import("layoutlmv3_session.zig");

pub const OcrToken = document_prep.OcrToken;

pub const Task = enum {
    sequence,
    token,
};

pub const LabelSource = enum {
    request,
    sequence_head_config,
    token_head_config,
    config,
};

pub const LabelSet = struct {
    allocator: std.mem.Allocator,
    labels: []const []const u8,
    source: LabelSource,

    pub fn deinit(self: *LabelSet) void {
        for (self.labels) |label| self.allocator.free(label);
        self.allocator.free(self.labels);
        self.* = undefined;
    }
};

pub const SequenceOutput = struct {
    allocator: std.mem.Allocator,
    labels: LabelSet,
    prepared: document_prep.PreparedInputs,
    summary: document_prep.PreparationSummary,
    results: []layoutlmv3.SequenceResult,

    pub fn deinit(self: *SequenceOutput) void {
        self.allocator.free(self.results);
        freePreparationSummary(self.allocator, &self.summary);
        self.prepared.deinit();
        self.labels.deinit();
        self.* = undefined;
    }
};

pub const TokenOutput = struct {
    allocator: std.mem.Allocator,
    labels: LabelSet,
    prepared: document_prep.PreparedInputs,
    summary: document_prep.PreparationSummary,
    predictions: []layoutlmv3.TokenPrediction,

    pub fn deinit(self: *TokenOutput) void {
        self.allocator.free(self.predictions);
        freePreparationSummary(self.allocator, &self.summary);
        self.prepared.deinit();
        self.labels.deinit();
        self.* = undefined;
    }
};

pub const RuntimeBundleReport = struct {
    task: []const u8 = "inspect_layoutlmv3_runtime_bundle",
    model_dir: []const u8,
    looks_like_full_bundle: bool,
    has_config: bool,
    has_tokenizer: bool,
    has_preprocessor: bool,
    has_dense_checkpoint: bool,
    has_sequence_labels: bool,
    has_token_labels: bool,
    label_source_sequence: ?[]const u8 = null,
    label_source_token: ?[]const u8 = null,
    missing_required: []const []const u8,

    pub fn deinit(self: *RuntimeBundleReport, allocator: std.mem.Allocator) void {
        allocator.free(self.missing_required);
        self.* = undefined;
    }
};

pub fn looksLikeFullBundle(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return false;
    if (!hasDenseCheckpoint(allocator, model_dir)) return false;

    const config_bytes = c_file.readFileFromDir(allocator, model_dir, "config.json") catch return false;
    defer allocator.free(config_bytes);
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const model_type = parsed.value.object.get("model_type") orelse return false;
    return model_type == .string and std.mem.eql(u8, model_type.string, "layoutlmv3");
}

pub fn inspectRuntimeBundle(allocator: std.mem.Allocator, model_dir: []const u8) !RuntimeBundleReport {
    const has_config = c_file.fileExistsInDir(allocator, model_dir, "config.json");
    const has_tokenizer = c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json");
    const has_preprocessor = c_file.fileExistsInDir(allocator, model_dir, "preprocessor_config.json");
    const dense = hasDenseCheckpoint(allocator, model_dir);

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    errdefer missing.deinit(allocator);
    if (!has_config) try missing.append(allocator, "config.json");
    if (!has_tokenizer) try missing.append(allocator, "tokenizer.json");
    if (!has_preprocessor) try missing.append(allocator, "preprocessor_config.json");
    if (!dense) try missing.append(allocator, "model.safetensors or model.safetensors.index.json");

    var sequence_labels = loadLabels(allocator, model_dir, .sequence, null) catch null;
    defer if (sequence_labels) |*labels| labels.deinit();
    var token_labels = loadLabels(allocator, model_dir, .token, null) catch null;
    defer if (token_labels) |*labels| labels.deinit();

    return .{
        .model_dir = model_dir,
        .looks_like_full_bundle = looksLikeFullBundle(allocator, model_dir),
        .has_config = has_config,
        .has_tokenizer = has_tokenizer,
        .has_preprocessor = has_preprocessor,
        .has_dense_checkpoint = dense,
        .has_sequence_labels = sequence_labels != null,
        .has_token_labels = token_labels != null,
        .label_source_sequence = if (sequence_labels) |labels| @tagName(labels.source) else null,
        .label_source_token = if (token_labels) |labels| @tagName(labels.source) else null,
        .missing_required = try missing.toOwnedSlice(allocator),
    };
}

pub fn classifySequence(
    allocator: std.mem.Allocator,
    session: backends.Session,
    model_dir: []const u8,
    image_path: []const u8,
    tokens: []const OcrToken,
    labels_override: ?[]const []const u8,
    max_length_override: ?usize,
) !SequenceOutput {
    var labels = try loadLabels(allocator, model_dir, .sequence, labels_override);
    errdefer labels.deinit();
    var prepared = try document_prep.prepareFromFiles(allocator, model_dir, image_path, tokens, max_length_override);
    errdefer prepared.deinit();
    var summary = try document_prep.summarizePreparedInputs(allocator, model_dir, image_path, &prepared);
    errdefer freePreparationSummary(allocator, &summary);
    const results = try layoutlmv3.classifySequencePrepared(allocator, session, &prepared, labels.labels);
    return .{
        .allocator = allocator,
        .labels = labels,
        .prepared = prepared,
        .summary = summary,
        .results = results,
    };
}

pub fn classifyTokens(
    allocator: std.mem.Allocator,
    session: backends.Session,
    model_dir: []const u8,
    image_path: []const u8,
    tokens: []const OcrToken,
    labels_override: ?[]const []const u8,
    max_length_override: ?usize,
) !TokenOutput {
    var labels = try loadLabels(allocator, model_dir, .token, labels_override);
    errdefer labels.deinit();
    var prepared = try document_prep.prepareFromFiles(allocator, model_dir, image_path, tokens, max_length_override);
    errdefer prepared.deinit();
    var summary = try document_prep.summarizePreparedInputs(allocator, model_dir, image_path, &prepared);
    errdefer freePreparationSummary(allocator, &summary);
    const predictions = try layoutlmv3.classifyTokenPrepared(allocator, session, &prepared, labels.labels);
    return .{
        .allocator = allocator,
        .labels = labels,
        .prepared = prepared,
        .summary = summary,
        .predictions = predictions,
    };
}

pub fn loadLabels(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    task: Task,
    labels_override: ?[]const []const u8,
) !LabelSet {
    if (labels_override) |labels| {
        if (labels.len > 0) return .{
            .allocator = allocator,
            .labels = try dupeLabels(allocator, labels),
            .source = .request,
        };
    }

    const task_config = switch (task) {
        .sequence => "sequence_head_config.json",
        .token => "token_head_config.json",
    };
    if (try loadLabelsFromFile(allocator, model_dir, task_config)) |labels| {
        return .{
            .allocator = allocator,
            .labels = labels,
            .source = switch (task) {
                .sequence => .sequence_head_config,
                .token => .token_head_config,
            },
        };
    }
    if (try loadLabelsFromFile(allocator, model_dir, "config.json")) |labels| {
        return .{ .allocator = allocator, .labels = labels, .source = .config };
    }
    return error.NoLabelsProvided;
}

pub fn freePreparationSummary(allocator: std.mem.Allocator, summary: *document_prep.PreparationSummary) void {
    allocator.free(summary.sample_input_ids);
    allocator.free(summary.sample_bboxes);
}

fn hasDenseCheckpoint(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    return c_file.fileExistsInDir(allocator, model_dir, "model.safetensors") or
        c_file.fileExistsInDir(allocator, model_dir, "pytorch_model.safetensors") or
        c_file.fileExistsInDir(allocator, model_dir, "model.safetensors.index.json") or
        c_file.fileExistsInDir(allocator, model_dir, "pytorch_model.safetensors.index.json");
}

fn loadLabelsFromFile(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    basename: []const u8,
) !?[]const []const u8 {
    const bytes = c_file.readFileFromDir(allocator, model_dir, basename) catch return null;
    defer allocator.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const obj = parsed.value.object;
    if (obj.get("labels")) |labels_value| {
        if (try labelsFromArray(allocator, labels_value)) |labels| return labels;
    }
    if (obj.get("id2label")) |labels_value| {
        if (try labelsFromId2Label(allocator, labels_value)) |labels| return labels;
    }
    if (obj.get("label2id")) |labels_value| {
        if (try labelsFromLabel2Id(allocator, labels_value)) |labels| return labels;
    }
    return null;
}

fn labelsFromArray(allocator: std.mem.Allocator, value: std.json.Value) !?[]const []const u8 {
    if (value != .array or value.array.items.len == 0) return null;
    const labels = try allocator.alloc([]const u8, value.array.items.len);
    errdefer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    for (value.array.items, 0..) |item, idx| {
        if (item != .string) return null;
        labels[idx] = try allocator.dupe(u8, item.string);
    }
    return labels;
}

const LabelEntry = struct {
    index: usize,
    label: []const u8,
};

fn labelsFromId2Label(allocator: std.mem.Allocator, value: std.json.Value) !?[]const []const u8 {
    if (value != .object or value.object.count() == 0) return null;
    var entries = std.ArrayListUnmanaged(LabelEntry).empty;
    defer entries.deinit(allocator);
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .string) continue;
        const index = std.fmt.parseInt(usize, entry.key_ptr.*, 10) catch continue;
        try entries.append(allocator, .{ .index = index, .label = entry.value_ptr.string });
    }
    return labelsFromEntries(allocator, entries.items);
}

fn labelsFromLabel2Id(allocator: std.mem.Allocator, value: std.json.Value) !?[]const []const u8 {
    if (value != .object or value.object.count() == 0) return null;
    var entries = std.ArrayListUnmanaged(LabelEntry).empty;
    defer entries.deinit(allocator);
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .integer) continue;
        if (entry.value_ptr.integer < 0) continue;
        try entries.append(allocator, .{ .index = @intCast(entry.value_ptr.integer), .label = entry.key_ptr.* });
    }
    return labelsFromEntries(allocator, entries.items);
}

fn labelsFromEntries(allocator: std.mem.Allocator, entries: []LabelEntry) !?[]const []const u8 {
    if (entries.len == 0) return null;
    std.mem.sort(LabelEntry, entries, {}, struct {
        fn lessThan(_: void, lhs: LabelEntry, rhs: LabelEntry) bool {
            return lhs.index < rhs.index;
        }
    }.lessThan);
    for (entries, 0..) |entry, idx| {
        if (entry.index != idx) return error.NonContiguousLabels;
    }
    const labels = try allocator.alloc([]const u8, entries.len);
    errdefer {
        for (labels) |label| allocator.free(label);
        allocator.free(labels);
    }
    for (entries, 0..) |entry, idx| {
        labels[idx] = try allocator.dupe(u8, entry.label);
    }
    return labels;
}

fn dupeLabels(allocator: std.mem.Allocator, labels: []const []const u8) ![]const []const u8 {
    const out = try allocator.alloc([]const u8, labels.len);
    errdefer {
        for (out) |label| allocator.free(label);
        allocator.free(out);
    }
    for (labels, 0..) |label, idx| out[idx] = try allocator.dupe(u8, label);
    return out;
}

test "loadLabels reads ordered id2label from config" {
    const allocator = std.testing.allocator;
    const root = try std.fs.path.join(allocator, &.{ "/tmp", "termite-layoutlmv3-document-labels" });
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);
    const path = try std.fs.path.join(allocator, &.{ root, "config.json" });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = path,
        .data =
        \\{"id2label":{"1":"invoice","0":"receipt"}}
        ,
    });

    var labels = try loadLabels(allocator, root, .sequence, null);
    defer labels.deinit();
    try std.testing.expectEqualStrings("receipt", labels.labels[0]);
    try std.testing.expectEqualStrings("invoice", labels.labels[1]);
    try std.testing.expectEqual(LabelSource.config, labels.source);
}

test "inspectRuntimeBundle reports missing production files" {
    const allocator = std.testing.allocator;
    const root = try std.fs.path.join(allocator, &.{ "/tmp", "termite-layoutlmv3-runtime-inspect" });
    defer allocator.free(root);
    compat.cwd().deleteTree(compat.io(), root) catch {};
    defer compat.cwd().deleteTree(compat.io(), root) catch {};
    try compat.cwd().createDirPath(compat.io(), root);

    var report = try inspectRuntimeBundle(allocator, root);
    defer report.deinit(allocator);
    try std.testing.expect(!report.looks_like_full_bundle);
    try std.testing.expectEqual(@as(usize, 4), report.missing_required.len);
}
