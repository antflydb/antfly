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
const compat = @import("../io/compat.zig");
const document_classification = @import("../pipelines/document_classification.zig");
const resolve_mod = @import("jsonl_resolve.zig");

pub const TokenBox = struct {
    text: []const u8,
    bbox: [4]i32,
};

pub const PageExample = struct {
    document_id: []const u8,
    page_id: []const u8,
    image_path: []const u8,
    tokens: []TokenBox,
    label: ?[]const u8 = null,
    token_labels: ?[][]const u8 = null,
    runtime_token_weights: ?[]f32 = null,
    teacher_token_hidden: ?[][]f32 = null,
    teacher_token_probs: ?[][]f32 = null,
};

pub const SequenceExample = struct {
    image_path: []const u8,
    resolved_image_path: []const u8,
    label: []const u8,
    num_tokens: usize,
    image_size_bytes: u64,
    image_width: u32,
    image_height: u32,
    image_components: u8,
    mean_darkness: f32,
    std_darkness: f32,
    top_darkness: f32,
    bottom_darkness: f32,
    left_darkness: f32,
    right_darkness: f32,
    center_darkness: f32,
};

pub const TokenTaskExample = struct {
    image_path: []const u8,
    tokens: []TokenBox,
    token_labels: [][]const u8,
    runtime_token_weights: ?[]f32 = null,
    teacher_token_hidden: ?[][]f32 = null,
    teacher_token_probs: ?[][]f32 = null,
};

pub const DatasetStats = struct {
    num_examples: usize = 0,
    avg_tokens: f64 = 0,
    examples_with_cls: usize = 0,
    examples_with_tok: usize = 0,
    class_labels: usize = 0,
    token_labels: usize = 0,
};

pub const LoadedExamples = struct {
    arena: std.heap.ArenaAllocator,
    dataset_root: []const u8,
    examples: []PageExample,

    pub fn deinit(self: *LoadedExamples) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn loadExamples(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !LoadedExamples {
    var resolved = try resolve_mod.resolveJsonlFiles(allocator, path, split);
    defer resolved.deinit();
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    var examples: std.ArrayListUnmanaged(PageExample) = .empty;
    defer examples.deinit(arena_alloc);
    for (resolved.paths) |resolved_path| {
        try loadExamplesFromFile(arena_alloc, resolved_path, &examples);
    }

    const first_path = resolved.paths[0];

    return .{
        .arena = arena,
        .dataset_root = try arena_alloc.dupe(u8, std.fs.path.dirname(first_path) orelse "."),
        .examples = try examples.toOwnedSlice(arena_alloc),
    };
}

pub fn computeStats(allocator: std.mem.Allocator, examples: []const PageExample) !DatasetStats {
    var stats = DatasetStats{ .num_examples = examples.len };
    if (examples.len == 0) return stats;

    var total_tokens: usize = 0;
    var class_labels = std.StringHashMapUnmanaged(void){};
    defer class_labels.deinit(allocator);
    var token_labels = std.StringHashMapUnmanaged(void){};
    defer token_labels.deinit(allocator);

    for (examples) |ex| {
        total_tokens += ex.tokens.len;
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len > 0) {
                stats.examples_with_cls += 1;
                try class_labels.put(allocator, label, {});
            }
        }
        if (ex.token_labels) |labels| {
            stats.examples_with_tok += 1;
            for (labels) |label| {
                if (std.mem.trim(u8, label, " \t\r\n").len > 0) {
                    try token_labels.put(allocator, label, {});
                }
            }
        }
    }

    stats.avg_tokens = @as(f64, @floatFromInt(total_tokens)) / @as(f64, @floatFromInt(examples.len));
    stats.class_labels = class_labels.count();
    stats.token_labels = token_labels.count();
    return stats;
}

pub fn buildSequenceLabelVocab(allocator: std.mem.Allocator, examples: []const PageExample) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    for (examples) |ex| {
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
        }
    }
    return try collectSortedKeys(allocator, &labels);
}

pub fn buildTokenLabelVocab(allocator: std.mem.Allocator, examples: []const PageExample) ![][]const u8 {
    var labels = std.StringHashMapUnmanaged(void){};
    defer labels.deinit(allocator);
    for (examples) |ex| {
        if (ex.token_labels) |items| {
            for (items) |label| {
                if (std.mem.trim(u8, label, " \t\r\n").len > 0) try labels.put(allocator, label, {});
            }
        }
    }
    return try collectSortedKeys(allocator, &labels);
}

pub fn filterSequenceExamples(
    allocator: std.mem.Allocator,
    dataset_root: []const u8,
    examples: []const PageExample,
) ![]SequenceExample {
    var out: std.ArrayListUnmanaged(SequenceExample) = .empty;
    errdefer {
        for (out.items) |item| allocator.free(item.resolved_image_path);
        out.deinit(allocator);
    }
    for (examples) |ex| {
        if (ex.label) |label| {
            if (std.mem.trim(u8, label, " \t\r\n").len == 0) continue;
            const resolved_image_path = try resolveImagePath(allocator, dataset_root, ex.image_path);
            const image_size_bytes = fileSizeOrZero(resolved_image_path);
            const features = try document_classification.extractFeatures(allocator, .{
                .image_path = resolved_image_path,
                .num_tokens = ex.tokens.len,
            });
            try out.append(allocator, .{
                .image_path = ex.image_path,
                .resolved_image_path = resolved_image_path,
                .label = label,
                .num_tokens = ex.tokens.len,
                .image_size_bytes = image_size_bytes,
                .image_width = features.image_width,
                .image_height = features.image_height,
                .image_components = features.image_components,
                .mean_darkness = features.mean_darkness,
                .std_darkness = features.std_darkness,
                .top_darkness = features.top_darkness,
                .bottom_darkness = features.bottom_darkness,
                .left_darkness = features.left_darkness,
                .right_darkness = features.right_darkness,
                .center_darkness = features.center_darkness,
            });
        }
    }
    return try out.toOwnedSlice(allocator);
}

pub fn filterTokenExamples(
    allocator: std.mem.Allocator,
    examples: []const PageExample,
) ![]TokenTaskExample {
    var out: std.ArrayListUnmanaged(TokenTaskExample) = .empty;
    errdefer out.deinit(allocator);
    for (examples) |ex| {
        if (ex.token_labels) |labels| {
            if (labels.len == 0) continue;
            try out.append(allocator, .{
                .image_path = ex.image_path,
                .tokens = ex.tokens,
                .token_labels = labels,
                .runtime_token_weights = ex.runtime_token_weights,
                .teacher_token_hidden = ex.teacher_token_hidden,
                .teacher_token_probs = ex.teacher_token_probs,
            });
        }
    }
    return try out.toOwnedSlice(allocator);
}

pub fn freeSequenceExamples(allocator: std.mem.Allocator, examples: []const SequenceExample) void {
    for (examples) |ex| allocator.free(ex.resolved_image_path);
    allocator.free(examples);
}

pub fn validateBoxes(tokens: []const TokenBox) !void {
    for (tokens) |tok| {
        const x0 = tok.bbox[0];
        const y0 = tok.bbox[1];
        const x1 = tok.bbox[2];
        const y1 = tok.bbox[3];
        if (x0 < 0 or y0 < 0 or x1 < 0 or y1 < 0 or x0 > 1000 or y0 > 1000 or x1 > 1000 or y1 > 1000) {
            return error.BBoxOutOfRange;
        }
        if (x1 < x0 or y1 < y0) return error.InvertedBBox;
    }
}

pub fn resolveImagePath(allocator: std.mem.Allocator, dataset_root: []const u8, image_path: []const u8) ![]const u8 {
    if (std.fs.path.isAbsolute(image_path)) {
        return try allocator.dupe(u8, image_path);
    }
    return try std.fs.path.join(allocator, &.{ dataset_root, image_path });
}

fn loadExamplesFromFile(allocator: std.mem.Allocator, path: []const u8, out: *std.ArrayListUnmanaged(PageExample)) !void {
    const data = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(128 * 1024 * 1024));
    var lines = std.mem.tokenizeScalar(u8, data, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0) continue;
        const parsed = try std.json.parseFromSliceLeaky(PageExample, allocator, line, .{
            .ignore_unknown_fields = true,
        });
        if (std.mem.trim(u8, parsed.image_path, " \t\r\n").len == 0) return error.MissingImagePath;
        try validateBoxes(parsed.tokens);
        if (parsed.token_labels) |labels| {
            if (labels.len != parsed.tokens.len) return error.TokenLabelCountMismatch;
        }
        if (parsed.runtime_token_weights) |weights| {
            if (weights.len != parsed.tokens.len) return error.TokenLabelCountMismatch;
        }
        if (parsed.teacher_token_hidden) |hidden| {
            if (hidden.len != parsed.tokens.len) return error.TokenLabelCountMismatch;
        }
        if (parsed.teacher_token_probs) |probs| {
            if (probs.len != parsed.tokens.len) return error.TokenLabelCountMismatch;
        }
        try out.append(allocator, parsed);
    }
}

fn collectSortedKeys(allocator: std.mem.Allocator, labels: *std.StringHashMapUnmanaged(void)) ![][]const u8 {
    var out = try allocator.alloc([]const u8, labels.count());
    var iter = labels.iterator();
    var i: usize = 0;
    while (iter.next()) |entry| : (i += 1) {
        out[i] = try allocator.dupe(u8, entry.key_ptr.*);
    }
    std.mem.sort([]const u8, out, {}, lessThanString);
    return out;
}

fn lessThanString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn fileSizeOrZero(path: []const u8) u64 {
    const stat = compat.cwd().statFile(compat.io(), path, .{}) catch return 0;
    return stat.size;
}

test "load document examples and build vocabs" {
    const allocator = std.testing.allocator;
    const root = try std.fmt.allocPrint(allocator, "termite-finetune-tests-{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    const root_dir = try std.fs.path.join(allocator, &.{ "/tmp", root, "document-data" });
    defer allocator.free(root_dir);
    defer compat.cwd().deleteTree(compat.io(), root_dir) catch {};
    try compat.cwd().createDirPath(compat.io(), root_dir);

    const train_jsonl =
        \\{"document_id":"d1","page_id":"p1","image_path":"a.png","tokens":[{"text":"hello","bbox":[0,0,10,10]}],"label":"email"}
        \\{"document_id":"d2","page_id":"p1","image_path":"b.png","tokens":[{"text":"world","bbox":[0,0,10,10]}],"token_labels":["B-QUESTION"]}
        \\
    ;
    const jsonl_path = try std.fs.path.join(allocator, &.{ root_dir, "train-00000.jsonl" });
    defer allocator.free(jsonl_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = jsonl_path, .data = train_jsonl });

    const path = try allocator.dupe(u8, root_dir);
    defer allocator.free(path);

    var loaded = try loadExamples(allocator, path, "train");
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), loaded.examples.len);

    const stats = try computeStats(allocator, loaded.examples);
    try std.testing.expectEqual(@as(usize, 1), stats.examples_with_cls);
    try std.testing.expectEqual(@as(usize, 1), stats.examples_with_tok);

    const seq_vocab = try buildSequenceLabelVocab(allocator, loaded.examples);
    defer {
        for (seq_vocab) |item| allocator.free(item);
        allocator.free(seq_vocab);
    }
    try std.testing.expectEqual(@as(usize, 1), seq_vocab.len);
    try std.testing.expectEqualStrings("email", seq_vocab[0]);

    const tok_vocab = try buildTokenLabelVocab(allocator, loaded.examples);
    defer {
        for (tok_vocab) |item| allocator.free(item);
        allocator.free(tok_vocab);
    }
    try std.testing.expectEqual(@as(usize, 1), tok_vocab.len);
    try std.testing.expectEqualStrings("B-QUESTION", tok_vocab[0]);
}

test "loadExamples honors directory split selection" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train-00000.jsonl", .data = "{\"document_id\":\"train\",\"page_id\":\"p1\",\"image_path\":\"a.png\",\"tokens\":[{\"text\":\"hello\",\"bbox\":[0,0,1,1]}],\"label\":\"train\"}\n" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "val-00000.jsonl", .data = "{\"document_id\":\"val\",\"page_id\":\"p1\",\"image_path\":\"b.png\",\"tokens\":[{\"text\":\"world\",\"bbox\":[0,0,1,1]}],\"label\":\"val\"}\n" });

    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(root);

    var train_loaded = try loadExamples(allocator, root, "train");
    defer train_loaded.deinit();
    try std.testing.expectEqual(@as(usize, 1), train_loaded.examples.len);
    try std.testing.expectEqualStrings("train", train_loaded.examples[0].document_id);

    var val_loaded = try loadExamples(allocator, root, "val");
    defer val_loaded.deinit();
    try std.testing.expectEqual(@as(usize, 1), val_loaded.examples.len);
    try std.testing.expectEqualStrings("val", val_loaded.examples[0].document_id);
}

test "filter sequence and token examples" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const valid_png_2x2 = [_]u8{
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
        0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x02,
        0x08, 0x02, 0x00, 0x00, 0x00, 0xfd, 0xd4, 0x9a, 0x73, 0x00, 0x00, 0x00,
        0x09, 0x70, 0x48, 0x59, 0x73, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x4f, 0x25, 0xc4, 0xd6, 0x00, 0x00, 0x00, 0x10, 0x49, 0x44,
        0x41, 0x54, 0x78, 0x9c, 0x63, 0xfc, 0xc3, 0x00, 0x02, 0x2c, 0x60, 0x92,
        0x01, 0x00, 0x0d, 0x04, 0x01, 0x02, 0xbf, 0x50, 0x15, 0xb3, 0x00, 0x00,
        0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    };
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "a.png", .data = &valid_png_2x2 });

    var tokens1 = [_]TokenBox{.{ .text = "hello", .bbox = .{ 0, 0, 10, 10 } }};
    var tokens2 = [_]TokenBox{.{ .text = "world", .bbox = .{ 0, 0, 10, 10 } }};
    var token_labels = [_][]const u8{"B-QUESTION"};
    const examples = [_]PageExample{
        .{
            .document_id = "d1",
            .page_id = "p1",
            .image_path = "a.png",
            .tokens = tokens1[0..],
            .label = "email",
        },
        .{
            .document_id = "d2",
            .page_id = "p1",
            .image_path = "b.png",
            .tokens = tokens2[0..],
            .token_labels = token_labels[0..],
        },
    };

    const dataset_root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dataset_root);

    const seq = try filterSequenceExamples(allocator, dataset_root, examples[0..]);
    defer freeSequenceExamples(allocator, seq);
    try std.testing.expectEqual(@as(usize, 1), seq.len);
    const expected_image_path = try std.fs.path.join(allocator, &.{ dataset_root, "a.png" });
    defer allocator.free(expected_image_path);
    try std.testing.expectEqualStrings(expected_image_path, seq[0].resolved_image_path);

    const tok = try filterTokenExamples(allocator, examples[0..]);
    defer allocator.free(tok);
    try std.testing.expectEqual(@as(usize, 1), tok.len);
    try std.testing.expectEqual(@as(usize, 1), tok[0].token_labels.len);
}
