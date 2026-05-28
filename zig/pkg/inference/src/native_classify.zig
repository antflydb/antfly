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
const backends = @import("backends/backends.zig");
const classification_mod = @import("pipelines/classification.zig");
const graph_runtime = @import("graph/runtime.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_choice = @import("native_backend_choice.zig");

const print = std.debug.print;

const Options = struct {
    model_dir: []const u8,
    texts: std.ArrayListUnmanaged([]const u8) = .empty,
    labels: std.ArrayListUnmanaged([]const u8) = .empty,
    backend: native_backend_choice.Choice = .auto,
    multi_label: bool = false,
    hypothesis_template: []const u8 = "This example is {}.",
    entailment_index: ?usize = null,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        self.labels.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    try native_backend_choice.validate(opts.backend);
    native_backend_choice.configureSessionPreference(&session_manager, opts.backend);
    session_manager.graph_runtime_strategy = opts.graph_runtime_strategy;

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const entailment_idx = opts.entailment_index orelse detectEntailmentIndex(model);
    var pipeline = model.classificationPipeline(allocator, .{
        .max_length = model.manifest.max_position_embeddings,
        .hypothesis_template = opts.hypothesis_template,
        .multi_label = opts.multi_label,
        .entailment_index = entailment_idx,
    });

    const all_results = try pipeline.classifyBatch(opts.texts.items, opts.labels.items);
    defer {
        for (all_results) |results| allocator.free(results);
        allocator.free(all_results);
    }

    try writeClassificationJson(allocator, opts.model_dir, all_results);
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !Options {
    if (args.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
    };
    errdefer opts.deinit(allocator);

    var i: usize = 1;
    if (!std.mem.startsWith(u8, args[i], "--")) {
        try opts.texts.append(allocator, args[i]);
        i += 1;
    }

    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = native_backend_choice.parse(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--graph-runtime")) {
            i += 1;
            if (i >= args.len) return error.MissingGraphRuntimeValue;
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(args[i]) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.startsWith(u8, arg, "--graph-runtime=")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(arg["--graph-runtime=".len..]) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.eql(u8, arg, "--text")) {
            i += 1;
            if (i >= args.len) return error.MissingTextValue;
            try opts.texts.append(allocator, args[i]);
        } else if (std.mem.eql(u8, arg, "--label")) {
            i += 1;
            if (i >= args.len) return error.MissingLabelValue;
            try opts.labels.append(allocator, args[i]);
        } else if (std.mem.eql(u8, arg, "--multi-label")) {
            opts.multi_label = true;
        } else if (std.mem.eql(u8, arg, "--hypothesis-template")) {
            i += 1;
            if (i >= args.len) return error.MissingHypothesisTemplateValue;
            opts.hypothesis_template = args[i];
        } else if (std.mem.eql(u8, arg, "--entailment-index")) {
            i += 1;
            if (i >= args.len) return error.MissingEntailmentIndexValue;
            opts.entailment_index = try std.fmt.parseInt(usize, args[i], 10);
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    if (opts.texts.items.len == 0 or opts.labels.items.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }

    return opts;
}

fn detectEntailmentIndex(model: *model_manager_mod.LoadedModel) ?usize {
    if (model.manifest.id2label) |labels| {
        for (labels, 0..) |label, i| {
            if (std.mem.eql(u8, label, "entailment") or std.mem.eql(u8, label, "ENTAILMENT")) {
                return i;
            }
        }
    }
    return null;
}

fn writeClassificationJson(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    all_results: []const []const classification_mod.ClassificationResult,
) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"classifications\":[");
    for (all_results, 0..) |results, text_index| {
        if (text_index > 0) try buf.append(allocator, ',');
        try buf.append(allocator, '[');
        for (results, 0..) |result, result_index| {
            if (result_index > 0) try buf.append(allocator, ',');
            try buf.appendSlice(allocator, "{\"label\":");
            try jsonEncodeString(&buf, allocator, result.label);
            const score = try std.fmt.allocPrint(allocator, ",\"score\":{d}}}", .{result.score});
            defer allocator.free(score);
            try buf.appendSlice(allocator, score);
        }
        try buf.append(allocator, ']');
    }
    try buf.appendSlice(allocator, "]}\n");

    print("{s}", .{buf.items});
}

fn jsonEncodeString(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    try buf.append(allocator, '"');
    for (s) |ch| {
        switch (ch) {
            '"' => try buf.appendSlice(allocator, "\\\""),
            '\\' => try buf.appendSlice(allocator, "\\\\"),
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => try buf.appendSlice(allocator, "\\r"),
            '\t' => try buf.appendSlice(allocator, "\\t"),
            else => {
                if (ch < 0x20) {
                    const hex = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{ch});
                    defer allocator.free(hex);
                    try buf.appendSlice(allocator, hex);
                } else {
                    try buf.append(allocator, ch);
                }
            },
        }
    }
    try buf.append(allocator, '"');
}

fn printUsage() void {
    print(
        \\usage: termite classify <model-dir> [<text>] [--text <text>]... --label <label>... [--backend auto|onnx|native|metal|mlx|xla] [--graph-runtime interpreter|partitioned|compiled|compiled-required] [--multi-label] [--hypothesis-template <template>] [--entailment-index <n>]
        \\  Runs native local classification and prints a JSON response to stdout.
        \\  graph-runtime controls imported static graph execution; default is environment fallback, then interpreter.
        \\
    , .{});
}

test "parseArgs collects repeated texts and labels" {
    const allocator = std.testing.allocator;
    var opts = try parseArgs(allocator, &.{
        "/tmp/model",
        "--text",
        "first",
        "--text",
        "second",
        "--label",
        "technology",
        "--label",
        "sports",
        "--multi-label",
        "--entailment-index",
        "0",
        "--graph-runtime=compiled-preferred",
    });
    defer opts.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqual(@as(usize, 2), opts.texts.items.len);
    try std.testing.expectEqualStrings("first", opts.texts.items[0]);
    try std.testing.expectEqualStrings("second", opts.texts.items[1]);
    try std.testing.expectEqual(@as(usize, 2), opts.labels.items.len);
    try std.testing.expectEqualStrings("technology", opts.labels.items[0]);
    try std.testing.expectEqualStrings("sports", opts.labels.items[1]);
    try std.testing.expect(opts.multi_label);
    try std.testing.expectEqual(@as(?usize, 0), opts.entailment_index);
    try std.testing.expectEqual(graph_runtime.Strategy.compiled_preferred, opts.graph_runtime_strategy.?);
}

test "parseArgs accepts positional first text" {
    const allocator = std.testing.allocator;
    var opts = try parseArgs(allocator, &.{
        "/tmp/model",
        "hello world",
        "--label",
        "technology",
    });
    defer opts.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), opts.texts.items.len);
    try std.testing.expectEqualStrings("hello world", opts.texts.items[0]);
    try std.testing.expectEqual(@as(usize, 1), opts.labels.items.len);
    try std.testing.expectEqualStrings("technology", opts.labels.items[0]);
}
