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
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const cleanup_model_mod = @import("finetune/entity_cleanup_model.zig");
const cleanup_pipeline_mod = @import("pipelines/entity_cleanup.zig");
const graph_runtime = @import("graph/runtime.zig");
const model_manager_mod = @import("server/model_manager.zig");
const ner_mod = @import("pipelines/ner.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    metal,
    mlx,
    cuda,
};

const Options = struct {
    model_dir: []const u8,
    text: []const u8,
    backend: BackendChoice = .auto,
    labels: std.ArrayListUnmanaged([]const u8) = .empty,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.labels.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);
    session_manager.graph_runtime_strategy = opts.graph_runtime_strategy;

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const texts = [_][]const u8{opts.text};
    const entities = if (model.isGlinerModel()) blk: {
        var pipeline = model.glinerPipeline(allocator);
        const labels: ?[]const []const u8 = if (opts.labels.items.len > 0) opts.labels.items else null;
        break :blk try pipeline.recognizeBatch(&texts, labels);
    } else blk: {
        var pipeline = model.nerPipeline(allocator);
        break :blk try pipeline.recognizeBatch(&texts);
    };
    defer freeEntities(allocator, entities);

    const cleaned_entities = try applyLearnedCleanupIfPresent(allocator, try model.getCleanupHead(), &texts, entities);
    defer if (cleaned_entities) |cleaned| freeEntities(allocator, cleaned);

    try writeRecognizeJson(allocator, opts.model_dir, cleaned_entities orelse entities);
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !Options {
    if (args.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
        .text = args[1],
    };
    errdefer opts.deinit(allocator);

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--graph-runtime")) {
            i += 1;
            if (i >= args.len) return error.MissingGraphRuntimeValue;
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(args[i]) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.startsWith(u8, arg, "--graph-runtime=")) {
            opts.graph_runtime_strategy = graph_runtime.parseStrategy(arg["--graph-runtime=".len..]) orelse return error.InvalidGraphRuntime;
        } else if (std.mem.eql(u8, arg, "--label")) {
            i += 1;
            if (i >= args.len) return error.MissingLabelValue;
            try opts.labels.append(allocator, args[i]);
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn writeRecognizeJson(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    all_entities: []const []const ner_mod.Entity,
) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"entities\":[");
    for (all_entities, 0..) |entities, ti| {
        if (ti > 0) try buf.append(allocator, ',');
        try buf.append(allocator, '[');
        for (entities, 0..) |e, ei| {
            if (ei > 0) try buf.append(allocator, ',');
            try buf.appendSlice(allocator, "{\"text\":");
            try jsonEncodeString(&buf, allocator, e.text);
            try buf.appendSlice(allocator, ",\"label\":");
            try jsonEncodeString(&buf, allocator, e.label);
            const meta = try std.fmt.allocPrint(
                allocator,
                ",\"start\":{d},\"end\":{d},\"score\":{d}}}",
                .{ e.start, e.end, e.score },
            );
            defer allocator.free(meta);
            try buf.appendSlice(allocator, meta);
        }
        try buf.append(allocator, ']');
    }
    try buf.appendSlice(allocator, "]}\n");

    print("{s}", .{buf.items});
}

fn freeEntities(allocator: std.mem.Allocator, all_entities: [][]ner_mod.Entity) void {
    for (all_entities) |entities| {
        for (entities) |e| allocator.free(e.text);
        allocator.free(entities);
    }
    allocator.free(all_entities);
}

fn applyLearnedCleanupIfPresent(
    allocator: std.mem.Allocator,
    cleanup_head: ?*const cleanup_model_mod.CleanupHead,
    texts: []const []const u8,
    entities_by_text: []const []const ner_mod.Entity,
) !?[][]ner_mod.Entity {
    if (texts.len != entities_by_text.len) return error.ShapeMismatch;

    const head = cleanup_head orelse return null;

    const out = try allocator.alloc([]ner_mod.Entity, texts.len);
    var built: usize = 0;
    errdefer {
        freeEntities(allocator, out[0..built]);
        allocator.free(out);
    }

    for (texts, entities_by_text, 0..) |text, entities, idx| {
        var cleanup_entities = try allocator.alloc(cleanup_pipeline_mod.Entity, entities.len);
        defer allocator.free(cleanup_entities);
        for (entities, 0..) |entity, entity_idx| {
            cleanup_entities[entity_idx] = .{
                .text = entity.text,
                .label = entity.label,
                .start = entity.start,
                .end = entity.end,
                .score = entity.score,
            };
        }

        const scored = try cleanup_model_mod.scoreEntities(allocator, head, text, cleanup_entities);
        defer {
            for (scored) |*mention| mention.deinit(allocator);
            allocator.free(scored);
        }

        var cleaned = try cleanup_pipeline_mod.cleanupMentions(allocator, scored, .{
            .min_validity_score = head.min_validity_score,
            .dedup_similarity_threshold = head.dedup_similarity_threshold,
        });
        defer cleaned.deinit(allocator);

        out[idx] = try allocator.alloc(ner_mod.Entity, cleaned.resolved_entities.len);
        for (cleaned.resolved_entities, 0..) |resolved_entity, entity_idx| {
            out[idx][entity_idx] = .{
                .text = try allocator.dupe(u8, resolved_entity.text),
                .label = try allocator.dupe(u8, resolved_entity.label),
                .start = resolved_entity.start,
                .end = resolved_entity.end,
                .score = resolved_entity.detect_score * resolved_entity.validity_score,
            };
        }
        built += 1;
    }

    return out;
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

fn parseBackendChoice(value: []const u8) ?BackendChoice {
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.metal, backends.BackendType.mlx, backends.BackendType.native }
        else if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else if (build_options.enable_mlx)
            &.{ backends.BackendType.mlx, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .mlx => if (build_options.enable_mlx) &.{backends.BackendType.mlx} else &.{backends.BackendType.native},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
    };
}

fn printUsage() void {
    print(
        \\usage: antfly inference recognize <model-dir> <text> [--label NAME]... [--backend auto|native|metal|mlx|cuda] [--graph-runtime interpreter|partitioned|compiled|compiled-required]
        \\  Runs native local recognition and prints a JSON response to stdout.
        \\  --graph-runtime selects how the GLiNER head executes:
        \\    interpreter (default) -- eager forward via gliner_head.forwardCt
        \\    compiled / partitioned / compiled-required -- route the head
        \\        through gliner_head_graph.runHeadGraph, which runs as a
        \\        graph against the chosen ComputeBackend
        \\
    , .{});
}

test "parseArgs collects repeated labels" {
    const allocator = std.testing.allocator;
    var opts = try parseArgs(allocator, &.{
        "/tmp/model",
        "John works at Google",
        "--label",
        "person",
        "--label",
        "organization",
    });
    defer opts.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("John works at Google", opts.text);
    try std.testing.expectEqual(@as(usize, 2), opts.labels.items.len);
    try std.testing.expectEqualStrings("person", opts.labels.items[0]);
    try std.testing.expectEqualStrings("organization", opts.labels.items[1]);
}
