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
const platform = @import("antfly_platform");
const backends = @import("backends/backends.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_choice = @import("native_backend_choice.zig");
const reranking = @import("pipelines/reranking.zig");
const qwen3vl_multimodal_reranker = @import("pipelines/qwen3vl_multimodal_reranker.zig");
const qwen3vl_reranker = @import("architectures/qwen3vl_reranker.zig");
const session_factory = @import("architectures/session_factory.zig");

const print = std.debug.print;

const Options = struct {
    model_dir: []const u8,
    query: []const u8 = "",
    documents: std.ArrayListUnmanaged([]const u8) = .empty,
    image_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    backend: native_backend_choice.Choice = .auto,
    qwen3vl_qualification_json: ?[]const u8 = null,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.documents.deinit(allocator);
        self.image_paths.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    try native_backend_choice.validate(opts.backend);
    native_backend_choice.configureSessionPreference(&session_manager, opts.backend);

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    if (opts.image_paths.items.len > 0) {
        return runQwen3VlMultimodal(allocator, io, opts, model);
    }
    var pipeline = model.rerankingPipeline(allocator);
    var qualification_trace: ?reranking.GenerativeQualificationTrace = if (opts.qwen3vl_qualification_json != null) blk: {
        if (!model.manifest.isQwen3VlReranker()) return error.InvalidQualificationModel;
        break :blk reranking.GenerativeQualificationTrace.init(allocator);
    } else null;
    defer if (qualification_trace) |*trace| trace.deinit();
    if (qualification_trace) |*trace| pipeline.generative_qualification_trace = trace;
    const scores = try pipeline.rerank(opts.query, opts.documents.items);
    defer allocator.free(scores);

    try writeRerankJson(allocator, opts, scores);
    if (opts.qwen3vl_qualification_json) |path| {
        const trace = if (qualification_trace) |*value| value else return error.IncompleteQualificationTrace;
        try writeQwen3VlQualificationJson(
            allocator,
            io,
            path,
            opts,
            model.session.backend(),
            trace,
            scores,
        );
    }
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !Options {
    if (args.len < 1) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{ .model_dir = args[0] };
    errdefer opts.deinit(allocator);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--query")) {
            i += 1;
            if (i >= args.len) return error.MissingQueryValue;
            opts.query = args[i];
        } else if (std.mem.eql(u8, arg, "--doc")) {
            i += 1;
            if (i >= args.len) return error.MissingDocumentValue;
            try opts.documents.append(allocator, args[i]);
        } else if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = native_backend_choice.parse(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--image")) {
            i += 1;
            if (i >= args.len or args[i].len == 0) return error.MissingImagePath;
            try opts.image_paths.append(allocator, args[i]);
        } else if (std.mem.eql(u8, arg, "--qwen3vl-qualification-json")) {
            i += 1;
            if (i >= args.len or args[i].len == 0) return error.MissingQualificationPath;
            opts.qwen3vl_qualification_json = args[i];
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    if (opts.query.len == 0 or opts.documents.items.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }
    if (opts.image_paths.items.len > 0 and opts.documents.items.len != 1) {
        return error.MultimodalRerankerRequiresSingleDocument;
    }

    return opts;
}

fn runQwen3VlMultimodal(
    allocator: std.mem.Allocator,
    io: std.Io,
    opts: Options,
    model: *model_manager_mod.LoadedModel,
) !void {
    if (!model.manifest.isQwen3VlRerankerGgufBundle()) {
        return error.InvalidQualificationModel;
    }
    if (opts.image_paths.items.len == 0 or opts.image_paths.items.len > 8 or opts.documents.items.len != 1) {
        return error.InvalidMultimodalRerankerInput;
    }
    const projector_path = model.manifest.gguf_projector_path orelse
        return error.MissingQwen3VlProjector;
    const gpt_config = session_factory.getGptConfig(model.session) orelse
        return error.InvalidQualificationModel;

    const image_bytes = try allocator.alloc([]const u8, opts.image_paths.items.len);
    defer allocator.free(image_bytes);
    var image_count: usize = 0;
    defer for (image_bytes[0..image_count]) |bytes| allocator.free(bytes);
    for (opts.image_paths.items, 0..) |path, index| {
        image_bytes[index] = try std.Io.Dir.cwd().readFileAlloc(
            io,
            path,
            allocator,
            .limited(64 * 1024 * 1024),
        );
        image_count += 1;
    }

    var document_content = std.ArrayListUnmanaged(u8).empty;
    defer document_content.deinit(allocator);
    for (image_bytes) |_| {
        try document_content.appendSlice(allocator, qwen3vl_reranker.image_marker);
    }
    try document_content.appendSlice(allocator, opts.documents.items[0]);

    const execution_mutex = model.targetInferenceExecutionMutex();
    if (execution_mutex) |mutex| platform.sync.lockYielding(mutex);
    defer if (execution_mutex) |mutex| mutex.unlock();
    var cb = try session_factory.getComputeBackend(model.session, allocator);
    defer cb.deinit();
    var pipeline = try qwen3vl_multimodal_reranker.Pipeline.init(
        allocator,
        &cb,
        model.getTokenizer(),
        gpt_config,
        projector_path,
        .{
            .max_length = @min(model.manifest.maxTextSequenceLength(), qwen3vl_reranker.default_max_length),
        },
    );
    var qualification_trace: ?qwen3vl_multimodal_reranker.QualificationTrace =
        if (opts.qwen3vl_qualification_json != null)
            qwen3vl_multimodal_reranker.QualificationTrace.init(allocator)
        else
            null;
    defer if (qualification_trace) |*trace| trace.deinit();
    if (qualification_trace) |*trace| pipeline.qualification_trace = trace;
    const result = try pipeline.scoreDocument(opts.query, document_content.items, image_bytes);
    const scores = [_]f32{result.score};
    try writeRerankJson(allocator, opts, &scores);
    if (opts.qwen3vl_qualification_json) |path| {
        const trace = if (qualification_trace) |*value| value else return error.IncompleteQualificationTrace;
        const encoded = try std.json.Stringify.valueAlloc(allocator, .{
            .schema = "antfly.qwen3vl.multimodal_reranker_qualification.v1",
            .model_dir = opts.model_dir,
            .backend = @tagName(model.session.backend()),
            .instruction = qwen3vl_reranker.default_instruction,
            .query = opts.query,
            .document = opts.documents.items[0],
            .images = opts.image_paths.items,
            .rendered_prompt = trace.rendered_prompt,
            .placeholder_token_ids = trace.placeholder_token_ids,
            .expanded_token_ids = trace.expanded_token_ids,
            .mrope_positions = trace.mrope_positions,
            .visual_token_mask = trace.visual_token_mask,
            .visual_tokens = result.visual_tokens,
            .prompt_tokens = result.prompt_tokens,
            .raw_logit = result.raw_logit,
            .score = result.score,
        }, .{});
        defer allocator.free(encoded);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = encoded });
    }
}

fn writeQwen3VlQualificationJson(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    opts: Options,
    backend: backends.BackendType,
    trace: *const reranking.GenerativeQualificationTrace,
    scores: []const f32,
) !void {
    if (trace.pairs.items.len != scores.len or trace.raw_logits.items.len != scores.len) {
        return error.IncompleteQualificationTrace;
    }
    const PairEvidence = struct {
        rendered_prompt: []const u8,
        token_ids: []const i32,
    };
    const pairs = try allocator.alloc(PairEvidence, trace.pairs.items.len);
    defer allocator.free(pairs);
    for (trace.pairs.items, pairs) |source, *destination| {
        destination.* = .{
            .rendered_prompt = source.rendered_prompt,
            .token_ids = source.token_ids,
        };
    }
    const encoded = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.qwen3vl.reranker_qualification.v1",
        .model_dir = opts.model_dir,
        .backend = @tagName(backend),
        .instruction = qwen3vlInstruction(),
        .query = opts.query,
        .documents = opts.documents.items,
        .pairs = pairs,
        .raw_logits = trace.raw_logits.items,
        .scores = scores,
    }, .{});
    defer allocator.free(encoded);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = encoded });
}

fn qwen3vlInstruction() []const u8 {
    return @import("architectures/qwen3vl_reranker.zig").default_instruction;
}

fn writeRerankJson(allocator: std.mem.Allocator, opts: Options, scores: []const f32) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, opts.model_dir);
    try buf.appendSlice(allocator, ",\"query\":");
    try jsonEncodeString(&buf, allocator, opts.query);
    try buf.appendSlice(allocator, ",\"scores\":[");
    for (scores, 0..) |score, idx| {
        if (idx > 0) try buf.append(allocator, ',');
        const fragment = try std.fmt.allocPrint(allocator, "{{\"index\":{d},\"score\":{d}}}", .{ idx, score });
        defer allocator.free(fragment);
        try buf.appendSlice(allocator, fragment);
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
        \\usage: antfly inference rerank <model-dir> --query <query> --doc <document>... [--image <path>...] [--backend auto|onnx|native|metal|mlx|cuda]
        \\  Runs local reranking and prints JSON scores in document order.
        \\  Qwen3-VL accepts up to eight --image values with exactly one --doc; images are inserted before that document in argument order.
        \\  --qwen3vl-qualification-json <path> emits offline Qwen3-VL prompt/token/raw-logit evidence.
        \\
    , .{});
}

test "parseArgs collects query documents and backend" {
    const allocator = std.testing.allocator;
    var opts = try parseArgs(allocator, &.{
        "/tmp/model",
        "--query",
        "what is cuda",
        "--doc",
        "cuda is a gpu platform",
        "--doc",
        "unrelated",
        "--backend",
        "native",
        "--qwen3vl-qualification-json",
        "/tmp/qwen3vl-reranker.json",
    });
    defer opts.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("what is cuda", opts.query);
    try std.testing.expectEqual(@as(usize, 2), opts.documents.items.len);
    try std.testing.expectEqualStrings("cuda is a gpu platform", opts.documents.items[0]);
    try std.testing.expectEqual(native_backend_choice.Choice.native, opts.backend);
    try std.testing.expectEqualStrings("/tmp/qwen3vl-reranker.json", opts.qwen3vl_qualification_json.?);
}

test "parseArgs bounds Qwen3-VL multimodal reranking to one ordered document" {
    const allocator = std.testing.allocator;
    var opts = try parseArgs(allocator, &.{
        "/tmp/model",
        "--query",
        "invoice total",
        "--image",
        "/tmp/page-1.png",
        "--image",
        "/tmp/page-2.png",
        "--doc",
        "two page invoice",
    });
    defer opts.deinit(allocator);
    try std.testing.expectEqualSlices(
        []const u8,
        &.{ "/tmp/page-1.png", "/tmp/page-2.png" },
        opts.image_paths.items,
    );
    try std.testing.expectError(
        error.MultimodalRerankerRequiresSingleDocument,
        parseArgs(allocator, &.{
            "/tmp/model",
            "--query",
            "invoice total",
            "--image",
            "/tmp/page.png",
            "--doc",
            "first",
            "--doc",
            "second",
        }),
    );
}
