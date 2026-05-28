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
const mlx_backend = if (build_options.enable_mlx) @import("backends/mlx.zig") else struct {};
const metal_runtime = if (build_options.enable_metal) @import("backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }
};
const c_file = @import("util/c_file.zig");
const graph_runtime = @import("graph/runtime.zig");
const graph_executor_stats = @import("graph/executor_stats.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_guard = @import("native_backend_guard.zig");
const sparse_embedding_mod = @import("pipelines/sparse_embedding.zig");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    onnx,
    native,
    metal,
    mlx,
    cuda,
};

const Modality = enum {
    text,
    image,
    audio,
};

const InputRef = struct {
    modality: Modality,
    index: usize,
};

const Options = struct {
    model_dir: []const u8,
    backend: BackendChoice = .auto,
    texts: std.ArrayListUnmanaged([]const u8) = .empty,
    image_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    audio_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    order: std.ArrayListUnmanaged(InputRef) = .empty,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    print_timing: bool = false,

    fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        self.image_paths.deinit(allocator);
        self.audio_paths.deinit(allocator);
        self.order.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    if (opts.order.items.len == 0) {
        printUsage();
        return error.InvalidArguments;
    }

    const started_at = std.Io.Timestamp.now(io, .awake);
    try ensureRequestedMetalHostedBackendAvailable(opts.backend);

    // Forward the harness Io into the SessionManager so the GEMM backend
    // (NativeCompute) can dispatch parallel work via linalg.sgemm*Io.  Falls
    // back to the process-wide futex pool when null.
    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);
    session_manager.graph_runtime_strategy = opts.graph_runtime_strategy;
    if (opts.graph_runtime_strategy == null) {
        graph_executor_stats.printBypass("inference.embed", "embedding_pipeline_direct_runtime");
    }

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const loaded_model_at = std.Io.Timestamp.now(io, .awake);
    if (model.manifest.hasCapability("sparse")) {
        if (opts.image_paths.items.len > 0 or opts.audio_paths.items.len > 0) {
            print("error: sparse embedding models only support --text inputs\n", .{});
            return error.SparseEmbeddingRequiresTextInput;
        }

        try model.ensureEmbeddingAssets(opts.texts.items.len > 0, false, false);
        const ensured_assets_at = std.Io.Timestamp.now(io, .awake);
        var sparse_pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
            .allocator = allocator,
            .session = model.session,
            .tok = model.getTokenizer(),
            .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
        };
        const sparse_embeddings = try sparse_pipeline.embed(opts.texts.items);
        const embedded_text_at = std.Io.Timestamp.now(io, .awake);
        defer freeSparseEmbeddings(allocator, sparse_embeddings);

        try writeSparseResultJson(
            allocator,
            opts.model_dir,
            opts.order.items,
            sparse_embeddings,
        );
        const finished_at = std.Io.Timestamp.now(io, .awake);
        if (opts.print_timing) {
            print(
                "timing_ms: load_model={d} ensure_assets={d} text={d} write_json={d} total={d}\n",
                .{
                    durationMillis(started_at, loaded_model_at),
                    durationMillis(loaded_model_at, ensured_assets_at),
                    durationMillis(ensured_assets_at, embedded_text_at),
                    durationMillis(embedded_text_at, finished_at),
                    durationMillis(started_at, finished_at),
                },
            );
        }
        return;
    }

    try model.ensureEmbeddingAssets(
        opts.texts.items.len > 0,
        opts.image_paths.items.len > 0,
        opts.audio_paths.items.len > 0,
    );
    const ensured_assets_at = std.Io.Timestamp.now(io, .awake);
    var pipeline = model.embeddingPipeline(allocator);
    pipeline.print_timing = opts.print_timing;

    const text_embeddings = if (opts.texts.items.len > 0)
        try pipeline.embed(opts.texts.items)
    else
        try allocator.alloc([]f32, 0);
    const embedded_text_at = std.Io.Timestamp.now(io, .awake);
    defer freeEmbeddings(allocator, text_embeddings);

    const image_bytes = try loadFiles(allocator, opts.image_paths.items);
    const loaded_images_at = std.Io.Timestamp.now(io, .awake);
    defer freeOwnedBytes(allocator, image_bytes);
    const image_embeddings = if (image_bytes.len > 0)
        try pipeline.embedImages(image_bytes)
    else
        try allocator.alloc([]f32, 0);
    const embedded_images_at = std.Io.Timestamp.now(io, .awake);
    defer freeEmbeddings(allocator, image_embeddings);

    const audio_bytes = try loadFiles(allocator, opts.audio_paths.items);
    const loaded_audio_at = std.Io.Timestamp.now(io, .awake);
    defer freeOwnedBytes(allocator, audio_bytes);
    const audio_embeddings = if (audio_bytes.len > 0)
        try pipeline.embedAudio(audio_bytes)
    else
        try allocator.alloc([]f32, 0);
    const embedded_audio_at = std.Io.Timestamp.now(io, .awake);
    defer freeEmbeddings(allocator, audio_embeddings);

    try writeResultJson(
        allocator,
        opts.model_dir,
        opts.order.items,
        text_embeddings,
        image_embeddings,
        audio_embeddings,
    );
    const finished_at = std.Io.Timestamp.now(io, .awake);
    if (opts.print_timing) {
        print(
            "timing_ms: load_model={d} ensure_assets={d} text={d} image_load={d} image={d} audio_load={d} audio={d} write_json={d} total={d}\n",
            .{
                durationMillis(started_at, loaded_model_at),
                durationMillis(loaded_model_at, ensured_assets_at),
                durationMillis(ensured_assets_at, embedded_text_at),
                durationMillis(embedded_text_at, loaded_images_at),
                durationMillis(loaded_images_at, embedded_images_at),
                durationMillis(embedded_images_at, loaded_audio_at),
                durationMillis(loaded_audio_at, embedded_audio_at),
                durationMillis(embedded_audio_at, finished_at),
                durationMillis(started_at, finished_at),
            },
        );
    }
}

fn parseArgs(allocator: std.mem.Allocator, args: []const []const u8) !Options {
    if (args.len < 1) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
    };
    errdefer opts.deinit(allocator);

    var i: usize = 1;
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
        } else if (std.mem.eql(u8, arg, "--print-timing")) {
            opts.print_timing = true;
        } else if (std.mem.eql(u8, arg, "--text")) {
            i += 1;
            if (i >= args.len) return error.MissingTextValue;
            const idx = opts.texts.items.len;
            try opts.texts.append(allocator, args[i]);
            try opts.order.append(allocator, .{ .modality = .text, .index = idx });
        } else if (std.mem.eql(u8, arg, "--image")) {
            i += 1;
            if (i >= args.len) return error.MissingImageValue;
            const idx = opts.image_paths.items.len;
            try opts.image_paths.append(allocator, args[i]);
            try opts.order.append(allocator, .{ .modality = .image, .index = idx });
        } else if (std.mem.eql(u8, arg, "--audio")) {
            i += 1;
            if (i >= args.len) return error.MissingAudioValue;
            const idx = opts.audio_paths.items.len;
            try opts.audio_paths.append(allocator, args[i]);
            try opts.order.append(allocator, .{ .modality = .audio, .index = idx });
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn loadFiles(allocator: std.mem.Allocator, paths: []const []const u8) ![][]const u8 {
    const out = try allocator.alloc([]const u8, paths.len);
    for (out) |*bytes| bytes.* = &.{};
    errdefer {
        for (out) |bytes| {
            if (bytes.len > 0) allocator.free(bytes);
        }
        allocator.free(out);
    }

    var loaded: usize = 0;
    errdefer {
        for (out[0..loaded]) |bytes| allocator.free(bytes);
    }

    for (paths, 0..) |path, i| {
        out[i] = try c_file.readFile(allocator, path);
        loaded += 1;
    }
    return out;
}

fn freeOwnedBytes(allocator: std.mem.Allocator, items: [][]const u8) void {
    for (items) |bytes| allocator.free(bytes);
    allocator.free(items);
}

fn freeEmbeddings(allocator: std.mem.Allocator, embeddings: [][]f32) void {
    for (embeddings) |emb| allocator.free(emb);
    allocator.free(embeddings);
}

fn freeSparseEmbeddings(allocator: std.mem.Allocator, embeddings: []sparse_embedding_mod.SparseVector) void {
    for (embeddings) |*emb| emb.deinit(allocator);
    allocator.free(embeddings);
}

fn nanosToMillis(nanos: i128) u64 {
    return @intCast(@divTrunc(nanos, std.time.ns_per_ms));
}

fn durationMillis(from: std.Io.Timestamp, to: std.Io.Timestamp) u64 {
    return nanosToMillis(std.Io.Timestamp.durationTo(from, to).nanoseconds);
}

fn writeResultJson(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    order: []const InputRef,
    text_embeddings: [][]f32,
    image_embeddings: [][]f32,
    audio_embeddings: [][]f32,
) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"modalities\":[");
    for (order, 0..) |item, i| {
        if (i > 0) try buf.append(allocator, ',');
        const name = switch (item.modality) {
            .text => "text",
            .image => "image",
            .audio => "audio",
        };
        try jsonEncodeString(&buf, allocator, name);
    }
    try buf.appendSlice(allocator, "],\"embeddings\":[");
    for (order, 0..) |item, i| {
        if (i > 0) try buf.append(allocator, ',');
        const emb = switch (item.modality) {
            .text => text_embeddings[item.index],
            .image => image_embeddings[item.index],
            .audio => audio_embeddings[item.index],
        };
        try appendEmbeddingJson(&buf, allocator, emb);
    }
    try buf.appendSlice(allocator, "]}\n");

    print("{s}", .{buf.items});
}

fn writeSparseResultJson(
    allocator: std.mem.Allocator,
    model_name: []const u8,
    order: []const InputRef,
    text_embeddings: []const sparse_embedding_mod.SparseVector,
) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"modalities\":[");
    for (order, 0..) |item, i| {
        if (i > 0) try buf.append(allocator, ',');
        std.debug.assert(item.modality == .text);
        try jsonEncodeString(&buf, allocator, "text");
    }
    try buf.appendSlice(allocator, "],\"embeddings\":[");
    for (order, 0..) |item, i| {
        if (i > 0) try buf.append(allocator, ',');
        try appendSparseEmbeddingJson(&buf, allocator, text_embeddings[item.index]);
    }
    try buf.appendSlice(allocator, "]}\n");

    print("{s}", .{buf.items});
}

fn appendEmbeddingJson(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, emb: []const f32) !void {
    try buf.append(allocator, '[');
    for (emb, 0..) |value, i| {
        if (i > 0) try buf.append(allocator, ',');
        const num = try std.fmt.allocPrint(allocator, "{d}", .{value});
        defer allocator.free(num);
        try buf.appendSlice(allocator, num);
    }
    try buf.append(allocator, ']');
}

fn appendSparseEmbeddingJson(
    buf: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    emb: sparse_embedding_mod.SparseVector,
) !void {
    try buf.appendSlice(allocator, "{\"indices\":[");
    for (emb.indices, 0..) |idx, i| {
        if (i > 0) try buf.append(allocator, ',');
        const num = try std.fmt.allocPrint(allocator, "{d}", .{idx});
        defer allocator.free(num);
        try buf.appendSlice(allocator, num);
    }
    try buf.appendSlice(allocator, "],\"values\":[");
    for (emb.values, 0..) |value, i| {
        if (i > 0) try buf.append(allocator, ',');
        const num = try std.fmt.allocPrint(allocator, "{d}", .{value});
        defer allocator.free(num);
        try buf.appendSlice(allocator, num);
    }
    try buf.appendSlice(allocator, "]}");
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
    if (std.mem.eql(u8, value, "onnx")) return .onnx;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "mlx")) return .mlx;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal and build_options.enable_mlx)
            &.{ backends.BackendType.onnx, backends.BackendType.metal, backends.BackendType.mlx, backends.BackendType.native }
        else if (build_options.enable_metal)
            &.{ backends.BackendType.onnx, backends.BackendType.metal, backends.BackendType.native }
        else if (build_options.enable_mlx)
            &.{ backends.BackendType.onnx, backends.BackendType.mlx, backends.BackendType.native }
        else
            &.{ backends.BackendType.onnx, backends.BackendType.native },
        .onnx => &.{backends.BackendType.onnx},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .mlx => if (build_options.enable_mlx) &.{backends.BackendType.mlx} else &.{backends.BackendType.native},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
    };
}

fn ensureRequestedMetalHostedBackendAvailable(choice: BackendChoice) !void {
    if (choice != .metal and choice != .mlx and choice != .cuda) return;
    if (choice == .cuda) {
        if (!build_options.enable_cuda) return error.CudaNotEnabled;
        return;
    }
    if (choice == .metal) {
        if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
            native_backend_guard.printFailure(failure);
            return native_backend_guard.raise(failure);
        }
        return;
    }
    const mlx_metal_available = if (build_options.enable_mlx) mlx_backend.metalDeviceAvailable() else false;
    if (native_backend_guard.checkMlx(build_options.enable_mlx, mlx_metal_available)) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
    }
}

fn printUsage() void {
    print(
        \\usage: termite embed <model-dir> [--backend auto|onnx|native|metal|mlx|cuda] [--graph-runtime interpreter|partitioned|compiled|compiled-required] [--print-timing] [--text <text>]... [--image <path>]... [--audio <path>]...
        \\  Runs local embedding and prints a JSON response to stdout.
        \\  Input order is preserved across repeated --text/--image/--audio flags.
        \\  graph-runtime controls imported static graph execution; default is environment fallback, then interpreter.
        \\  Benchmark gates: TERMITE_GRAPH_RUNTIME_FAIL_CLOSED=1, TERMITE_GRAPH_EXECUTOR_STATS=1, TERMITE_GRAPH_PARTITION_REPORT=1.
        \\  --print-timing prints phase timings to stderr.
        \\
    , .{});
}

test "parseArgs preserves multimodal input order" {
    var opts = try parseArgs(std.testing.allocator, &.{
        "/tmp/model",
        "--text",
        "hello",
        "--image",
        "/tmp/a.png",
        "--audio",
        "/tmp/a.wav",
        "--text",
        "world",
        "--backend",
        "mlx",
        "--graph-runtime",
        "partitioned",
        "--print-timing",
    });
    defer opts.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqual(BackendChoice.mlx, opts.backend);
    try std.testing.expectEqual(graph_runtime.Strategy.partitioned, opts.graph_runtime_strategy.?);
    try std.testing.expect(opts.print_timing);
    try std.testing.expectEqual(@as(usize, 2), opts.texts.items.len);
    try std.testing.expectEqual(@as(usize, 1), opts.image_paths.items.len);
    try std.testing.expectEqual(@as(usize, 1), opts.audio_paths.items.len);
    try std.testing.expectEqual(@as(usize, 4), opts.order.items.len);
    try std.testing.expectEqual(Modality.text, opts.order.items[0].modality);
    try std.testing.expectEqual(Modality.image, opts.order.items[1].modality);
    try std.testing.expectEqual(Modality.audio, opts.order.items[2].modality);
    try std.testing.expectEqual(Modality.text, opts.order.items[3].modality);
    try std.testing.expectEqual(@as(usize, 1), opts.order.items[3].index);
}

test "appendSparseEmbeddingJson writes cli sparse embedding shape" {
    const allocator = std.testing.allocator;
    var indices = [_]u32{ 2, 17, 42 };
    var values = [_]f32{ 0.25, 1.5, 3.0 };
    const emb = sparse_embedding_mod.SparseVector{
        .indices = &indices,
        .values = &values,
    };

    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try appendSparseEmbeddingJson(&buf, allocator, emb);
    try std.testing.expectEqualStrings(
        "{\"indices\":[2,17,42],\"values\":[0.25,1.5,3]}",
        buf.items,
    );
}
