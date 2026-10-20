// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const audio_mod = @import("pipelines/audio.zig");
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
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
const embedding_mod = @import("pipelines/embedding.zig");
const gemma2_model = @import("models/embedding_gemma2.zig");
const data_uri = @import("antfly_scraping").data_uri;

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    metal,
    cuda,
};

const Modality = enum {
    text,
    image,
    audio,
    video,
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
    video_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    order: std.ArrayListUnmanaged(InputRef) = .empty,
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    print_timing: bool = false,
    combined: bool = false,
    content_json_path: ?[]const u8 = null,
    task_type: ?[]const u8 = null,
    dimensions: ?usize = null,
    group: bool = false,
    title: ?[]const u8 = null,

    pub fn deinit(self: *Options, allocator: std.mem.Allocator) void {
        self.texts.deinit(allocator);
        self.image_paths.deinit(allocator);
        self.audio_paths.deinit(allocator);
        self.video_paths.deinit(allocator);
        self.order.deinit(allocator);
    }
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    var opts = try parseArgs(allocator, args);
    defer opts.deinit(allocator);

    // This one-shot command retains its inputs until native calls finish.
    const control = @import("execution_control.zig").InferenceExecutionControl{ .io = io, .native_call_policy = .in_process };

    if (opts.order.items.len == 0 and opts.content_json_path == null) {
        printUsage();
        return error.InvalidArguments;
    }

    var stdout_buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buffer);

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

    var model_handle = try model_manager.acquireFromDirWithControl(opts.model_dir, control);
    defer model_handle.release();
    const model = model_handle.get();
    const loaded_model_at = std.Io.Timestamp.now(io, .awake);
    if (model.manifest.embedding_style == .embedding_gemma2) {
        if (model.session.backend() == .cuda or opts.content_json_path != null) {
            try model.verifyEmbeddingIdentity();
            try runEmbeddingGemma2(allocator, &stdout.interface, model, &opts, control);
            try model.verifyEmbeddingIdentity();
            return;
        }
        return embedGemma2(allocator, io, &stdout.interface, model, opts);
    }
    if (opts.combined or opts.group or opts.title != null or opts.content_json_path != null or opts.task_type != null or opts.dimensions != null) return error.UnsupportedEmbeddingOptions;
    if (model.manifest.hasCapability("sparse")) {
        if (opts.image_paths.items.len > 0 or opts.audio_paths.items.len > 0 or opts.video_paths.items.len > 0) {
            print("error: sparse embedding models only support --text inputs\n", .{});
            return error.SparseEmbeddingRequiresTextInput;
        }

        {
            try model.lockEmbeddingAssetsWithControl(control);
            defer model.unlockEmbeddingAssets();
            try model.ensureEmbeddingAssetsLockedWithControl(opts.texts.items.len > 0, false, false, control);
        }
        const ensured_assets_at = std.Io.Timestamp.now(io, .awake);
        var sparse_pipeline = sparse_embedding_mod.SparseEmbeddingPipeline{
            .allocator = allocator,
            .session = model.session,
            .tok = model.getTokenizer(),
            .execution_control = control,
            .config = sparse_embedding_mod.SparseEmbeddingConfig.fromManifest(&model.manifest),
        };
        const sparse_embeddings = try sparse_pipeline.embed(opts.texts.items);
        const embedded_text_at = std.Io.Timestamp.now(io, .awake);
        defer freeSparseEmbeddings(allocator, sparse_embeddings);

        try writeSparseResultJson(
            allocator,
            &stdout.interface,
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

    const has_primary_inputs = opts.texts.items.len > 0 or opts.image_paths.items.len > 0;
    if (opts.video_paths.items.len > 0) return error.UnsupportedVideoBackend;
    var asset_lease = model.acquireEmbeddingAssetLease(opts.audio_paths.items.len > 0);
    defer asset_lease.release();
    var pipeline = initial_pipeline: {
        model.lockEmbeddingAssets();
        defer model.unlockEmbeddingAssets();
        if (opts.audio_paths.items.len > 0)
            try model.ensureAudioEmbeddingAssetsLockedWithControl(control)
        else
            try model.ensurePrimaryEmbeddingAssetsLockedWithControl(opts.texts.items.len > 0, opts.image_paths.items.len > 0, control);
        break :initial_pipeline model.embeddingPipelineLocked(allocator);
    };
    errdefer if (opts.audio_paths.items.len > 0) releaseAudioEmbeddingAssets(model);
    const ensured_initial_assets_at = std.Io.Timestamp.now(io, .awake);
    pipeline.print_timing = opts.print_timing;
    pipeline.execution_control = control;

    const audio_bytes = try loadFiles(allocator, opts.audio_paths.items);
    const loaded_audio_at = std.Io.Timestamp.now(io, .awake);
    defer freeOwnedBytes(allocator, audio_bytes);
    const audio_embeddings = if (audio_bytes.len > 0) audio: {
        defer releaseAudioEmbeddingAssets(model);
        break :audio try pipeline.embedAudio(audio_bytes);
    } else try allocator.alloc([]f32, 0);
    const embedded_audio_at = std.Io.Timestamp.now(io, .awake);
    defer freeEmbeddings(allocator, audio_embeddings);

    if (opts.audio_paths.items.len > 0 and has_primary_inputs) {
        asset_lease.downgradeExclusiveToShared();
        model.lockEmbeddingAssets();
        defer model.unlockEmbeddingAssets();
        try model.ensurePrimaryEmbeddingAssetsLockedWithControl(opts.texts.items.len > 0, opts.image_paths.items.len > 0, control);
        model.bindEmbeddingPipelineAssetsLocked(&pipeline);
    } else if (opts.audio_paths.items.len > 0) {
        asset_lease.release();
    }
    const ensured_primary_assets_at = std.Io.Timestamp.now(io, .awake);

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
    asset_lease.release();

    try writeResultJson(
        allocator,
        &stdout.interface,
        opts.model_dir,
        opts.order.items,
        text_embeddings,
        image_embeddings,
        audio_embeddings,
    );
    const finished_at = std.Io.Timestamp.now(io, .awake);
    if (opts.print_timing) {
        print(
            "timing_ms: load_model={d} ensure_initial_assets={d} audio_load={d} audio={d} ensure_primary_assets={d} text={d} image_load={d} image={d} write_json={d} total={d}\n",
            .{
                durationMillis(started_at, loaded_model_at),
                durationMillis(loaded_model_at, ensured_initial_assets_at),
                durationMillis(ensured_initial_assets_at, loaded_audio_at),
                durationMillis(loaded_audio_at, embedded_audio_at),
                durationMillis(embedded_audio_at, ensured_primary_assets_at),
                durationMillis(ensured_primary_assets_at, embedded_text_at),
                durationMillis(embedded_text_at, loaded_images_at),
                durationMillis(loaded_images_at, embedded_images_at),
                durationMillis(embedded_images_at, finished_at),
                durationMillis(started_at, finished_at),
            },
        );
    }
}

fn releaseAudioEmbeddingAssets(model: *model_manager_mod.LoadedModel) void {
    model.lockEmbeddingAssets();
    defer model.unlockEmbeddingAssets();
    model.releaseAudioEmbeddingAssetsLocked();
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
        } else if (std.mem.eql(u8, arg, "--combined")) {
            opts.combined = true;
        } else if (std.mem.eql(u8, arg, "--content-json")) {
            i += 1;
            if (i >= args.len) return error.MissingContentJsonValue;
            opts.content_json_path = args[i];
        } else if (std.mem.eql(u8, arg, "--group")) {
            opts.group = true;
        } else if (std.mem.eql(u8, arg, "--title")) {
            i += 1;
            if (i >= args.len) return error.MissingTitleValue;
            opts.title = args[i];
        } else if (std.mem.eql(u8, arg, "--task-type")) {
            i += 1;
            if (i >= args.len) return error.MissingTaskTypeValue;
            _ = try @import("architectures/embedding_gemma2.zig").taskPrefix(args[i]);
            opts.task_type = args[i];
        } else if (std.mem.eql(u8, arg, "--dimensions")) {
            i += 1;
            if (i >= args.len) return error.MissingDimensionsValue;
            opts.dimensions = try parseEmbeddingDimensions(args[i]);
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
        } else if (std.mem.eql(u8, arg, "--video")) {
            i += 1;
            if (i >= args.len) return error.MissingVideoValue;
            const idx = opts.video_paths.items.len;
            try opts.video_paths.append(allocator, args[i]);
            try opts.order.append(allocator, .{ .modality = .video, .index = idx });
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn runEmbeddingGemma2(allocator: std.mem.Allocator, writer: *std.Io.Writer, model: *model_manager_mod.LoadedModel, opts: *const Options, control: @import("execution_control.zig").InferenceExecutionControl) !void {
    if (opts.content_json_path != null and opts.order.items.len > 0) return error.ConflictingEmbeddingInputs;
    var pipeline = model.embeddingPipeline(allocator);
    pipeline.print_timing = opts.print_timing;
    pipeline.execution_control = control;
    var dimensions = opts.dimensions;
    var task_type = opts.task_type orelse "RETRIEVAL_DOCUMENT";
    pipeline.config.text_prefix = try gemma2_model.taskPrefix(task_type);
    var title_prefix: ?[]u8 = null;
    defer if (title_prefix) |prefix| allocator.free(prefix);
    if (opts.title) |title| {
        if (!std.mem.eql(u8, opts.task_type orelse "RETRIEVAL_DOCUMENT", "RETRIEVAL_DOCUMENT") or opts.texts.items.len == 0) return error.InvalidEmbeddingTitle;
        title_prefix = try std.fmt.allocPrint(allocator, "title: {s} | text: ", .{title});
        pipeline.config.text_prefix = title_prefix.?;
    }
    var groups = std.ArrayListUnmanaged(embedding_mod.EmbeddingContentInput).empty;
    defer {
        for (groups.items) |group| allocator.free(group.content);
        groups.deinit(allocator);
    }
    var owned_media = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned_media.items) |bytes| allocator.free(bytes);
        owned_media.deinit(allocator);
    }
    var parsed: ?std.json.Parsed(std.json.Value) = null;
    defer if (parsed) |*value| value.deinit();
    if (opts.content_json_path) |path| {
        const json = try c_file.readFileMax(allocator, path, 64 * 1024 * 1024);
        defer allocator.free(json);
        parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{ .allocate = .alloc_always });
        const root = parsed.?.value;
        const input = if (root == .object) root.object.get("input") orelse root else root;
        if (root == .object) {
            if (root.object.get("dimensions")) |value| {
                if (value != .integer or value.integer < 1 or value.integer > 768) return error.InvalidEmbeddingDimensions;
                const body_dimensions: usize = @intCast(value.integer);
                if (dimensions) |flag_dimensions| if (flag_dimensions != body_dimensions) return error.ConflictingEmbeddingDimensions;
                dimensions = body_dimensions;
            }
            if (root.object.get("task_type")) |task| {
                if (task != .string) return error.InvalidEmbeddingTaskType;
                if (opts.task_type) |flag_task| if (!std.mem.eql(u8, flag_task, task.string)) return error.ConflictingEmbeddingTaskType;
                task_type = task.string;
                pipeline.config.text_prefix = try gemma2_model.taskPrefix(task_type);
            }
        }
        if (input == .array) {
            for (input.array.items) |item| try appendCliContentInput(allocator, &groups, &owned_media, item);
        } else try appendCliContentInput(allocator, &groups, &owned_media, input);
    } else {
        try owned_media.ensureUnusedCapacity(allocator, opts.image_paths.items.len + opts.audio_paths.items.len + opts.video_paths.items.len);
        const images = try loadFiles(allocator, opts.image_paths.items);
        defer allocator.free(images);
        for (images) |bytes| owned_media.appendAssumeCapacity(@constCast(bytes));
        const audio = try loadFiles(allocator, opts.audio_paths.items);
        defer allocator.free(audio);
        for (audio) |bytes| owned_media.appendAssumeCapacity(@constCast(bytes));
        const videos = try loadFilesBounded(allocator, opts.video_paths.items, 256 * 1024 * 1024);
        defer allocator.free(videos);
        for (videos) |bytes| owned_media.appendAssumeCapacity(@constCast(bytes));
        const parts = try allocator.alloc(embedding_mod.EmbeddingContentPart, opts.order.items.len);
        var parts_owned = true;
        defer if (parts_owned) allocator.free(parts);
        for (opts.order.items, 0..) |item, index| parts[index] = switch (item.modality) {
            .text => .{ .text = opts.texts.items[item.index] },
            .image => .{ .image = images[item.index] },
            .audio => .{ .audio = .{ .bytes = audio[item.index] } },
            .video => .{ .video = videos[item.index] },
        };
        if (opts.combined or opts.group) {
            try groups.append(allocator, .{ .content = parts });
            parts_owned = false;
        } else {
            for (parts) |part| {
                const single = try allocator.alloc(embedding_mod.EmbeddingContentPart, 1);
                errdefer allocator.free(single);
                single[0] = part;
                try groups.append(allocator, .{ .content = single });
            }
        }
    }
    if (opts.title != null and !opts.combined and !opts.group) return error.InvalidEmbeddingTitle;
    const embeddings = if (model.session.backend() == .cuda) try pipeline.embedContent(groups.items) else generic: {
        const grouped = @import("pipelines/embedding_gemma2.zig");
        const vectors = try allocator.alloc([]f32, groups.items.len);
        errdefer allocator.free(vectors);
        var filled: usize = 0;
        errdefer for (vectors[0..filled]) |vector| allocator.free(vector);
        for (groups.items, vectors) |group, *vector| {
            const parts = try allocator.alloc(grouped.Part, group.content.len);
            defer allocator.free(parts);
            for (group.content, parts) |part, *dest| dest.* = switch (part) {
                .text => |text| .{ .text = text },
                .image => |image| .{ .image = image },
                .audio => |audio| .{ .audio = audio.bytes },
                .video => |video| .{ .video = video },
            };
            const result = try grouped.embed(allocator, model.session, model.getTokenizer(), .{ .title = opts.title, .content = parts }, .{ .task_type = task_type, .dimensions = dimensions orelse 768 }, model.embeddingExecutionLock(), pipeline.execution_control);
            vector.* = result.vector;
            pipeline.last_input_tokens +|= result.input_tokens;
            filled += 1;
        }
        break :generic vectors;
    };
    defer freeEmbeddings(allocator, embeddings);
    var buffer = std.ArrayListUnmanaged(u8).empty;
    defer buffer.deinit(allocator);
    try buffer.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buffer, allocator, opts.model_dir);
    if (model.embedding_identity) |identity| {
        try buffer.appendSlice(allocator, ",\"model_identity\":");
        try jsonEncodeString(&buffer, allocator, &identity);
    }
    try buffer.appendSlice(allocator, ",\"backend\":");
    try jsonEncodeString(&buffer, allocator, @tagName(model.session.backend()));
    try buffer.appendSlice(allocator, ",\"embeddings\":[");
    for (embeddings, 0..) |embedding, index| {
        if (index > 0) try buffer.append(allocator, ',');
        const width = dimensions orelse embedding.len;
        if (width > embedding.len) return error.InvalidEmbeddingDimensions;
        const selected = embedding[0..width];
        if (width < embedding.len) {
            var squared_norm: f32 = 0;
            for (selected) |value| squared_norm += value * value;
            if (!std.math.isFinite(squared_norm) or squared_norm <= 0) return error.InvalidEmbeddingOutput;
            const scale = 1.0 / @sqrt(squared_norm);
            for (selected) |*value| value.* *= scale;
        }
        try appendEmbeddingJson(&buffer, allocator, selected);
    }
    try buffer.appendSlice(allocator, "],\"usage\":{\"prompt_tokens\":");
    var count_buffer: [32]u8 = undefined;
    try buffer.appendSlice(allocator, try std.fmt.bufPrint(&count_buffer, "{d}", .{pipeline.last_input_tokens}));
    try buffer.appendSlice(allocator, "}}\n");
    try writer.writeAll(buffer.items);
    try writer.flush();
}

fn parseEmbeddingDimensions(value: []const u8) !usize {
    const dimensions = std.fmt.parseInt(usize, value, 10) catch return error.InvalidEmbeddingDimensions;
    if (dimensions == 0 or dimensions > 768) return error.InvalidEmbeddingDimensions;
    return dimensions;
}

fn appendCliContentInput(allocator: std.mem.Allocator, groups: *std.ArrayListUnmanaged(embedding_mod.EmbeddingContentInput), owned: *std.ArrayListUnmanaged([]u8), item: std.json.Value) !void {
    const values: []const std.json.Value = if (item == .object and item.object.contains("content")) content: {
        const value = item.object.get("content").?;
        if (value != .array or value.array.items.len == 0) return error.InvalidOrderedEmbeddingContent;
        break :content value.array.items;
    } else &.{item};
    const parts = try allocator.alloc(embedding_mod.EmbeddingContentPart, values.len);
    errdefer allocator.free(parts);
    for (values, 0..) |value, index| parts[index] = try cliContentPart(allocator, owned, value);
    try groups.append(allocator, .{ .content = parts });
}

fn cliContentPart(allocator: std.mem.Allocator, owned: *std.ArrayListUnmanaged([]u8), item: std.json.Value) !embedding_mod.EmbeddingContentPart {
    if (item == .string) return .{ .text = item.string };
    if (item != .object) return error.InvalidOrderedEmbeddingContent;
    const kind = item.object.get("type") orelse return error.ContentPartTypeRequired;
    if (kind != .string) return error.ContentPartTypeRequired;
    if (std.mem.eql(u8, kind.string, "text")) {
        const value = item.object.get("text") orelse return error.TextContentPartMissingText;
        if (value != .string) return error.TextContentPartMissingText;
        return .{ .text = value.string };
    }
    const media = std.mem.eql(u8, kind.string, "media");
    const image_url = std.mem.eql(u8, kind.string, "image_url");
    if (!media and !image_url) return error.UnsupportedMediaMimeType;
    const value = if (media) item.object.get("data") orelse return error.MediaContentPartMissingData else item.object.get("image_url") orelse return error.ImageUrlContentPartMissingUrl;
    const encoded = if (value == .string) value.string else if (image_url and value == .object) url: {
        const v = value.object.get("url") orelse return error.ImageUrlContentPartMissingUrl;
        if (v != .string) return error.ImageUrlContentPartMissingUrl;
        break :url v.string;
    } else return error.InvalidMediaBase64;
    const mime_value = if (media) item.object.get("mime_type") orelse return error.MediaContentPartMissingMimeType else null;
    if (mime_value) |mime| if (mime != .string) return error.MediaContentPartMissingMimeType;
    var mime: []const u8 = if (mime_value) |v| v.string else "image/";
    var uri_mime: ?[]u8 = null;
    defer if (uri_mime) |value_mime| allocator.free(value_mime);
    const decoded = if (data_uri.hasScheme(encoded)) uri: {
        const value_uri = try data_uri.decodeAlloc(allocator, encoded);
        uri_mime = value_uri.media_type;
        if (uri_mime) |actual| {
            if (media and !std.ascii.eqlIgnoreCase(try data_uri.mediaTypeEssence(mime), try data_uri.mediaTypeEssence(actual))) {
                allocator.free(value_uri.data);
                return error.MediaDataMimeTypeMismatch;
            }
            mime = actual;
        }
        break :uri value_uri.data;
    } else raw: {
        if (image_url) return error.NativeEmbedRequiresInlineImage;
        const len = try data_uri.validateCanonicalStandardBase64(encoded);
        const bytes = try allocator.alloc(u8, len);
        errdefer allocator.free(bytes);
        try std.base64.standard.Decoder.decode(bytes, encoded);
        break :raw bytes;
    };
    errdefer allocator.free(decoded);
    if (decoded.len == 0) return error.InvalidMediaBase64;
    const is_video = std.ascii.eqlIgnoreCase(try data_uri.mediaTypeEssence(mime), "video/mp4");
    if (!std.ascii.startsWithIgnoreCase(mime, "image/") and !std.ascii.startsWithIgnoreCase(mime, "audio/") and !is_video) return error.UnsupportedMediaMimeType;
    try owned.append(allocator, decoded);
    return if (is_video) .{ .video = decoded } else if (std.ascii.startsWithIgnoreCase(mime, "image/")) .{ .image = decoded } else .{ .audio = .{ .bytes = decoded, .decode_options = .{ .format_hint = audio_mod.detectFormatFromMime(mime) } } };
}

fn embedGemma2(a: std.mem.Allocator, io: std.Io, writer: *std.Io.Writer, model: *model_manager_mod.LoadedModel, opts: Options) !void {
    const grouped = @import("pipelines/embedding_gemma2.zig");
    const watchdog = if (model.session.interruption() == .uninterruptible) try @import("hard_cancellation_watchdog.zig").HardCancellationWatchdog.create(a) else null;
    defer if (watchdog) |owned| owned.destroy();
    if (watchdog) |owned| try owned.start(io);
    const control = @import("execution_control.zig").InferenceExecutionControl{ .io = io, .hard_cancellation = if (watchdog) |owned| owned.boundary() else null };
    try model.verifyEmbeddingIdentity();
    if (opts.title != null and !opts.group and !opts.combined) return error.InvalidEmbeddingTitle;
    const images = try loadFiles(a, opts.image_paths.items);
    defer freeOwnedBytes(a, images);
    const audio = try loadFiles(a, opts.audio_paths.items);
    defer freeOwnedBytes(a, audio);
    const parts = try a.alloc(grouped.Part, opts.order.items.len);
    defer a.free(parts);
    for (opts.order.items, parts) |item, *part| part.* = switch (item.modality) {
        .text => .{ .text = opts.texts.items[item.index] },
        .image => .{ .image = images[item.index] },
        .audio => .{ .audio = audio[item.index] },
        .video => return error.UnsupportedVideoBackend,
    };
    var lease = model.acquireEmbeddingAssetLease(false);
    defer lease.release();
    try model.ensureEmbeddingAssets(true, false, false);
    var vectors: std.ArrayListUnmanaged([]f32) = .empty;
    defer {
        for (vectors.items) |vector| a.free(vector);
        vectors.deinit(a);
    }
    var tokens: usize = 0;
    for (0..if (opts.group or opts.combined) 1 else parts.len) |i| {
        const result = try grouped.embed(a, model.session, model.getTokenizer(), .{ .title = opts.title, .content = if (opts.group or opts.combined) parts else parts[i..][0..1] }, .{ .task_type = opts.task_type orelse "RETRIEVAL_DOCUMENT", .dimensions = opts.dimensions orelse 768 }, model.embeddingExecutionLock(), control);
        errdefer a.free(result.vector);
        try vectors.append(a, result.vector);
        tokens += result.input_tokens;
    }
    try model.verifyEmbeddingIdentity();
    const json = try std.json.Stringify.valueAlloc(a, .{ .model = opts.model_dir, .model_identity = model.embedding_identity.?[0..], .embeddings = vectors.items, .dimensions = opts.dimensions, .input_tokens = tokens, .grouped = opts.group }, .{});
    defer a.free(json);
    try writer.writeAll(json);
    try writer.writeAll("\n");
    try writer.flush();
}

fn loadFiles(allocator: std.mem.Allocator, paths: []const []const u8) ![][]const u8 {
    return loadFilesBounded(allocator, paths, 64 * 1024 * 1024);
}
fn loadFilesBounded(allocator: std.mem.Allocator, paths: []const []const u8, max_bytes: usize) ![][]const u8 {
    const out = try allocator.alloc([]const u8, paths.len);
    var loaded: usize = 0;
    errdefer {
        for (out[0..loaded]) |bytes| allocator.free(bytes);
        allocator.free(out);
    }

    for (paths, 0..) |path, i| {
        out[i] = try c_file.readFileMax(allocator, path, max_bytes);
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
    writer: *std.Io.Writer,
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
            .video => "video",
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
            .video => return error.UnsupportedVideoBackend,
        };
        try appendEmbeddingJson(&buf, allocator, emb);
    }
    try buf.appendSlice(allocator, "]}\n");

    try writer.writeAll(buf.items);
    try writer.flush();
}

fn writeSparseResultJson(
    allocator: std.mem.Allocator,
    writer: *std.Io.Writer,
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

    try writer.writeAll(buf.items);
    try writer.flush();
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
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    // Match this one-shot command's execution control for session admission
    // and close as well as the native call itself.
    session_manager.native_call_policy = .in_process;
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
        .cuda => if (build_options.enable_cuda) &.{backends.BackendType.cuda} else &.{backends.BackendType.native},
    };
}

fn ensureRequestedMetalHostedBackendAvailable(choice: BackendChoice) !void {
    if (choice != .metal and choice != .cuda) return;
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
}

fn printUsage() void {
    print(
        \\usage: antfly inference embed <model-dir> [--backend auto|native|metal|cuda] [--graph-runtime interpreter|partitioned|compiled|compiled-required] [--print-timing] [--text <text>]... [--image <path>]... [--audio <path>]... [--video <mp4>]...
        \\  Runs local embedding and prints a JSON response to stdout.
        \\  Input order is preserved across repeated --text/--image/--audio/--video flags.
        \\  graph-runtime controls imported static graph execution; default is environment fallback, then interpreter.
        \\  Benchmark gates: TERMITE_GRAPH_RUNTIME_FAIL_CLOSED=1, TERMITE_GRAPH_EXECUTOR_STATS=1, TERMITE_GRAPH_PARTITION_REPORT=1.
        \\  --print-timing prints phase timings to stderr.
        \\  EmbeddingGemma 2: --task-type <task>, --dimensions 1..768 (trained: 128/256/512/768).
        \\  --combined embeds repeated input flags as one ordered EmbeddingGemma 2 input.
        \\  --content-json <path> accepts API input JSON with ordered content groups and inline media.
        \\  --group combines all text/image/audio parts in argument order into one vector.
        \\  --title <title> requires --group or --combined and RETRIEVAL_DOCUMENT with text.
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
        "metal",
        "--graph-runtime",
        "partitioned",
        "--print-timing",
    });
    defer opts.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqual(BackendChoice.metal, opts.backend);
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

test "EmbeddingGemma 2 CLI parses ordered content tasks and MRL dimensions" {
    var opts = try parseArgs(std.testing.allocator, &.{ "/tmp/model", "--combined", "--task-type", "CODE_RETRIEVAL", "--dimensions", "128", "--text", "find a sort function" });
    defer opts.deinit(std.testing.allocator);
    try std.testing.expect(opts.combined);
    try std.testing.expectEqualStrings("CODE_RETRIEVAL", opts.task_type.?);
    try std.testing.expectEqual(@as(usize, 128), opts.dimensions.?);
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseEmbeddingDimensions("0"));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseEmbeddingDimensions("769"));
    try std.testing.expectError(error.InvalidEmbeddingDimensions, parseEmbeddingDimensions("-1"));
}

test "EmbeddingGemma 2 CLI content groups preserve caller order and reject nesting" {
    const allocator = std.testing.allocator;
    var json = try std.json.parseFromSlice(std.json.Value, allocator, "{\"content\":[{\"type\":\"text\",\"text\":\"hello\"},\" world\"]}", .{});
    defer json.deinit();
    var groups = std.ArrayListUnmanaged(embedding_mod.EmbeddingContentInput).empty;
    defer {
        for (groups.items) |group| allocator.free(group.content);
        groups.deinit(allocator);
    }
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer owned.deinit(allocator);
    try appendCliContentInput(allocator, &groups, &owned, json.value);
    try std.testing.expectEqual(@as(usize, 1), groups.items.len);
    try std.testing.expectEqualStrings("hello", groups.items[0].content[0].text);
    try std.testing.expectEqualStrings(" world", groups.items[0].content[1].text);
    try std.testing.expectError(error.ContentPartTypeRequired, cliContentPart(allocator, &owned, json.value));
}

test "embed auto backend prefers Metal then native" {
    var session_manager = backends.SessionManager.init(std.testing.allocator);
    configureBackendPreference(&session_manager, .auto);
    try std.testing.expectEqual(.in_process, session_manager.native_call_policy);
    try std.testing.expect(session_manager.uninterruptibleBackendsAllowed());
    if (build_options.enable_metal) {
        try std.testing.expectEqualSlices(backends.BackendType, &.{ .metal, .native }, session_manager.preferred_backends);
    } else {
        try std.testing.expectEqualSlices(backends.BackendType, &.{.native}, session_manager.preferred_backends);
    }
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

test "embed result writer preserves multimodal order and escaped model name" {
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var text = [_]f32{ 1, 2 };
    var image = [_]f32{ 3, 4 };
    var audio = [_]f32{ 5, 6 };
    var texts = [_][]f32{&text};
    var images = [_][]f32{&image};
    var audios = [_][]f32{&audio};
    try writeResultJson(std.testing.allocator, &output.writer, "model\"name", &.{
        .{ .modality = .audio, .index = 0 },
        .{ .modality = .text, .index = 0 },
        .{ .modality = .image, .index = 0 },
        .{ .modality = .text, .index = 0 },
    }, &texts, &images, &audios);
    try std.testing.expectEqualStrings(
        "{\"model\":\"model\\\"name\",\"modalities\":[\"audio\",\"text\",\"image\",\"text\"],\"embeddings\":[[5,6],[1,2],[3,4],[1,2]]}\n",
        output.written(),
    );
}

test "embed sparse result writer preserves input order" {
    var output = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer output.deinit();
    var indices = [_]u32{ 2, 17 };
    var values = [_]f32{ 0.25, 1.5 };
    const embeddings = [_]sparse_embedding_mod.SparseVector{
        .{ .indices = indices[0..1], .values = values[0..1] },
        .{ .indices = indices[1..], .values = values[1..] },
    };
    try writeSparseResultJson(std.testing.allocator, &output.writer, "model", &.{
        .{ .modality = .text, .index = 1 },
        .{ .modality = .text, .index = 0 },
    }, &embeddings);
    try std.testing.expectEqualStrings(
        "{\"model\":\"model\",\"modalities\":[\"text\",\"text\"],\"embeddings\":[{\"indices\":[17],\"values\":[1.5]},{\"indices\":[2],\"values\":[0.25]}]}\n",
        output.written(),
    );
}

test "embed result writers propagate output failures" {
    var output = std.Io.Writer.fixed(&.{});
    try std.testing.expectError(error.WriteFailed, writeResultJson(
        std.testing.allocator,
        &output,
        "model",
        &.{},
        &.{},
        &.{},
        &.{},
    ));
    try std.testing.expectError(error.WriteFailed, writeSparseResultJson(
        std.testing.allocator,
        &output,
        "model",
        &.{},
        &.{},
    ));
}
