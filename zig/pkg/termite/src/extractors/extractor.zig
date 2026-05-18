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
const backends_mod = @import("../backends/backends.zig");
const extraction_mod = @import("../pipelines/extraction.zig");
const readers_mod = @import("../readers/reader.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const manifest_mod = @import("../models/manifest.zig");
const model_caps = @import("../models/capabilities.zig");
const registry_mod = @import("../registry/registry.zig");
const c_file = @import("../util/c_file.zig");

pub const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    models_dir: []const u8,
    session_manager: *backends_mod.SessionManager,
    model_manager: *model_manager_mod.ModelManager,
};

pub const Extractor = union(enum) {
    recognizer: RecognizerExtractor,
    reader: ReaderExtractor,

    pub fn initRecognizer(allocator: std.mem.Allocator, model_path: []const u8, model_name: []const u8) !Extractor {
        return .{ .recognizer = .{
            .model_path = try allocator.dupe(u8, model_path),
            .model_name = try allocator.dupe(u8, model_name),
        } };
    }

    pub fn initReader(allocator: std.mem.Allocator, model_path: []const u8) !Extractor {
        return .{ .reader = .{
            .model_path = try allocator.dupe(u8, model_path),
        } };
    }

    pub fn deinit(self: *Extractor, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .recognizer => |*recognizer| recognizer.deinit(allocator),
            .reader => |*reader| reader.deinit(allocator),
        }
    }

    pub fn extractText(
        self: *Extractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        texts: []const []const u8,
    ) ![]extraction_mod.ExtractionResult {
        return switch (self.*) {
            .recognizer => |*recognizer| recognizer.extractText(ctx, schemas, config, texts),
            .reader => error.UnsupportedInput,
        };
    }

    pub fn extractImages(
        self: *Extractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![]extraction_mod.ExtractionResult {
        return switch (self.*) {
            .recognizer => |*recognizer| recognizer.extractImages(ctx, schemas, config, image_datas, read_options),
            .reader => |*reader| reader.extractImages(ctx, schemas, config, image_datas, read_options),
        };
    }
};

pub fn resolve(ctx: Context, model_name: []const u8, wants_images: bool) !Extractor {
    if (wants_images) {
        if (try tryResolveReader(ctx, model_name)) |extractor| return extractor;
        if (try tryResolveRecognizer(ctx, model_name)) |extractor| return extractor;
    } else {
        if (try tryResolveRecognizer(ctx, model_name)) |extractor| return extractor;
        if (try tryResolveReader(ctx, model_name)) |extractor| return extractor;
    }
    return error.ModelNotFound;
}

const RecognizerExtractor = struct {
    model_path: []const u8,
    model_name: []const u8,

    fn deinit(self: *RecognizerExtractor, allocator: std.mem.Allocator) void {
        allocator.free(self.model_path);
        allocator.free(self.model_name);
    }

    fn extractText(
        self: *RecognizerExtractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        texts: []const []const u8,
    ) ![]extraction_mod.ExtractionResult {
        const model = try ctx.model_manager.loadFromDir(self.model_path);
        if (!model.isGlinerModel() or !model.supportsExtraction()) return error.InvalidModelForExtraction;
        if (!model_caps.modelAcceptsInput(&model.manifest, "text")) return error.UnsupportedInput;

        var gliner = model.glinerPipeline(ctx.allocator);
        var extraction_config = config;
        extraction_config.cleanup_model = try model.getCleanupHead();
        return extraction_mod.extractBatch(ctx.allocator, &gliner, texts, schemas, extraction_config);
    }

    fn extractImages(
        self: *RecognizerExtractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![]extraction_mod.ExtractionResult {
        const model = try ctx.model_manager.loadFromDir(self.model_path);
        if (!model.isGlinerModel() or !model.supportsExtraction()) return error.InvalidModelForExtraction;
        if (!model_caps.modelAcceptsInput(&model.manifest, "text")) return error.UnsupportedInput;

        const texts = try readTextsForExtraction(ctx, self.model_name, image_datas, read_options);
        defer {
            for (texts) |text| ctx.allocator.free(text);
            ctx.allocator.free(texts);
        }
        return self.extractText(ctx, schemas, config, texts);
    }
};

const ReaderExtractor = struct {
    model_path: []const u8,

    fn deinit(self: *ReaderExtractor, allocator: std.mem.Allocator) void {
        allocator.free(self.model_path);
    }

    fn extractImages(
        self: *ReaderExtractor,
        ctx: Context,
        schemas: []const extraction_mod.ExtractionSchema,
        config: extraction_mod.ExtractionConfig,
        image_datas: []const []const u8,
        read_options: readers_mod.ReadOptions,
    ) ![]extraction_mod.ExtractionResult {
        var reader = try readers_mod.LoadedReader.loadFromDir(
            ctx.allocator,
            self.model_path,
            ctx.session_manager,
            ctx.model_manager,
        );
        defer reader.deinit();

        var results = std.ArrayListUnmanaged(readers_mod.Result).empty;
        defer {
            for (results.items) |*result| result.deinit();
            results.deinit(ctx.allocator);
        }

        for (image_datas) |image_data| {
            try results.append(ctx.allocator, try reader.read(image_data, read_options));
        }

        return extraction_mod.extractBatchFromReaderResults(ctx.allocator, results.items, schemas, config);
    }
};

fn tryResolveRecognizer(ctx: Context, model_name: []const u8) !?Extractor {
    const path = resolveNamedModelPath(ctx, model_name, "recognizers") catch |err| switch (err) {
        error.ModelNotFound => return null,
        else => return err,
    };
    defer ctx.allocator.free(path);

    var manifest = manifest_mod.loadFromDir(ctx.allocator, path) catch return null;
    defer manifest.deinit();
    if (!model_caps.modelSupportsCapability("recognizer", manifest.gliner_model_type, manifest.capabilities, "extraction")) return null;

    return try Extractor.initRecognizer(ctx.allocator, path, model_name);
}

fn tryResolveReader(ctx: Context, model_name: []const u8) !?Extractor {
    const path = resolveNamedModelPath(ctx, model_name, "readers") catch |err| switch (err) {
        error.ModelNotFound => return null,
        else => return err,
    };
    defer ctx.allocator.free(path);

    var manifest = manifest_mod.loadFromDir(ctx.allocator, path) catch return null;
    defer manifest.deinit();
    if (!model_caps.modelSupportsCapability("reader", manifest.gliner_model_type, manifest.capabilities, "extraction")) return null;
    if (!model_caps.modelAcceptsInput(&manifest, "image")) return null;

    return try Extractor.initReader(ctx.allocator, path);
}

fn readTextsForExtraction(
    ctx: Context,
    extractor_model_name: []const u8,
    image_datas: []const []const u8,
    read_options: readers_mod.ReadOptions,
) ![][]const u8 {
    const model_path = try resolveReaderModelPathForExtraction(ctx, extractor_model_name);

    var reader = try readers_mod.LoadedReader.loadFromDir(
        ctx.allocator,
        model_path,
        ctx.session_manager,
        ctx.model_manager,
    );
    defer reader.deinit();

    const texts = try ctx.allocator.alloc([]const u8, image_datas.len);
    var initialized: usize = 0;
    errdefer {
        for (texts[0..initialized]) |text| ctx.allocator.free(text);
        ctx.allocator.free(texts);
    }

    for (image_datas, 0..) |image_data, i| {
        var result = try reader.read(image_data, read_options);
        defer result.deinit();
        texts[i] = try ctx.allocator.dupe(u8, result.text);
        initialized += 1;
    }

    return texts;
}

fn resolveReaderModelPathForExtraction(ctx: Context, extractor_model_name: []const u8) ![]const u8 {
    if (platform.env.getenv("TERMITE_EXTRACT_DEFAULT_READER_MODEL")) |override| {
        return resolveNamedReaderPath(ctx, override);
    }

    if (resolveNamedReaderPath(ctx, extractor_model_name)) |path| {
        return path;
    } else |_| {}

    var registry = registry_mod.ModelRegistry.init(ctx.allocator, ctx.models_dir);
    const discovered = registry.discover(ctx.io) catch return error.NoReaderModelAvailable;
    defer {
        for (discovered) |entry| {
            ctx.allocator.free(entry.name);
            ctx.allocator.free(entry.path);
        }
        if (discovered.len > 0) ctx.allocator.free(discovered);
    }

    var best_name: ?[]const u8 = null;
    var best_rank: u8 = std.math.maxInt(u8);
    for (discovered) |entry| {
        if (entry.kind != .reader) continue;
        if (!model_manager_mod.isModelDirPotentiallyLoadableInCurrentBuild(ctx.allocator, entry.path)) continue;

        const rank = extractionReaderPreference(entry.name, extractor_model_name);
        if (best_name == null or rank < best_rank or (rank == best_rank and std.mem.lessThan(u8, entry.name, best_name.?))) {
            best_name = entry.name;
            best_rank = rank;
        }
    }

    const reader_name = best_name orelse return error.NoReaderModelAvailable;
    return resolveNamedReaderPath(ctx, reader_name);
}

fn resolveNamedModelPath(ctx: Context, requested_name: []const u8, task_type: []const u8) ![]const u8 {
    const normalized = if (std.mem.startsWith(u8, requested_name, "hf:")) requested_name[3..] else requested_name;
    const name_without_variant = if (std.mem.indexOfScalar(u8, normalized, ':')) |colon| normalized[0..colon] else normalized;

    if (std.mem.startsWith(u8, name_without_variant, "/")) {
        return try ctx.allocator.dupe(u8, name_without_variant);
    }

    const root_path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ ctx.models_dir, name_without_variant });
    if (dirExists(root_path)) {
        return root_path;
    }
    ctx.allocator.free(root_path);

    const task_path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}/{s}", .{ ctx.models_dir, task_type, name_without_variant });
    if (dirExists(task_path)) {
        return task_path;
    }
    ctx.allocator.free(task_path);

    const task_dir = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ ctx.models_dir, task_type });
    defer ctx.allocator.free(task_dir);
    if (registry_mod.resolveVariant(ctx.allocator, ctx.io, task_dir, name_without_variant)) |variant_path| {
        return variant_path;
    }

    if (registry_mod.resolveVariant(ctx.allocator, ctx.io, ctx.models_dir, name_without_variant)) |variant_path| {
        return variant_path;
    }

    if (std.mem.indexOfScalar(u8, name_without_variant, '/')) |slash| {
        const model_only = name_without_variant[slash + 1 ..];
        const flat_path = try std.fmt.allocPrint(ctx.allocator, "{s}/{s}", .{ ctx.models_dir, model_only });
        if (dirExists(flat_path)) {
            return flat_path;
        }
        ctx.allocator.free(flat_path);

        if (registry_mod.resolveVariant(ctx.allocator, ctx.io, ctx.models_dir, model_only)) |variant_path| {
            return variant_path;
        }
    }

    return error.ModelNotFound;
}

fn resolveNamedReaderPath(ctx: Context, requested_name: []const u8) ![]const u8 {
    return resolveNamedModelPath(ctx, requested_name, "readers");
}

fn extractionReaderPreference(reader_name: []const u8, extractor_model_name: []const u8) u8 {
    if (std.mem.eql(u8, reader_name, extractor_model_name)) return 0;
    if (std.mem.indexOf(u8, reader_name, "trocr") != null) return 10;
    if (std.mem.indexOf(u8, reader_name, "paddleocr") != null) return 20;
    if (std.mem.indexOf(u8, reader_name, "florence") != null) return 30;
    if (std.mem.indexOf(u8, reader_name, "donut") != null) return 40;
    return 100;
}

fn dirExists(path: []const u8) bool {
    return c_file.fileExists(std.heap.page_allocator, path);
}

fn writeTestManifest(dir: std.Io.Dir, sub_path: []const u8, manifest_json: []const u8) !void {
    try dir.createDirPath(std.testing.io, sub_path);
    const file_path = try std.fs.path.join(std.testing.allocator, &.{ sub_path, "model_manifest.json" });
    defer std.testing.allocator.free(file_path);
    try dir.writeFile(std.testing.io, .{
        .sub_path = file_path,
        .data = manifest_json,
    });
}

test "extractor prefers same-name reader first" {
    try std.testing.expectEqual(@as(u8, 0), extractionReaderPreference("foo/bar", "foo/bar"));
    try std.testing.expect(extractionReaderPreference("Xenova/trocr-base-printed", "other/model") < extractionReaderPreference("monkt/paddleocr-onnx", "other/model"));
}

test "resolve prefers reader for image extraction when both exist" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "readers/acme/doc-extract", "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}");
    try writeTestManifest(tmp.dir, "recognizers/acme/doc-extract", "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var extractor = try resolve(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/doc-extract", true);
    defer extractor.deinit(allocator);

    try std.testing.expect(extractor == .reader);
}

test "resolveNamedReaderPath supports flat default model layout" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "Xenova/trocr-base-printed", "{\"type\":\"reader\",\"inputs\":[\"image\"]}");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    const path = try resolveNamedReaderPath(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "Xenova/trocr-base-printed");
    defer allocator.free(path);

    try std.testing.expect(std.mem.endsWith(u8, path, "Xenova/trocr-base-printed"));
}

test "resolve prefers recognizer for text extraction when both exist" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(tmp.dir, "readers/acme/doc-extract", "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}");
    try writeTestManifest(tmp.dir, "recognizers/acme/doc-extract", "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}");

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var extractor = try resolve(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "acme/doc-extract", false);
    defer extractor.deinit(allocator);

    try std.testing.expect(extractor == .recognizer);
}

test "reader extractor does not accept text input" {
    const allocator = std.testing.allocator;
    var extractor = try Extractor.initReader(allocator, "/tmp/model");
    defer extractor.deinit(allocator);

    try std.testing.expectError(error.UnsupportedInput, extractor.extractText(.{
        .allocator = allocator,
        .io = undefined,
        .models_dir = ".",
        .session_manager = undefined,
        .model_manager = undefined,
    }, &.{}, .{}, &.{}));
}
