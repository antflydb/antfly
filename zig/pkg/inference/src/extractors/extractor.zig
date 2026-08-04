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

const reader_selection_cache_ttl_ns: i96 = 30 * std.time.ns_per_s;

pub const Context = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    models_dir: []const u8,
    session_manager: *backends_mod.SessionManager,
    model_manager: *model_manager_mod.ModelManager,
    reader_resolver: ?*ReaderResolver = null,
};

/// Node-scoped positive cache for the default OCR reader. Model discovery and
/// compatibility inspection are filesystem work, so concurrent extraction
/// requests share one selection instead of reparsing every installed model.
/// The short TTL makes newly installed preferred readers visible without a
/// restart; failed construction invalidates immediately so replaced or removed
/// artifacts are rediscovered on the next request.
pub const ReaderResolver = struct {
    allocator: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    cached_default_path: ?[]u8 = null,
    cached_at: ?std.Io.Timestamp = null,

    pub fn init(allocator: std.mem.Allocator) ReaderResolver {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ReaderResolver) void {
        if (self.cached_default_path) |path| self.allocator.free(path);
        self.* = undefined;
    }

    fn invalidate(self: *ReaderResolver, io: std.Io, failed_path: []const u8) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const cached = self.cached_default_path orelse return;
        if (!std.mem.eql(u8, cached, failed_path)) return;
        self.allocator.free(cached);
        self.cached_default_path = null;
        self.cached_at = null;
    }
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
        var model_handle = try ctx.model_manager.acquireFromDir(self.model_path);
        defer model_handle.release();
        const model = model_handle.get();
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

        const results = try reader.readBatch(image_datas, read_options);
        defer {
            for (results) |*result| result.deinit();
            ctx.allocator.free(results);
        }
        if (results.len != image_datas.len) return error.InvalidReadResultCount;

        return extraction_mod.extractBatchFromReaderResults(ctx.allocator, results, schemas, config);
    }
};

fn tryResolveRecognizer(ctx: Context, model_name: []const u8) !?Extractor {
    const path = resolveNamedModelPath(ctx, model_name, "recognizers") catch |err| switch (err) {
        error.ModelNotFound => return null,
        else => return err,
    };
    defer ctx.allocator.free(path);

    var manifest = manifest_mod.loadListingFromDir(ctx.allocator, path) catch return null;
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

    var manifest = manifest_mod.loadListingFromDir(ctx.allocator, path) catch return null;
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
    defer ctx.allocator.free(model_path);

    var reader = readers_mod.LoadedReader.loadFromDir(
        ctx.allocator,
        model_path,
        ctx.session_manager,
        ctx.model_manager,
    ) catch |err| {
        if (ctx.reader_resolver) |resolver| resolver.invalidate(ctx.io, model_path);
        return err;
    };
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
        return resolveSupportedNamedReaderPath(ctx, override);
    }

    if (resolveSupportedNamedReaderPath(ctx, extractor_model_name)) |path| {
        return path;
    } else |_| {}

    if (ctx.reader_resolver) |resolver| {
        resolver.mutex.lockUncancelable(ctx.io);
        defer resolver.mutex.unlock(ctx.io);
        const now = std.Io.Timestamp.now(ctx.io, .awake);
        if (resolver.cached_default_path) |path| {
            if (resolver.cached_at) |cached_at| {
                const age = std.Io.Timestamp.durationTo(cached_at, now).nanoseconds;
                if (age >= 0 and age < reader_selection_cache_ttl_ns) {
                    return ctx.allocator.dupe(u8, path);
                }
            }
            resolver.allocator.free(path);
            resolver.cached_default_path = null;
            resolver.cached_at = null;
        }

        const path = try discoverReaderModelPathForExtraction(ctx, extractor_model_name);
        errdefer ctx.allocator.free(path);
        resolver.cached_default_path = try resolver.allocator.dupe(u8, path);
        resolver.cached_at = now;
        return path;
    }

    return discoverReaderModelPathForExtraction(ctx, extractor_model_name);
}

fn discoverReaderModelPathForExtraction(ctx: Context, extractor_model_name: []const u8) ![]const u8 {
    var registry = registry_mod.ModelRegistry.init(ctx.allocator, ctx.models_dir);
    const discovered = registry.discoverShallow(ctx.io) catch return error.NoReaderModelAvailable;
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
        var manifest = manifest_mod.loadListingFromDir(ctx.allocator, entry.path) catch continue;
        defer manifest.deinit();
        if (manifest.model_type != .reader and entry.kind != .reader) continue;
        if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) continue;
        if (!readers_mod.isSupportedManifest(ctx.allocator, entry.path, manifest)) continue;

        const rank = extractionReaderPreference(entry.name, extractor_model_name);
        if (best_name == null or rank < best_rank or (rank == best_rank and std.mem.lessThan(u8, entry.name, best_name.?))) {
            best_name = entry.name;
            best_rank = rank;
        }
    }

    const reader_name = best_name orelse return error.NoReaderModelAvailable;
    return resolveSupportedNamedReaderPath(ctx, reader_name);
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

fn resolveSupportedNamedReaderPath(ctx: Context, requested_name: []const u8) ![]const u8 {
    const path = try resolveNamedReaderPath(ctx, requested_name);
    errdefer ctx.allocator.free(path);
    var manifest = manifest_mod.loadListingFromDir(ctx.allocator, path) catch return error.ModelNotFound;
    defer manifest.deinit();
    if (!model_manager_mod.isManifestPotentiallyLoadableInCurrentBuild(manifest)) return error.ModelNotFound;
    if (!readers_mod.isSupportedManifest(ctx.allocator, path, manifest)) return error.ModelNotFound;
    return path;
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

fn writeTestFlorenceReader(dir: std.Io.Dir, sub_path: []const u8) !void {
    try writeTestManifest(dir, sub_path, "{\"type\":\"reader\",\"inputs\":[\"image\"]}");
    const config_path = try std.fs.path.join(std.testing.allocator, &.{ sub_path, "config.json" });
    defer std.testing.allocator.free(config_path);
    try dir.writeFile(std.testing.io, .{
        .sub_path = config_path,
        .data = "{\"model_type\":\"florence2\",\"architectures\":[\"Florence2ForConditionalGeneration\"]}",
    });
    const model_path = try std.fs.path.join(std.testing.allocator, &.{ sub_path, "model.gguf" });
    defer std.testing.allocator.free(model_path);
    try dir.writeFile(std.testing.io, .{ .sub_path = model_path, .data = "GGUFstub" });
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

test "image extraction fallback ignores manifest-only unsupported readers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(
        tmp.dir,
        "readers/monkt/paddleocr-onnx",
        "{\"type\":\"reader\",\"capabilities\":[\"extraction\"],\"inputs\":[\"image\"]}",
    );

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "fastino/gliner2-base-v1"));
}

test "image extraction does not reinterpret the extractor root as a reader" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestManifest(
        tmp.dir,
        "fastino/gliner2-base-v1",
        "{\"type\":\"recognizer\",\"capabilities\":[\"extraction\"],\"inputs\":[\"text\"]}",
    );

    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    try std.testing.expectError(error.NoReaderModelAvailable, resolveReaderModelPathForExtraction(.{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
    }, "fastino/gliner2-base-v1"));
}

test "image extraction caches a supported fallback reader selection" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try writeTestFlorenceReader(tmp.dir, "readers/antflydb/florence-2-base");
    const models_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(models_dir);

    var resolver = ReaderResolver.init(allocator);
    defer resolver.deinit();
    const ctx = Context{
        .allocator = allocator,
        .io = std.testing.io,
        .models_dir = models_dir,
        .session_manager = undefined,
        .model_manager = undefined,
        .reader_resolver = &resolver,
    };

    const first = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(first);
    try std.testing.expect(resolver.cached_default_path != null);
    try std.testing.expectEqualStrings(first, resolver.cached_default_path.?);

    const second = try resolveReaderModelPathForExtraction(ctx, "fastino/gliner2-base-v1");
    defer allocator.free(second);
    try std.testing.expectEqualStrings(first, second);
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
