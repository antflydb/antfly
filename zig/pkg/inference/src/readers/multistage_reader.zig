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
const metadata_mod = @import("multistage_metadata.zig");
const reader_types = @import("types.zig");
const multistage_ocr = @import("../pipelines/multistage_ocr.zig");
const ctc_decode = @import("../pipelines/ctc_decode.zig");
const image = @import("../pipelines/image.zig");
const model_manager_mod = @import("../server/model_manager.zig");

const AssetResolver = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    canonical_root: []u8,

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        model_path: []const u8,
    ) !AssetResolver {
        return .{
            .allocator = allocator,
            .io = io,
            .canonical_root = try realPathAlloc(
                allocator,
                io,
                model_path,
            ),
        };
    }

    fn deinit(self: *AssetResolver) void {
        self.allocator.free(self.canonical_root);
        self.* = undefined;
    }

    fn resolve(self: *const AssetResolver, relative: []const u8) ![]u8 {
        if (!metadata_mod.isSafeRelativeAssetPath(relative))
            return error.InvalidMetadata;
        const joined = try std.fs.path.join(
            self.allocator,
            &.{ self.canonical_root, relative },
        );
        defer self.allocator.free(joined);
        const canonical = try realPathAlloc(
            self.allocator,
            self.io,
            joined,
        );
        errdefer self.allocator.free(canonical);
        if (!pathIsWithinRoot(self.canonical_root, canonical))
            return error.ModelAssetOutsideRoot;
        return canonical;
    }
};

fn realPathAlloc(
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) ![]u8 {
    const sentinel_path = try std.Io.Dir.cwd().realPathFileAlloc(
        io,
        path,
        allocator,
    );
    defer allocator.free(sentinel_path);
    return try allocator.dupe(u8, sentinel_path);
}

fn pathIsWithinRoot(root: []const u8, path: []const u8) bool {
    if (std.mem.eql(u8, root, path)) return true;
    if (root.len == 1 and root[0] == std.fs.path.sep)
        return path.len > 0 and path[0] == std.fs.path.sep;
    return path.len > root.len and
        std.mem.startsWith(u8, path, root) and
        path[root.len] == std.fs.path.sep;
}

pub const LoadedMultiStageReader = struct {
    allocator: std.mem.Allocator,
    pipeline: multistage_ocr.MultiStageOCRPipeline,
    managed_sessions: std.ArrayListUnmanaged(model_manager_mod.ManagedSession) = .empty,

    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        session_manager: *backends.SessionManager,
        model_manager: *model_manager_mod.ModelManager,
    ) !LoadedMultiStageReader {
        var metadata = try metadata_mod.loadFromDir(allocator, model_path);
        defer metadata.deinit();
        if (!metadata_mod.isMultiStage(&metadata)) return error.InvalidMetadata;
        const runtime_io = session_manager.io orelse
            std.Io.Threaded.global_single_threaded.io();
        var asset_resolver = try AssetResolver.init(
            allocator,
            runtime_io,
            model_path,
        );
        defer asset_resolver.deinit();
        const component_paths = try collectComponentPaths(
            allocator,
            &asset_resolver,
            &metadata,
        );
        defer {
            for (component_paths) |path| allocator.free(path);
            allocator.free(component_paths);
        }

        var component_loader = try model_manager.componentLoaderForPathsWithContract(
            model_path,
            session_manager.preferred_backends,
            component_paths,
            .multistage_ocr,
        );
        var first_err: ?anyerror = null;
        for (component_loader.preferredBackends()) |backend| {
            var backend_loader = try component_loader.restrictToBackend(backend);
            return loadFromDirWithSessionManager(
                allocator,
                model_path,
                &asset_resolver,
                &metadata,
                &backend_loader,
            ) catch |err| {
                if (backendIndependentLoadError(err)) return err;
                if (first_err == null) first_err = err;
                std.log.err("multistage reader backend {s} failed for {s}: {s}", .{ @tagName(backend), model_path, @errorName(err) });
                continue;
            };
        }

        return first_err orelse error.NoBackendAvailable;
    }

    fn collectComponentPaths(
        allocator: std.mem.Allocator,
        asset_resolver: *const AssetResolver,
        metadata: *const metadata_mod.MultiStageMetadata,
    ) ![]const []const u8 {
        var paths = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (paths.items) |path| allocator.free(path);
            paths.deinit(allocator);
        }

        var stages = metadata.stages.valueIterator();
        while (stages.next()) |stage| {
            const candidates = [_]?[]const u8{
                stage.model_file,
                stage.encoder_file,
                stage.decoder_file,
            };
            for (candidates) |maybe_relative| {
                const relative = maybe_relative orelse continue;
                const path = try asset_resolver.resolve(relative);
                var duplicate = false;
                for (paths.items) |existing| {
                    if (std.mem.eql(u8, existing, path)) {
                        duplicate = true;
                        break;
                    }
                }
                if (duplicate) {
                    allocator.free(path);
                    continue;
                }
                try paths.append(allocator, path);
            }
        }
        if (paths.items.len == 0) return error.InvalidMetadata;
        return try paths.toOwnedSlice(allocator);
    }

    fn loadFromDirWithSessionManager(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        asset_resolver: *const AssetResolver,
        metadata: *const metadata_mod.MultiStageMetadata,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
    ) !LoadedMultiStageReader {
        var managed_sessions = std.ArrayListUnmanaged(model_manager_mod.ManagedSession).empty;
        errdefer {
            for (managed_sessions.items) |*managed| managed.deinit();
            managed_sessions.deinit(allocator);
        }
        const detection_stage = metadata.stages.get("detection") orelse return error.InvalidMetadata;
        const detection_file = detection_stage.model_file orelse return error.InvalidMetadata;
        const detection_path = try asset_resolver.resolve(detection_file);
        defer allocator.free(detection_path);

        var detector_managed = try component_loader.load(detection_path);
        errdefer detector_managed.deinit();
        const detector = detector_managed.disownSession();
        var detector_owned = true;
        errdefer if (detector_owned) detector.close();
        try managed_sessions.append(allocator, detector_managed);
        detector_managed.resource_lease = null;

        const detection_preprocess = try loadStagePreprocessConfig(
            allocator,
            asset_resolver,
            metadata,
            &detection_stage,
            detector,
            .detection,
        );

        const post_processor = try loadPostProcessor(detection_stage);

        var pipeline = multistage_ocr.MultiStageOCRPipeline{
            .allocator = allocator,
            .detector = detector,
            .detection_preprocess = detection_preprocess,
            .post_processor = post_processor,
        };
        errdefer pipeline.deinit();
        detector_owned = false;

        if (metadata.stages.get("recognition")) |recognition_stage| {
            const stage_type = recognition_stage.stage_type orelse return error.InvalidMetadata;
            if (std.mem.eql(u8, stage_type, "ctc")) {
                const model_file = recognition_stage.model_file orelse return error.InvalidMetadata;
                const rec_path = try asset_resolver.resolve(model_file);
                defer allocator.free(rec_path);

                var rec_managed = try component_loader.load(rec_path);
                errdefer rec_managed.deinit();
                const rec_session = rec_managed.disownSession();
                var rec_session_owned = true;
                errdefer if (rec_session_owned) rec_session.close();
                try managed_sessions.append(allocator, rec_managed);
                rec_managed.resource_lease = null;

                const char_dict_rel = recognition_stage.char_dict_file orelse return error.InvalidMetadata;
                const char_dict_path = try asset_resolver.resolve(char_dict_rel);
                defer allocator.free(char_dict_path);
                const char_dict = try ctc_decode.loadCharDictFile(allocator, char_dict_path);
                var char_dict_owned = true;
                errdefer if (char_dict_owned) ctc_decode.freeCharDict(allocator, char_dict);

                const recognition_preprocess = try loadStagePreprocessConfig(
                    allocator,
                    asset_resolver,
                    metadata,
                    &recognition_stage,
                    rec_session,
                    .recognition,
                );

                pipeline.recognizer = .{ .ctc = .{
                    .allocator = allocator,
                    .session = rec_session,
                    .char_dict = char_dict,
                    .preprocess = recognition_preprocess,
                } };
                rec_session_owned = false;
                char_dict_owned = false;
            } else if (std.mem.eql(u8, stage_type, "vision2seq")) {
                const encoder_file = recognition_stage.encoder_file orelse return error.InvalidMetadata;
                const decoder_file = recognition_stage.decoder_file orelse return error.InvalidMetadata;
                const encoder_path = try asset_resolver.resolve(encoder_file);
                defer allocator.free(encoder_path);
                const decoder_path = try asset_resolver.resolve(decoder_file);
                defer allocator.free(decoder_path);
                pipeline.recognizer = .{ .vision2seq = try multistage_ocr.Vision2SeqRecognizer.loadFromStagePaths(
                    allocator,
                    model_path,
                    encoder_path,
                    decoder_path,
                    component_loader,
                ) };
            } else {
                return error.MultiStageReaderNotYetSupported;
            }
        }

        if (metadata.stages.get("layout")) |layout_stage| {
            const model_file = layout_stage.model_file orelse return error.InvalidMetadata;
            const layout_path = try asset_resolver.resolve(model_file);
            defer allocator.free(layout_path);

            var layout_managed = try component_loader.load(layout_path);
            errdefer layout_managed.deinit();
            pipeline.layout = layout_managed.disownSession();
            try managed_sessions.append(allocator, layout_managed);
            layout_managed.resource_lease = null;
        }

        if (metadata.stages.get("order")) |order_stage| {
            const model_file = order_stage.model_file orelse return error.InvalidMetadata;
            const order_path = try asset_resolver.resolve(model_file);
            defer allocator.free(order_path);

            var order_managed = try component_loader.load(order_path);
            errdefer order_managed.deinit();
            pipeline.order = order_managed.disownSession();
            try managed_sessions.append(allocator, order_managed);
            order_managed.resource_lease = null;
        }

        return .{
            .allocator = allocator,
            .pipeline = pipeline,
            .managed_sessions = managed_sessions,
        };
    }

    pub fn deinit(self: *LoadedMultiStageReader) void {
        self.pipeline.deinit();
        for (self.managed_sessions.items) |*managed| managed.deinit();
        self.managed_sessions.deinit(self.allocator);
    }

    pub fn read(self: *LoadedMultiStageReader, image_data: []const u8, _: reader_types.ReadOptions) !reader_types.Result {
        var ocr_result = try self.pipeline.run(image_data);
        defer ocr_result.deinit();

        const text = try self.allocator.dupe(u8, ocr_result.full_text);
        errdefer self.allocator.free(text);

        var regions = std.ArrayListUnmanaged(reader_types.Region).empty;
        errdefer {
            for (regions.items) |*region| region.deinit(self.allocator);
            regions.deinit(self.allocator);
        }

        for (ocr_result.regions) |region| {
            try regions.append(self.allocator, .{
                .text = try self.allocator.dupe(u8, region.text),
                .bbox = region.bbox,
                .confidence = @floatCast(if (region.rec_confidence != 0) region.rec_confidence else region.confidence),
                .label = if (region.label) |label| try self.allocator.dupe(u8, label) else null,
            });
        }

        return .{
            .text = text,
            .regions = try regions.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
    }
};

const StageKind = enum {
    detection,
    recognition,
};

fn loadPostProcessor(stage: metadata_mod.StageMetadata) !multistage_ocr.DetectionPostProcessor {
    const kind = stage.post_processor orelse return error.InvalidMetadata;
    if (std.mem.eql(u8, kind, "db")) {
        return .{ .db = .{
            .threshold = 0.3,
            .box_threshold = 0.5,
            .unclip_ratio = 1.5,
            .min_box_area = 10,
        } };
    }
    if (std.mem.eql(u8, kind, "heatmap")) {
        return .{ .heatmap = .{
            .threshold = 0.5,
            .min_area = 50,
        } };
    }
    return error.UnsupportedDetectionPostProcessor;
}

fn loadStagePreprocessConfig(
    allocator: std.mem.Allocator,
    asset_resolver: *const AssetResolver,
    metadata: *const metadata_mod.MultiStageMetadata,
    stage: *const metadata_mod.StageMetadata,
    session: backends.Session,
    stage_kind: StageKind,
) !multistage_ocr.PreprocessConfig {
    var config = defaultPreprocessConfig(stage_kind);
    var loaded_stage_preprocessor = false;

    if (stage.processor_dir) |processor_dir| {
        const relative = try std.fs.path.join(
            allocator,
            &.{ processor_dir, "preprocessor_config.json" },
        );
        defer allocator.free(relative);
        if (asset_resolver.resolve(relative)) |preproc_path| {
            defer allocator.free(preproc_path);
            if (c_file.readFileMax(allocator, preproc_path, 1024 * 1024)) |bytes| {
                defer allocator.free(bytes);
                if (parsePreprocessorConfig(bytes, &config)) |_| {
                    loaded_stage_preprocessor = true;
                } else |_| {}
            } else |_| {}
        } else |err| switch (err) {
            error.InvalidMetadata, error.ModelAssetOutsideRoot => return err,
            else => {},
        }
    }

    if (session.inputInfo().len > 0 and session.inputInfo()[0].shape.len == 4) {
        applySessionShapeOverrides(&config, loaded_stage_preprocessor, stage_kind, session.inputInfo()[0].shape);
    }

    if (!loaded_stage_preprocessor) {
        if (metadata.model_type) |model_type| applyModelTypeNormalization(model_type, stage_kind, &config);
    }
    if (metadata.model_type) |model_type| applyModelTypeStageDefaults(model_type, stage_kind, &config);
    return config;
}

fn backendIndependentLoadError(err: anyerror) bool {
    return err == error.InvalidMetadata or
        err == error.ModelAssetOutsideRoot or
        err == error.MultiStageReaderNotYetSupported or
        err == error.UnsupportedDetectionPostProcessor;
}

test "multistage asset resolver rejects symlinks outside the model root" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.createDir(std.testing.io, "model", .default_dir);
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model/inside.onnx",
        .data = "inside",
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "outside.onnx",
        .data = "outside",
    });

    const base = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(base);
    const model_path = try std.fs.path.join(allocator, &.{ base, "model" });
    defer allocator.free(model_path);
    const outside_path = try std.fs.path.join(allocator, &.{ base, "outside.onnx" });
    defer allocator.free(outside_path);
    const canonical_outside = try realPathAlloc(
        allocator,
        std.testing.io,
        outside_path,
    );
    defer allocator.free(canonical_outside);
    const escape_path = try std.fs.path.join(allocator, &.{ model_path, "escape.onnx" });
    defer allocator.free(escape_path);
    try std.Io.Dir.cwd().symLink(
        std.testing.io,
        canonical_outside,
        escape_path,
        .{},
    );

    var resolver = try AssetResolver.init(allocator, std.testing.io, model_path);
    defer resolver.deinit();
    const inside = try resolver.resolve("inside.onnx");
    defer allocator.free(inside);
    try std.testing.expect(pathIsWithinRoot(resolver.canonical_root, inside));
    try std.testing.expectError(
        error.ModelAssetOutsideRoot,
        resolver.resolve("escape.onnx"),
    );
}

test "multistage backend-independent failures stop fallback" {
    try std.testing.expect(backendIndependentLoadError(error.InvalidMetadata));
    try std.testing.expect(backendIndependentLoadError(error.ModelAssetOutsideRoot));
    try std.testing.expect(!backendIndependentLoadError(error.NoBackendAvailable));
    try std.testing.expect(!backendIndependentLoadError(error.ResourceTemporarilyUnavailable));
}

fn defaultPreprocessConfig(stage_kind: StageKind) multistage_ocr.PreprocessConfig {
    return switch (stage_kind) {
        .detection => .{ .width = 960, .height = 960 },
        .recognition => .{ .width = 320, .height = 48 },
    };
}

fn applyModelTypeNormalization(model_type: []const u8, _: StageKind, config: *multistage_ocr.PreprocessConfig) void {
    if (std.mem.eql(u8, model_type, "paddleocr")) {
        config.mean = .{ 0.485, 0.456, 0.406 };
        config.std = .{ 0.229, 0.224, 0.225 };
        config.rescale_factor = 1.0 / 255.0;
    }
}

fn applyModelTypeStageDefaults(model_type: []const u8, stage_kind: StageKind, config: *multistage_ocr.PreprocessConfig) void {
    if (std.mem.eql(u8, model_type, "paddleocr") and stage_kind == .recognition) {
        config.keep_aspect_ratio = true;
        config.pad_value_rgb = .{ 255, 255, 255 };
    }
}

fn applySessionShapeOverrides(
    config: *multistage_ocr.PreprocessConfig,
    loaded_stage_preprocessor: bool,
    stage_kind: StageKind,
    shape: []const i64,
) void {
    if (shape.len != 4) return;
    const should_apply_session_shape = !loaded_stage_preprocessor or stage_kind == .recognition;
    if (!should_apply_session_shape) return;

    if (shape[2] > 0) config.height = @intCast(shape[2]);
    if (shape[3] > 0) config.width = @intCast(shape[3]);
    if (stage_kind == .recognition and shape[3] <= 0) config.dynamic_width = true;
}

fn parsePreprocessorConfig(bytes: []const u8, config: *multistage_ocr.PreprocessConfig) !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPreprocessorConfig;
    const obj = parsed.value.object;

    if (obj.get("size")) |size_val| {
        if (jsonGetSize(size_val)) |size| {
            config.width = size.width;
            config.height = size.height;
        }
    } else if (obj.get("crop_size")) |size_val| {
        if (jsonGetSize(size_val)) |size| {
            config.width = size.width;
            config.height = size.height;
        }
    }
    if (obj.get("image_mean")) |value| {
        if (jsonGetFloatArray3(value)) |mean| config.mean = mean;
    }
    if (obj.get("image_std")) |value| {
        if (jsonGetFloatArray3(value)) |stdv| config.std = stdv;
    }
    if (obj.get("rescale_factor")) |value| {
        if (jsonGetFloat(value)) |factor| config.rescale_factor = factor;
    }
    if (obj.get("resample")) |value| {
        if (jsonGetResample(value)) |resample| config.resample = resample;
    }
}

const Size = struct {
    width: u32,
    height: u32,
};

fn jsonGetSize(value: std.json.Value) ?Size {
    return switch (value) {
        .integer => |v| .{ .width = @intCast(v), .height = @intCast(v) },
        .float => |v| .{ .width = @intFromFloat(v), .height = @intFromFloat(v) },
        .array => |items| blk: {
            if (items.items.len == 0) break :blk null;
            if (items.items.len >= 2) {
                const width = jsonGetInt(items.items[0]) orelse break :blk null;
                const height = jsonGetInt(items.items[1]) orelse break :blk null;
                break :blk .{
                    .width = @intCast(width),
                    .height = @intCast(height),
                };
            }
            const size = jsonGetInt(items.items[0]) orelse break :blk null;
            break :blk .{
                .width = @intCast(size),
                .height = @intCast(size),
            };
        },
        .object => |obj| blk: {
            const width = if (obj.get("width")) |item| jsonGetInt(item) else null;
            const height = if (obj.get("height")) |item| jsonGetInt(item) else null;
            if (width != null and height != null) break :blk .{
                .width = @intCast(width.?),
                .height = @intCast(height.?),
            };
            if (obj.get("shortest_edge")) |item| {
                const size = jsonGetInt(item) orelse break :blk null;
                break :blk .{
                    .width = @intCast(size),
                    .height = @intCast(size),
                };
            }
            break :blk null;
        },
        else => null,
    };
}

fn jsonGetInt(value: std.json.Value) ?i64 {
    return switch (value) {
        .integer => |v| v,
        .float => |v| @intFromFloat(v),
        else => null,
    };
}

fn jsonGetFloat(value: std.json.Value) ?f32 {
    return switch (value) {
        .float => |v| @floatCast(v),
        .integer => |v| @floatFromInt(v),
        else => null,
    };
}

fn jsonGetFloatArray3(value: std.json.Value) ?[3]f32 {
    if (value != .array or value.array.items.len < 3) return null;
    var result: [3]f32 = undefined;
    for (0..3) |i| {
        result[i] = jsonGetFloat(value.array.items[i]) orelse return null;
    }
    return result;
}

fn jsonGetResample(value: std.json.Value) ?image.Resample {
    if (jsonGetInt(value)) |resample| {
        return switch (resample) {
            3 => .bicubic,
            2 => .bilinear,
            1 => .lanczos,
            0 => .nearest,
            else => .bilinear,
        };
    }
    return switch (value) {
        .string => |name| {
            if (std.ascii.eqlIgnoreCase(name, "nearest")) return .nearest;
            if (std.ascii.eqlIgnoreCase(name, "bilinear")) return .bilinear;
            if (std.ascii.eqlIgnoreCase(name, "bicubic")) return .bicubic;
            if (std.ascii.eqlIgnoreCase(name, "lanczos")) return .lanczos;
            return null;
        },
        else => null,
    };
}

test "parsePreprocessorConfig reads rectangular size" {
    var config = defaultPreprocessConfig(.detection);
    try parsePreprocessorConfig(
        \\{
        \\  "size": { "width": 640, "height": 320 },
        \\  "image_mean": [0.1, 0.2, 0.3],
        \\  "image_std": [0.9, 0.8, 0.7]
        \\}
    , &config);

    try std.testing.expectEqual(@as(u32, 640), config.width);
    try std.testing.expectEqual(@as(u32, 320), config.height);
    try std.testing.expectApproxEqAbs(@as(f32, 0.1), config.mean[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.7), config.std[2], 1e-6);
}

test "parsePreprocessorConfig reads array size shortest-edge fallback and rescale factor" {
    var config = defaultPreprocessConfig(.recognition);
    try parsePreprocessorConfig(
        \\{
        \\  "size": [320, 48],
        \\  "rescale_factor": 1.0,
        \\  "resample": "nearest"
        \\}
    , &config);

    try std.testing.expectEqual(@as(u32, 320), config.width);
    try std.testing.expectEqual(@as(u32, 48), config.height);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.rescale_factor, 1e-6);
    try std.testing.expectEqual(image.Resample.nearest, config.resample);

    try parsePreprocessorConfig(
        \\{
        \\  "size": { "shortest_edge": 512 }
        \\}
    , &config);

    try std.testing.expectEqual(@as(u32, 512), config.width);
    try std.testing.expectEqual(@as(u32, 512), config.height);
}

test "applyModelTypeNormalization only provides fallback defaults" {
    var config = multistage_ocr.PreprocessConfig{
        .width = 320,
        .height = 48,
        .mean = .{ 0.1, 0.2, 0.3 },
        .std = .{ 0.9, 0.8, 0.7 },
        .rescale_factor = 1.0,
    };

    applyModelTypeNormalization("paddleocr", .recognition, &config);
    try std.testing.expectApproxEqAbs(@as(f32, 0.485), config.mean[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.229), config.std[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0 / 255.0), config.rescale_factor, 1e-6);
}

test "detection stage keeps loaded preprocessor size while recognition still follows model shape" {
    var detection = defaultPreprocessConfig(.detection);
    try parsePreprocessorConfig(
        \\{
        \\  "size": { "width": 640, "height": 320 }
        \\}
    , &detection);
    applySessionShapeOverrides(&detection, true, .detection, &.{ 1, 3, 960, 960 });
    try std.testing.expectEqual(@as(u32, 640), detection.width);
    try std.testing.expectEqual(@as(u32, 320), detection.height);

    var recognition = defaultPreprocessConfig(.recognition);
    try parsePreprocessorConfig(
        \\{
        \\  "size": { "width": 640, "height": 320 }
        \\}
    , &recognition);
    applySessionShapeOverrides(&recognition, true, .recognition, &.{ 1, 3, 48, -1 });
    try std.testing.expectEqual(@as(u32, 640), recognition.width);
    try std.testing.expectEqual(@as(u32, 48), recognition.height);
    try std.testing.expect(recognition.dynamic_width);
}
