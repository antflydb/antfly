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
const manifest_mod = @import("../models/manifest.zig");
const session_factory = @import("../architectures/session_factory.zig");
const model_manager_mod = @import("../server/model_manager.zig");
const tokenizer_mod = @import("inference_tokenizer");
const hf_tokenizer = @import("inference_hf_tokenizer");
const reading_pipeline_mod = @import("../pipelines/reading.zig");
const image = @import("../pipelines/image.zig");
const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
const reader_types = @import("types.zig");
const c_file = @import("../util/c_file.zig");

pub const PreprocessorConfig = struct {
    image_size: usize = 384,
    image_seq_length: usize = 0,
    resample: image.Resample = .bilinear,
    image_mean: [3]f32 = .{ 0.5, 0.5, 0.5 },
    image_std: [3]f32 = .{ 0.5, 0.5, 0.5 },
    pix2struct_max_patches: usize = 0,
    pix2struct_patch_height: usize = 0,
    pix2struct_patch_width: usize = 0,
    pix2struct_do_normalize: bool = false,
};

pub const LoadedVisionReader = struct {
    allocator: std.mem.Allocator,
    encoder_session: backends.Session,
    decoder_session: backends.Session,
    dec_config: enc_dec_mod.DecoderConfig,
    preproc: PreprocessorConfig,
    loaded_model: ?*model_manager_mod.LoadedModel = null,
    hf_tok: ?*hf_tokenizer.HfTokenizer = null,
    owns_sessions: bool = false,
    encoder_managed: ?model_manager_mod.ManagedSession = null,
    decoder_managed: ?model_manager_mod.ManagedSession = null,
    florence_final_logits_bias_zero: ?bool = null,

    pub fn loadFromDir(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        session_manager: *backends.SessionManager,
        model_manager: *model_manager_mod.ModelManager,
    ) !LoadedVisionReader {
        const dec_config = enc_dec_mod.loadDecoderConfig(allocator, model_path) catch enc_dec_mod.DecoderConfig{};

        if (enc_dec_mod.findEncoderDecoderPaths(allocator, model_path)) |paths| {
            defer allocator.free(paths.encoder);
            defer allocator.free(paths.decoder);

            var loader = try model_manager.componentLoaderForPaths(
                model_path,
                session_manager.preferred_backends,
                &.{ paths.encoder, paths.decoder },
            );
            return loadEncoderDecoderPaths(allocator, model_path, paths.encoder, paths.decoder, dec_config, loadPreprocessorConfig(allocator, model_path), &loader);
        } else |_| {}

        const model = try model_manager.loadFromDir(model_path);
        const florence_config = session_factory.getFlorenceConfig(model.session) orelse return error.InvalidModelForReading;
        const preproc_path = model.manifest.preprocessor_config_path orelse return error.IncompleteFlorence2Bundle;
        const preproc = try loadPreprocessorConfigFile(allocator, preproc_path);
        if (preproc.image_size != @as(usize, florence_config.image_size)) {
            std.log.err("Florence preprocessor image size {d} does not match model image size {d}", .{ preproc.image_size, florence_config.image_size });
            return error.InvalidPreprocessorConfig;
        }

        return .{
            .allocator = allocator,
            .encoder_session = model.session,
            .decoder_session = model.session,
            .dec_config = dec_config,
            .preproc = preproc,
            .loaded_model = model,
            .owns_sessions = false,
        };
    }

    pub fn loadFromStagePaths(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
    ) !LoadedVisionReader {
        const dec_config = enc_dec_mod.loadDecoderConfig(allocator, model_path) catch enc_dec_mod.DecoderConfig{};
        const preproc = loadPreprocessorConfig(allocator, model_path);

        return loadEncoderDecoderPaths(allocator, model_path, encoder_path, decoder_path, dec_config, preproc, component_loader);
    }

    fn loadEncoderDecoderPaths(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        dec_config: enc_dec_mod.DecoderConfig,
        preproc: PreprocessorConfig,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
    ) !LoadedVisionReader {
        var encoder_managed = try component_loader.load(encoder_path);
        errdefer encoder_managed.deinit();
        const encoder_session = encoder_managed.session;

        var strict_loader = try component_loader.restrictToBackend(encoder_session.backend());
        var decoder_managed = try strict_loader.load(decoder_path);
        errdefer decoder_managed.deinit();
        const decoder_session = decoder_managed.session;

        const tok_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_path});
        defer allocator.free(tok_path);
        const tok_bytes = try c_file.readFile(allocator, tok_path);
        defer allocator.free(tok_bytes);

        const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
        errdefer tok.deinitSelf();

        return .{
            .allocator = allocator,
            .encoder_session = encoder_session,
            .decoder_session = decoder_session,
            .dec_config = dec_config,
            .preproc = preproc,
            .hf_tok = tok,
            .owns_sessions = true,
            .encoder_managed = encoder_managed,
            .decoder_managed = decoder_managed,
        };
    }

    pub fn deinit(self: *LoadedVisionReader) void {
        if (self.hf_tok) |tok| tok.deinitSelf();
        if (self.owns_sessions) {
            if (self.encoder_managed) |*managed| managed.deinit() else self.encoder_session.close();
            if (self.decoder_managed) |*managed| managed.deinit() else self.decoder_session.close();
        }
    }

    pub fn readRaw(self: *LoadedVisionReader, image_data: []const u8, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.read(image_data);
    }

    pub fn readRawBatch(self: *LoadedVisionReader, image_datas: []const []const u8, options: reader_types.ReadOptions) ![]reading_pipeline_mod.ReadResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.readBatch(image_datas);
    }

    pub fn readDecodedRaw(self: *LoadedVisionReader, img: image.Image, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.readDecoded(img);
    }

    fn pipeline(self: *LoadedVisionReader, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadingPipeline {
        const prefix_len: usize = if (self.dec_config.forced_bos_token_id == null) 1 else 2;
        const max_length = try resolveMaxLength(self.dec_config.max_length, options.max_tokens, prefix_len);
        return reading_pipeline_mod.ReadingPipeline.init(
            self.allocator,
            self.encoder_session,
            self.decoder_session,
            self.tokenizer(),
            .{
                .max_length = max_length,
                .decoder_start_token_id = self.dec_config.decoder_start_token_id,
                .eos_token_id = self.dec_config.eos_token_id,
                .pad_token_id = self.dec_config.pad_token_id,
                .forced_bos_token_id = self.dec_config.forced_bos_token_id,
                .no_repeat_ngram_size = self.dec_config.no_repeat_ngram_size,
                .image_size = self.preproc.image_size,
                .image_seq_length = self.preproc.image_seq_length,
                .resample = self.preproc.resample,
                .image_mean = self.preproc.image_mean,
                .image_std = self.preproc.image_std,
                .pix2struct_max_patches = self.preproc.pix2struct_max_patches,
                .pix2struct_patch_height = self.preproc.pix2struct_patch_height,
                .pix2struct_patch_width = self.preproc.pix2struct_patch_width,
                .pix2struct_do_normalize = self.preproc.pix2struct_do_normalize,
                .prompt = options.prompt,
            },
            &self.florence_final_logits_bias_zero,
        );
    }

    fn tokenizer(self: *LoadedVisionReader) tokenizer_mod.Tokenizer {
        if (self.loaded_model) |model| return model.getTokenizer();
        if (self.hf_tok) |tok| return tok.tokenizer();
        unreachable;
    }
};

pub fn resolveMaxLength(model_max: usize, requested: ?usize, prefix_len: usize) !usize {
    if (prefix_len == 0 or prefix_len > model_max) return error.InvalidMaxTokens;
    const max_length = if (requested) |max_tokens|
        std.math.add(usize, prefix_len, max_tokens) catch return error.InvalidMaxTokens
    else
        model_max;
    if (max_length == prefix_len or max_length > model_max) return error.InvalidMaxTokens;
    return max_length;
}

pub fn isSupportedModelDir(allocator: std.mem.Allocator, model_path: []const u8) bool {
    if (enc_dec_mod.findEncoderDecoderPaths(allocator, model_path)) |paths| {
        allocator.free(paths.encoder);
        allocator.free(paths.decoder);
        return true;
    } else |_| {}

    var man = manifest_mod.loadFromDir(allocator, model_path) catch return false;
    defer man.deinit();

    return isSupportedManifest(man);
}

/// Same check against a manifest the caller already has.
///
/// Only `native_arch_hint` and the artifact paths matter here, all of which
/// `loadListingFromDir` populates. Callers in listing paths should prefer this: a full
/// `loadFromDir` parses GGUF tokenizer metadata, which for a large vocab costs over a
/// second per model.
pub fn isSupportedManifest(man: manifest_mod.ModelManifest) bool {
    return man.native_arch_hint == .florence and
        (man.gguf_path != null or man.safetensors_path != null or man.safetensors_index_path != null);
}

pub fn loadPreprocessorConfig(allocator: std.mem.Allocator, model_dir: []const u8) PreprocessorConfig {
    const path = std.fmt.allocPrint(allocator, "{s}/preprocessor_config.json", .{model_dir}) catch return .{};
    defer allocator.free(path);

    return loadPreprocessorConfigFile(allocator, path) catch .{};
}

fn loadPreprocessorConfigFile(allocator: std.mem.Allocator, path: []const u8) !PreprocessorConfig {
    const data = try c_file.readFile(allocator, path);
    defer allocator.free(data);

    var config = PreprocessorConfig{};
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPreprocessorConfig;

    const obj = parsed.value.object;
    if (obj.get("size")) |size_val| {
        if (jsonValueGetSize(size_val)) |v| config.image_size = v;
    } else if (obj.get("crop_size")) |crop_val| {
        if (jsonValueGetSize(crop_val)) |v| config.image_size = v;
    }
    if (obj.get("image_seq_length")) |v| {
        if (jsonValueGetUsize(v)) |parsed_int| config.image_seq_length = parsed_int;
    }
    if (obj.get("resample")) |v| {
        if (jsonValueGetUsize(v)) |parsed_int| {
            config.resample = switch (parsed_int) {
                3 => .bicubic,
                2 => .bilinear,
                0 => .nearest,
                else => .bilinear,
            };
        }
    }
    if (obj.get("image_mean")) |v| {
        if (jsonValueGetFloatArray3(v)) |mean| config.image_mean = mean;
    }
    if (obj.get("image_std")) |v| {
        if (jsonValueGetFloatArray3(v)) |stddev| config.image_std = stddev;
    }
    if (obj.get("max_patches")) |v| {
        if (jsonValueGetUsize(v)) |parsed_int| config.pix2struct_max_patches = parsed_int;
    }
    if (obj.get("do_normalize")) |v| {
        switch (v) {
            .bool => |parsed_bool| config.pix2struct_do_normalize = parsed_bool,
            else => {},
        }
    }
    if (obj.get("patch_size")) |v| {
        if (v == .object) {
            if (v.object.get("height")) |height_val| {
                if (jsonValueGetUsize(height_val)) |parsed_int| config.pix2struct_patch_height = parsed_int;
            }
            if (v.object.get("width")) |width_val| {
                if (jsonValueGetUsize(width_val)) |parsed_int| config.pix2struct_patch_width = parsed_int;
            }
        }
    }

    return config;
}

fn jsonValueGetSize(val: std.json.Value) ?usize {
    return switch (val) {
        .integer => |i| @intCast(i),
        .object => |obj| blk: {
            if (obj.get("height")) |h| {
                if (jsonValueGetUsize(h)) |parsed| break :blk parsed;
            }
            if (obj.get("width")) |w| {
                if (jsonValueGetUsize(w)) |parsed| break :blk parsed;
            }
            break :blk null;
        },
        else => null,
    };
}

fn jsonValueGetUsize(val: std.json.Value) ?usize {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn jsonValueGetFloatArray3(val: std.json.Value) ?[3]f32 {
    if (val != .array or val.array.items.len < 3) return null;
    var result: [3]f32 = undefined;
    for (0..3) |i| {
        result[i] = switch (val.array.items[i]) {
            .float => |f| @floatCast(f),
            .integer => |n| @floatFromInt(n),
            else => return null,
        };
    }
    return result;
}

test "vision reader supports gguf-backed native Florence directories" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"florence2\",\"text_config\":{\"d_model\":768},\"vision_config\":{\"image_size\":768}}",
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "florence-2-base.Q4_K.gguf", .data = "GGUFstub" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    try std.testing.expect(isSupportedModelDir(allocator, model_dir));
}
