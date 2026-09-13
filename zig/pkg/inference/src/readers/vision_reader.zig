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
const reading_pipeline_mod = @import("../pipelines/reading.zig");
const image = @import("../pipelines/image.zig");
const enc_dec_mod = @import("../pipelines/encoder_decoder.zig");
const reader_types = @import("types.zig");
const metal_generated_quant_stats = @import("../metal_generated_quant_stats.zig");
const antfly_image = @import("antfly_image");
const vision_config = @import("vision_config.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const PreprocessorConfig = vision_config.PreprocessorConfig;

pub const LoadedVisionReader = struct {
    allocator: std.mem.Allocator,
    encoder_session: backends.Session,
    decoder_session: backends.Session,
    dec_config: enc_dec_mod.DecoderConfig,
    preproc: PreprocessorConfig,
    loaded_model: ?*model_manager_mod.LoadedModel = null,
    loaded_model_handle: ?model_manager_mod.ModelHandle = null,
    managed_hf_tok: ?model_manager_mod.ManagedHfTokenizer = null,
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
        return loadFromDirWithControl(allocator, model_path, session_manager, model_manager, null);
    }

    pub fn loadFromDirWithControl(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        session_manager: *backends.SessionManager,
        model_manager: *model_manager_mod.ModelManager,
        control: ?InferenceExecutionControl,
    ) !LoadedVisionReader {
        if (control) |active| try active.check();
        if (enc_dec_mod.findEncoderDecoderPaths(allocator, model_path)) |paths| {
            defer allocator.free(paths.encoder);
            defer allocator.free(paths.decoder);

            const dec_config = enc_dec_mod.loadDecoderConfig(allocator, model_path) catch enc_dec_mod.DecoderConfig{};
            var loader = try model_manager.componentLoaderForPaths(
                model_path,
                session_manager.preferred_backends,
                &.{ paths.encoder, paths.decoder },
            );
            return loadEncoderDecoderPaths(allocator, model_path, paths.encoder, paths.decoder, dec_config, loadPreprocessorConfig(allocator, model_path), &loader, null, control);
        } else |_| {}

        var model_handle = (if (control) |active|
            model_manager.acquireFromDirWithControl(model_path, active)
        else
            model_manager.acquireFromDir(model_path)) catch |err| {
            std.log.err("reader native model load failed model={s} err={t}", .{ model_path, err });
            return err;
        };
        errdefer model_handle.release();
        const model = model_handle.get();
        var reader = try loadFromBorrowedModel(allocator, model_path, model);
        reader.loaded_model_handle = model_handle;
        return reader;
    }

    /// Construct the lightweight reader wrapper around an already fenced
    /// immutable model generation. The caller must retain its ModelHandle until
    /// this reader is deinitialized.
    pub fn loadFromBorrowedModel(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        model: *model_manager_mod.LoadedModel,
    ) !LoadedVisionReader {
        const runtime_config = model.florence_reader_config orelse return error.IncompleteFlorence2Bundle;
        const dec_config = runtime_config.decoder;
        const florence_config = session_factory.getFlorenceConfig(model.session) orelse {
            std.log.err(
                "reader model resolved without a Florence session requested_path={s} loaded_path={s} backend={s}",
                .{ model_path, model.model_dir, @tagName(model.session.backend()) },
            );
            return error.InvalidModelForReading;
        };
        const preproc = runtime_config.preprocessor;
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
            .florence_final_logits_bias_zero = runtime_config.final_logits_bias_zero,
        };
    }

    pub fn loadFromStagePaths(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
    ) !LoadedVisionReader {
        return loadFromStagePathsWithControl(
            allocator,
            model_path,
            encoder_path,
            decoder_path,
            component_loader,
            null,
        );
    }

    pub fn loadFromStagePathsWithControl(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
        control: ?InferenceExecutionControl,
    ) !LoadedVisionReader {
        const dec_config = enc_dec_mod.loadDecoderConfig(allocator, model_path) catch enc_dec_mod.DecoderConfig{};
        const preproc = loadPreprocessorConfig(allocator, model_path);

        return loadEncoderDecoderPaths(allocator, model_path, encoder_path, decoder_path, dec_config, preproc, component_loader, null, control);
    }

    pub fn loadFromStagePathsWithTokenizer(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
        managed_tokenizer: *model_manager_mod.ManagedHfTokenizer,
    ) !LoadedVisionReader {
        return loadFromStagePathsWithTokenizerAndControl(
            allocator,
            model_path,
            encoder_path,
            decoder_path,
            component_loader,
            managed_tokenizer,
            null,
        );
    }

    pub fn loadFromStagePathsWithTokenizerAndControl(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
        managed_tokenizer: *model_manager_mod.ManagedHfTokenizer,
        control: ?InferenceExecutionControl,
    ) !LoadedVisionReader {
        const dec_config = enc_dec_mod.loadDecoderConfig(allocator, model_path) catch enc_dec_mod.DecoderConfig{};
        const preproc = loadPreprocessorConfig(allocator, model_path);
        return loadEncoderDecoderPaths(
            allocator,
            model_path,
            encoder_path,
            decoder_path,
            dec_config,
            preproc,
            component_loader,
            managed_tokenizer,
            control,
        );
    }

    fn loadEncoderDecoderPaths(
        allocator: std.mem.Allocator,
        model_path: []const u8,
        encoder_path: []const u8,
        decoder_path: []const u8,
        dec_config: enc_dec_mod.DecoderConfig,
        preproc: PreprocessorConfig,
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
        preloaded_tokenizer: ?*model_manager_mod.ManagedHfTokenizer,
        control: ?InferenceExecutionControl,
    ) !LoadedVisionReader {
        var encoder_managed = try loadComponent(component_loader, encoder_path, control);
        errdefer encoder_managed.deinit();
        const encoder_session = encoder_managed.session;

        var strict_loader = try component_loader.restrictToBackend(encoder_session.backend());
        var decoder_managed = try loadComponent(&strict_loader, decoder_path, control);
        errdefer decoder_managed.deinit();
        const decoder_session = decoder_managed.session;

        const tok_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_path});
        defer allocator.free(tok_path);
        var loaded_tokenizer = if (preloaded_tokenizer) |managed|
            managed.take()
        else
            try component_loader.loadHfTokenizerFile(tok_path);
        errdefer loaded_tokenizer.deinit();
        if (control) |active| try active.check();

        return .{
            .allocator = allocator,
            .encoder_session = encoder_session,
            .decoder_session = decoder_session,
            .dec_config = dec_config,
            .preproc = preproc,
            .managed_hf_tok = loaded_tokenizer,
            .owns_sessions = true,
            .encoder_managed = encoder_managed,
            .decoder_managed = decoder_managed,
        };
    }

    fn loadComponent(
        component_loader: *const model_manager_mod.ModelManager.ComponentLoader,
        model_path: []const u8,
        control: ?InferenceExecutionControl,
    ) !model_manager_mod.ManagedSession {
        if (control) |active| return component_loader.loadWithControl(model_path, active);
        return component_loader.load(model_path);
    }

    pub fn deinit(self: *LoadedVisionReader) void {
        if (self.managed_hf_tok) |*managed| managed.deinit();
        if (self.owns_sessions) {
            if (self.encoder_managed) |*managed| managed.deinit() else self.encoder_session.close();
            if (self.decoder_managed) |*managed| managed.deinit() else self.decoder_session.close();
        }
        if (self.loaded_model_handle) |*handle| handle.release();
    }

    pub fn readRaw(self: *LoadedVisionReader, image_data: []const u8, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.read(image_data);
    }

    pub fn readRawBatch(self: *LoadedVisionReader, image_datas: []const []const u8, options: reader_types.ReadOptions) ![]reading_pipeline_mod.ReadResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.readBatch(image_datas);
    }

    pub fn readRawBatchReported(self: *LoadedVisionReader, image_datas: []const []const u8, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadBatchResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.readBatchReported(image_datas);
    }

    pub fn readBorrowedRasterBatchReported(
        self: *LoadedVisionReader,
        rasters: []const antfly_image.BorrowedRasterAttachment,
        options: reader_types.ReadOptions,
    ) !reading_pipeline_mod.ReadBatchResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.readBorrowedRasterBatchReported(rasters);
    }

    pub fn readDecodedRaw(self: *LoadedVisionReader, img: image.Image, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadResult {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.readDecoded(img);
    }

    pub fn inputTokenCount(self: *LoadedVisionReader, options: reader_types.ReadOptions) !usize {
        var reader_pipeline = try self.pipeline(options);
        return reader_pipeline.inputTokenCount();
    }

    pub fn snapshotMetalGeneratedQuantStats(self: *LoadedVisionReader, allocator: std.mem.Allocator) metal_generated_quant_stats.Stats {
        var stats = metal_generated_quant_stats.snapshotForSession(allocator, self.encoder_session);
        if (self.decoder_session.ptr != self.encoder_session.ptr or self.decoder_session.vtable != self.encoder_session.vtable) {
            stats = stats.add(metal_generated_quant_stats.snapshotForSession(allocator, self.decoder_session));
        }
        return stats;
    }

    fn pipeline(self: *LoadedVisionReader, options: reader_types.ReadOptions) !reading_pipeline_mod.ReadingPipeline {
        if (options.execution_control) |control| try control.check();
        const prefix_len: usize = if (self.dec_config.forced_bos_token_id == null) 1 else 2;
        const max_length = try resolveMaxLength(self.dec_config.max_length, options.max_tokens, prefix_len);
        var reader_pipeline = reading_pipeline_mod.ReadingPipeline.init(
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
                .source_fingerprint = options.source_fingerprint,
                .preprocess_io = if (self.loaded_model) |model| model.executor_io else null,
            },
            self.florence_final_logits_bias_zero,
        );
        reader_pipeline.execution_control = options.execution_control;
        return reader_pipeline;
    }

    fn tokenizer(self: *LoadedVisionReader) tokenizer_mod.Tokenizer {
        if (self.loaded_model) |model| return model.getTokenizer();
        if (self.managed_hf_tok) |*managed| return managed.tokenizer.tokenizer();
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

pub fn isSupportedModelDir(allocator: std.mem.Allocator, model_path: []const u8) !bool {
    var man = try manifest_mod.loadListingFromDir(allocator, model_path);
    defer man.deinit();
    return try enc_dec_mod.hasEncoderDecoderPaths(allocator, model_path, man) or isSupportedManifest(man);
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

pub const loadPreprocessorConfig = vision_config.loadPreprocessorConfig;

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

    try std.testing.expect(try isSupportedModelDir(allocator, model_dir));
}
