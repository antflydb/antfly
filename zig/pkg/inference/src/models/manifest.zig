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

// Model manifest and config loading.
//
// Auto-detects model layout from a directory: ONNX files, tokenizer type,
// config.json, tokenizer_config.json, and optional model_manifest.json.

const std = @import("std");
const Dir = std.Io.Dir;
const bert = @import("bert.zig");
const gpt = @import("gpt.zig");
const compat = @import("../io/compat.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const managed_receipt = @import("../registry/managed_receipt.zig");
const build_options = @import("build_options");
const jinja = @import("jinja");

/// Built-in chat template for Gemma 4 models (uses <|turn>/<turn|> tokens).
/// Applied when tokenizer_config.json has sot_token=<|turn> but no
/// chat_template, and when GGUF metadata carries the upstream tool template
/// that requires Jinja features outside our rendering subset.
const gemma4_chat_template =
    "{{ bos_token }}" ++
    "{%- if messages[0]['role'] == 'system' -%}" ++
    "{%- if messages[0]['content'] is string -%}" ++
    "{%- set first_user_prefix = messages[0]['content'] + '\\n\\n' -%}" ++
    "{%- else -%}" ++
    "{%- set first_user_prefix = messages[0]['content'][0]['text'] + '\\n\\n' -%}" ++
    "{%- endif -%}" ++
    "{%- set loop_messages = messages[1:] -%}" ++
    "{%- else -%}" ++
    "{%- set first_user_prefix = \"\" -%}" ++
    "{%- set loop_messages = messages -%}" ++
    "{%- endif -%}" ++
    "{%- for message in loop_messages -%}" ++
    "{%- if (message['role'] == 'assistant') -%}" ++
    "{%- set role = \"model\" -%}" ++
    "{%- else -%}" ++
    "{%- set role = message['role'] -%}" ++
    "{%- endif -%}" ++
    "{{ '<|turn>' + role + '\\n' + (first_user_prefix if loop.first else \"\") }}" ++
    "{%- if message['content'] is string -%}" ++
    "{{ message['content'] | trim }}" ++
    "{%- elif message['content'] is iterable -%}" ++
    "{%- for item in message['content'] -%}" ++
    "{%- if item['type'] == 'text' -%}" ++
    "{{ item['text'] | trim }}" ++
    "{%- elif item['type'] == 'image' -%}" ++
    "{{ '<|image|>' }}" ++
    "{%- elif item['type'] == 'audio' -%}" ++
    "{{ '<|audio|>' }}" ++
    "{%- endif -%}" ++
    "{%- endfor -%}" ++
    "{%- endif -%}" ++
    "{{ '<turn|>\\n' }}" ++
    "{%- endfor -%}" ++
    "{%- if add_generation_prompt -%}" ++
    "{%- if enable_thinking is defined and not enable_thinking -%}" ++
    "{{ '<|turn>model\\n<|channel>final\\n<channel|>' }}" ++
    "{%- else -%}" ++
    "{{ '<|turn>model\\n<|channel>thought\\n<channel|>' }}" ++
    "{%- endif -%}" ++
    "{%- endif -%}";

pub const ModelType = enum {
    embedder,
    reranker,
    chunker,
    generator,
    recognizer,
    rewriter,
    classifier,
    reader,
    transcriber,
};

/// Records why `model_type` was selected. The default enum value is a neutral
/// placeholder, not an explicit embedder declaration; compatibility policy
/// must be able to distinguish those cases before using artifact metadata to
/// infer a serving route.
pub const ModelTypeOrigin = enum {
    default,
    path,
    config,
    manifest,
    tasks,
    heuristic,
    bundle,
};

pub const TokenizerType = enum {
    huggingface, // tokenizer.json (WordPiece, BPE, etc.)
    sentencepiece, // tokenizer.model (SentencePiece protobuf)
};

pub const PoolingStrategy = enum {
    mean,
    cls,
    max,
    last,
};

pub const Sparse3DOutputLayout = enum {
    batch_seq,
    seq_batch,
};

pub const NativeArchHint = enum {
    none,
    whisper,
    clip,
    clap,
    florence,
    layoutlmv3,
};

/// Primary native weight source selected consistently by compatibility,
/// admission, export, and runtime loading. Explicit GGUF bundles retain their
/// declared route. Otherwise the canonical safetensors artifacts take
/// precedence over colocated GGUF exports, so writing `export.gguf` into a
/// model directory cannot silently change the model loaded on the next run.
pub const NativeWeightArtifactKind = enum {
    gguf,
    safetensors,
    sharded_safetensors,
};

/// SafeTensors file candidates in priority order.
pub const safetensors_candidates = [_][]const u8{
    "model.safetensors",
    "pytorch_model.safetensors",
};

pub const safetensors_index_candidates = [_][]const u8{
    "model.safetensors.index.json",
    "pytorch_model.safetensors.index.json",
};

/// Files read by `loadListingFromDir` whose paths are not necessarily retained
/// on ModelManifest. Compatibility caches must include every entry so their
/// decision describes the same metadata snapshot that listing parsed.
pub const listing_compatibility_sidecars = [_][]const u8{
    "antfly_inference_bundle.json",
    "antfly_inference_variants.json",
    "gliner_config.json",
    "added_tokens.json",
    "clip_config.json",
    "special_tokens_map.json",
    "1_SpladePooling/config.json",
};

/// Resolved model configuration loaded from a model directory.
pub const ModelManifest = struct {
    allocator: std.mem.Allocator,

    // Identity
    model_type: ModelType = .embedder,
    model_type_origin: ModelTypeOrigin = .default,

    // Files (allocated strings — absolute paths)
    onnx_path: ?[]const u8 = null,
    safetensors_path: ?[]const u8 = null,
    safetensors_index_path: ?[]const u8 = null,
    gguf_path: ?[]const u8 = null,
    gguf_projector_path: ?[]const u8 = null,
    gliner_head_gguf_path: ?[]const u8 = null,
    gliner_head_safetensors_path: ?[]const u8 = null,
    config_path: ?[]const u8 = null,
    model_manifest_path: ?[]const u8 = null,
    tokenizer_json_path: ?[]const u8 = null,
    tokenizer_config_path: ?[]const u8 = null,
    special_tokens_map_path: ?[]const u8 = null,
    preprocessor_config_path: ?[]const u8 = null,
    processor_config_path: ?[]const u8 = null,
    inference_bundle_family: []const u8 = "",
    tokenizer_type: ?TokenizerType = null,

    // Multimodal ONNX files (CLIP, CLAP, CLIPCLAP)
    visual_model_path: ?[]const u8 = null,
    audio_model_path: ?[]const u8 = null,
    text_projection_path: ?[]const u8 = null,
    visual_projection_path: ?[]const u8 = null,
    audio_projection_path: ?[]const u8 = null,

    // Architecture (from config.json)
    hidden_size: u32 = 768,
    intermediate_size: u32 = 3072,
    max_position_embeddings: u32 = 512,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    bert_vocab_size: u32 = 30522,
    bert_type_vocab_size: u32 = 2,
    bert_layer_norm_eps: f32 = 1e-12,
    bert_model_type: bert.ModelType = .bert,
    bert_pad_token_id: i64 = 0,
    config_model_arch: []const u8 = "",

    // Pipeline config
    pooling: PoolingStrategy = .mean,
    normalize: bool = true,
    embedding_text_prefix: []const u8 = "",
    sparse_3d_output_layout: ?Sparse3DOutputLayout = null,
    native_arch_hint: NativeArchHint = .none,

    // Classification / NER
    num_labels: u32 = 0,
    id2label: ?[][]const u8 = null,

    // Chat template (from chat_template.jinja or tokenizer_config.json)
    chat_template: ?[]const u8 = null,

    // GLiNER NER config (from gliner_config.json)
    gliner_max_width: u32 = 12,
    gliner_threshold: f32 = 0.5,
    gliner_flat_ner: bool = true,
    gliner_model_type: []const u8 = "", // "gliner2", "uniencoder", etc.
    gliner_default_labels: [][]const u8 = &.{},
    gliner_relation_labels: [][]const u8 = &.{},
    gliner_relation_threshold: f32 = 0.0,

    // GLiNER special token IDs (from added_tokens.json)
    gliner_token_p: i32 = 0, // [P] token ID
    gliner_token_c: i32 = 0, // [C] token ID
    gliner_token_e: i32 = 0, // [E] token ID
    gliner_token_r: i32 = 0, // [R] token ID
    gliner_token_sep_text: i32 = 0, // [SEP_TEXT] token ID

    // Capabilities (from model_manifest.json)
    tasks: [][]const u8 = &.{},
    capabilities: [][]const u8 = &.{},
    inputs: [][]const u8 = &.{},

    // Special tokens (from tokenizer_config.json)
    bos_token: []const u8 = "",
    eos_token: []const u8 = "",
    unk_token: []const u8 = "",
    pad_token: []const u8 = "",
    add_bos_token: bool = false,
    add_eos_token: bool = false,

    pub fn maxTextSequenceLength(self: *const ModelManifest) usize {
        const position_id_mode: bert.PositionIdMode = if (self.bert_model_type == .roberta)
            .roberta_padding
        else
            .absolute;
        const config = bert.Config{
            .max_position_embeddings = self.max_position_embeddings,
            .pad_token_id = self.bert_pad_token_id,
            .position_id_mode = position_id_mode,
        };
        return config.maxSequenceLength();
    }

    pub fn deinit(self: *ModelManifest) void {
        if (self.onnx_path) |p| self.allocator.free(p);
        if (self.safetensors_path) |p| self.allocator.free(p);
        if (self.safetensors_index_path) |p| self.allocator.free(p);
        if (self.gguf_path) |p| self.allocator.free(p);
        if (self.gguf_projector_path) |p| self.allocator.free(p);
        if (self.gliner_head_gguf_path) |p| self.allocator.free(p);
        if (self.gliner_head_safetensors_path) |p| self.allocator.free(p);
        if (self.config_path) |p| self.allocator.free(p);
        if (self.model_manifest_path) |p| self.allocator.free(p);
        if (self.tokenizer_json_path) |p| self.allocator.free(p);
        if (self.tokenizer_config_path) |p| self.allocator.free(p);
        if (self.special_tokens_map_path) |p| self.allocator.free(p);
        if (self.preprocessor_config_path) |p| self.allocator.free(p);
        if (self.processor_config_path) |p| self.allocator.free(p);
        if (self.inference_bundle_family.len > 0) self.allocator.free(self.inference_bundle_family);
        if (self.visual_model_path) |p| self.allocator.free(p);
        if (self.audio_model_path) |p| self.allocator.free(p);
        if (self.text_projection_path) |p| self.allocator.free(p);
        if (self.visual_projection_path) |p| self.allocator.free(p);
        if (self.audio_projection_path) |p| self.allocator.free(p);
        if (self.id2label) |labels| {
            for (labels) |l| {
                if (l.len > 0) self.allocator.free(l);
            }
            self.allocator.free(labels);
        }
        if (self.chat_template) |t| self.allocator.free(t);
        if (self.embedding_text_prefix.len > 0) self.allocator.free(self.embedding_text_prefix);
        if (self.gliner_model_type.len > 0) self.allocator.free(self.gliner_model_type);
        if (self.config_model_arch.len > 0) self.allocator.free(self.config_model_arch);
        if (self.gliner_default_labels.len > 0) {
            for (self.gliner_default_labels) |l| self.allocator.free(l);
            self.allocator.free(self.gliner_default_labels);
        }
        if (self.gliner_relation_labels.len > 0) {
            for (self.gliner_relation_labels) |l| self.allocator.free(l);
            self.allocator.free(self.gliner_relation_labels);
        }
        if (self.tasks.len > 0) {
            for (self.tasks) |task| self.allocator.free(task);
            self.allocator.free(self.tasks);
        }
        if (self.capabilities.len > 0) {
            for (self.capabilities) |c| self.allocator.free(c);
            self.allocator.free(self.capabilities);
        }
        if (self.inputs.len > 0) {
            for (self.inputs) |input| self.allocator.free(input);
            self.allocator.free(self.inputs);
        }
        if (self.bos_token.len > 0) self.allocator.free(self.bos_token);
        if (self.eos_token.len > 0) self.allocator.free(self.eos_token);
        if (self.unk_token.len > 0) self.allocator.free(self.unk_token);
        if (self.pad_token.len > 0) self.allocator.free(self.pad_token);
    }

    pub fn hasCapability(self: *const ModelManifest, cap: []const u8) bool {
        for (self.capabilities) |c| {
            if (std.mem.eql(u8, c, cap)) return true;
        }
        return false;
    }

    pub fn hasTask(self: *const ModelManifest, task: []const u8) bool {
        for (self.tasks) |candidate| {
            if (std.mem.eql(u8, candidate, task)) return true;
        }
        return false;
    }

    pub fn nativeWeightArtifactKind(self: *const ModelManifest) ?NativeWeightArtifactKind {
        if (self.gguf_path != null and self.hasExplicitGgufBundleRoute()) return .gguf;
        if (self.safetensors_path != null) return .safetensors;
        if (self.safetensors_index_path != null) return .sharded_safetensors;
        if (self.gguf_path != null) return .gguf;
        return null;
    }

    pub fn usesGgufWeights(self: *const ModelManifest) bool {
        const artifact = self.nativeWeightArtifactKind() orelse return false;
        return artifact == .gguf;
    }

    fn hasExplicitGgufBundleRoute(self: *const ModelManifest) bool {
        return self.isSplitGlinerBundle() or
            std.mem.eql(u8, self.inference_bundle_family, "colqwen2_gguf_bundle/v1") or
            self.isClipclapGgufBundle() or
            self.isFlorence2GgufBundle();
    }

    pub fn prefersGenerationEncodingForLateInteraction(self: *const ModelManifest) bool {
        if (self.config_model_arch.len == 0) return false;
        return gpt.isGenerativeModel(self.config_model_arch);
    }

    pub fn isSplitGlinerBundle(self: *const ModelManifest) bool {
        return self.gliner_model_type.len > 0 and self.gguf_path != null and (self.gliner_head_gguf_path != null or self.gliner_head_safetensors_path != null);
    }

    pub fn hasIncompleteGlinerBundle(self: *const ModelManifest) bool {
        if (self.gliner_model_type.len == 0) return false;
        const has_encoder_gguf = self.gguf_path != null;
        const has_head = self.gliner_head_gguf_path != null or self.gliner_head_safetensors_path != null;
        return has_encoder_gguf != has_head;
    }

    pub fn isColqwenBundle(self: *const ModelManifest) bool {
        if (std.mem.eql(u8, self.inference_bundle_family, "colqwen2_gguf_bundle/v1")) return true;
        if (!self.hasCapability("colqwen") and !self.hasCapability("multimodal_late_interaction")) return false;
        if (self.config_model_arch.len == 0) return false;
        return std.mem.eql(u8, self.config_model_arch, "qwen2") or std.mem.eql(u8, self.config_model_arch, "qwen2_vl");
    }

    pub fn hasIncompleteColqwenBundle(self: *const ModelManifest) bool {
        if (!self.isColqwenBundle()) return false;
        return self.gguf_path == null or
            self.config_path == null or
            self.model_manifest_path == null or
            self.tokenizer_json_path == null or
            self.tokenizer_config_path == null or
            self.preprocessor_config_path == null or
            self.processor_config_path == null;
    }

    pub fn isClipclapGgufBundle(self: *const ModelManifest) bool {
        return std.mem.eql(u8, self.inference_bundle_family, "clipclap_gguf_bundle/v1");
    }

    pub fn hasIncompleteClipclapGgufBundle(self: *const ModelManifest) bool {
        if (!self.isClipclapGgufBundle()) return false;
        return self.gguf_path == null or
            self.audio_model_path == null or
            self.model_manifest_path == null or
            self.tokenizer_json_path == null or
            self.tokenizer_config_path == null or
            self.processor_config_path == null;
    }

    pub fn isFlorence2GgufBundle(self: *const ModelManifest) bool {
        return std.mem.eql(u8, self.inference_bundle_family, "florence2_gguf_bundle/v1");
    }

    pub fn hasIncompleteFlorence2GgufBundle(self: *const ModelManifest) bool {
        if (!self.isFlorence2GgufBundle()) return false;
        return self.gguf_path == null or
            self.config_path == null or
            self.model_manifest_path == null or
            self.tokenizer_json_path == null or
            self.tokenizer_config_path == null or
            self.preprocessor_config_path == null;
    }

    pub fn hasInput(self: *const ModelManifest, input: []const u8) bool {
        for (self.inputs) |candidate| {
            if (std.mem.eql(u8, candidate, input)) return true;
        }
        return false;
    }
};

/// ONNX file candidates in priority order.
const onnx_candidates = [_][]const u8{
    "text_model.onnx",
    "text_model_f16.onnx",
    "text_model_i8.onnx",
    "model.onnx",
    "model_f16.onnx",
    "model_i8.onnx",
    "model_i8-st.onnx",
    "model_i4.onnx",
    "model_quantized.onnx",
    "decoder_model_merged.onnx",
    "decoder_model_merged_fp16.onnx",
    "decoder_model_merged_quantized.onnx",
    "decoder_model_merged_q4.onnx",
    "decoder_model_merged_q4f16.onnx",
    "encoder.onnx",
};

/// Visual model candidates for CLIP/SigLIP.
const visual_model_candidates = [_][]const u8{
    "visual_model.onnx",
    "visual_model_f16.onnx",
    "visual_model_i8.onnx",
    "visual_model_quantized.onnx",
    "vision_model.onnx",
    "vision_model_f16.onnx",
    "vision_model_i8.onnx",
    "vision_model_quantized.onnx",
    "vision_encoder.onnx",
    "vision_encoder_fp16.onnx",
    "vision_encoder_quantized.onnx",
    "vision_encoder_q4.onnx",
    "vision_encoder_q4f16.onnx",
};

/// Audio model candidates for CLAP.
const audio_model_candidates = [_][]const u8{
    "audio_model.onnx",
    "audio_model_quantized.onnx",
    "audio_model_fp16.onnx",
    "audio_encoder.onnx",
};

/// Audio projection candidates for CLIPCLAP.
const text_projection_candidates = [_][]const u8{
    "text_projection.onnx",
};

const visual_projection_candidates = [_][]const u8{
    "visual_projection.onnx",
};

const audio_projection_candidates = [_][]const u8{
    "audio_projection.onnx",
};

/// Subdirectories to search for ONNX files.
const onnx_subdirs = [_][]const u8{ "", "onnx" };

/// Optional metadata files may be malformed or use fields newer than this
/// binary understands. Keep those paths best-effort, but never reinterpret
/// resource exhaustion as absent metadata.
fn ignoreNonResourceMetadataError(result: anytype) !void {
    result catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return,
    };
}

const ArtifactCatalog = struct {
    allocator: std.mem.Allocator,
    model_dir_path: []const u8,
    receipt: ?managed_receipt.ValidatedReceipt = null,

    fn initPublished(
        allocator: std.mem.Allocator,
        model_dir_path: []const u8,
    ) !ArtifactCatalog {
        return .{
            .allocator = allocator,
            .model_dir_path = model_dir_path,
            .receipt = try managed_receipt.loadValidated(
                allocator,
                std.Options.debug_io,
                model_dir_path,
            ),
        };
    }

    fn initPlan(
        allocator: std.mem.Allocator,
        model_dir_path: []const u8,
    ) !ArtifactCatalog {
        return .{
            .allocator = allocator,
            .model_dir_path = model_dir_path,
            .receipt = try managed_receipt.loadValidatedPlan(
                allocator,
                std.Options.debug_io,
                model_dir_path,
            ),
        };
    }

    fn deinit(self: *ArtifactCatalog) void {
        if (self.receipt) |*receipt| receipt.deinit();
        self.* = undefined;
    }

    fn find(self: *const ArtifactCatalog, relative_path: []const u8) ?*const managed_receipt.ValidatedArtifact {
        if (self.receipt) |*receipt| return receipt.find(relative_path);
        return null;
    }

    fn readOptional(self: *const ArtifactCatalog, name: []const u8) !?[]u8 {
        if (self.receipt != null) {
            const artifact = self.find(name) orelse return null;
            return try c_file.readFile(self.allocator, artifact.canonical_path);
        }
        return c_file.readFileFromDir(self.allocator, self.model_dir_path, name) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
    }

    fn exists(self: *const ArtifactCatalog, relative_path: []const u8) bool {
        if (self.receipt != null) return self.find(relative_path) != null;
        return c_file.fileExistsInDir(self.allocator, self.model_dir_path, relative_path);
    }

    fn resolve(self: *const ArtifactCatalog, relative_path: []const u8) !?[]u8 {
        if (self.receipt != null) {
            const artifact = self.find(relative_path) orelse return null;
            return @as(?[]u8, try self.allocator.dupe(u8, artifact.canonical_path));
        }
        return managed_receipt.resolveContainedArtifactPath(
            self.allocator,
            std.Options.debug_io,
            self.model_dir_path,
            relative_path,
        ) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => return null,
            else => return err,
        };
    }
};

/// A direct GGUF path still belongs to the nearest managed model ancestor, if
/// one exists. Preserve standalone-file support while preventing an absolute
/// path from bypassing an in-progress marker or selecting an unreceipted file
/// inside a managed publication.
const DirectGgufArtifact = struct {
    allocator: std.mem.Allocator,
    path: ?[]u8,
    requested_path: []u8,
    catalog: ArtifactCatalog,

    fn init(allocator: std.mem.Allocator, requested_path: []const u8) !DirectGgufArtifact {
        const resolved_requested_path = try managed_receipt.resolveRequestedFilePath(
            allocator,
            std.Options.debug_io,
            requested_path,
        );
        errdefer allocator.free(resolved_requested_path);
        const canonical_path = try managed_receipt.resolveRegularFilePath(
            allocator,
            std.Options.debug_io,
            resolved_requested_path,
        );
        errdefer allocator.free(canonical_path);

        var ancestor = std.fs.path.dirname(resolved_requested_path) orelse ".";
        while (true) {
            if (try managed_receipt.loadValidated(
                allocator,
                std.Options.debug_io,
                ancestor,
            )) |receipt| {
                var catalog = ArtifactCatalog{
                    .allocator = allocator,
                    .model_dir_path = ancestor,
                    .receipt = receipt,
                };
                errdefer catalog.deinit();

                var relative_start = ancestor.len;
                while (relative_start < resolved_requested_path.len and
                    std.fs.path.isSep(resolved_requested_path[relative_start]))
                {
                    relative_start += 1;
                }
                const relative_path = try allocator.dupe(u8, resolved_requested_path[relative_start..]);
                defer allocator.free(relative_path);
                for (relative_path) |*byte| {
                    if (std.fs.path.isSep(byte.*)) byte.* = '/';
                }
                const artifact = receipt.find(relative_path) orelse
                    return error.ModelArtifactNotPublished;
                if (!std.mem.eql(u8, artifact.canonical_path, canonical_path))
                    return error.ModelArtifactNotPublished;
                return .{
                    .allocator = allocator,
                    .path = canonical_path,
                    .requested_path = resolved_requested_path,
                    .catalog = catalog,
                };
            }

            const parent = std.fs.path.dirname(ancestor) orelse break;
            if (std.mem.eql(u8, parent, ancestor)) break;
            ancestor = parent;
        }

        return .{
            .allocator = allocator,
            .path = canonical_path,
            .requested_path = resolved_requested_path,
            .catalog = .{
                .allocator = allocator,
                .model_dir_path = std.fs.path.dirname(resolved_requested_path) orelse ".",
            },
        };
    }

    fn takePath(self: *DirectGgufArtifact) []u8 {
        const path = self.path.?;
        self.path = null;
        return path;
    }

    fn deinit(self: *DirectGgufArtifact) void {
        self.catalog.deinit();
        if (self.path) |path| self.allocator.free(path);
        self.allocator.free(self.requested_path);
        self.* = undefined;
    }
};

fn readOptionalMetadataFile(
    allocator: std.mem.Allocator,
    model_dir_path: []const u8,
    name: []const u8,
) !?[]u8 {
    var catalog = try ArtifactCatalog.initPublished(allocator, model_dir_path);
    defer catalog.deinit();
    return catalog.readOptional(name);
}

test "optional metadata preserves non-missing open failures" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "not-a-directory", .data = "file" });

    const model_path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
        "not-a-directory",
    });
    defer allocator.free(model_path);

    try std.testing.expectError(
        error.NotDir,
        readOptionalMetadataFile(allocator, model_path, "model_manifest.json"),
    );
}

/// Load a model manifest by inspecting the directory contents and parsing configs.
pub fn loadFromDir(allocator: std.mem.Allocator, model_dir_path: []const u8) !ModelManifest {
    if (std.mem.endsWith(u8, model_dir_path, ".gguf")) {
        var direct = try DirectGgufArtifact.init(allocator, model_dir_path);
        defer direct.deinit();
        var manifest = ModelManifest{ .allocator = allocator };
        errdefer manifest.deinit();
        manifest.gguf_path = direct.takePath();
        try ignoreNonResourceMetadataError(applyGgufTokenizerMetadata(
            &manifest,
            allocator,
            &direct.catalog,
            direct.catalog.model_dir_path,
            manifest.gguf_path.?,
        ));
        return manifest;
    }

    var catalog = try ArtifactCatalog.initPublished(allocator, model_dir_path);
    defer catalog.deinit();
    return loadFromCatalog(allocator, &catalog);
}

/// Load a private pull staging directory from its validated artifact plan.
/// This API must only be used while holding the corresponding pull lock.
pub fn loadFromManagedPlanDir(allocator: std.mem.Allocator, model_dir_path: []const u8) !ModelManifest {
    var catalog = try ArtifactCatalog.initPlan(allocator, model_dir_path);
    defer catalog.deinit();
    return loadFromCatalog(allocator, &catalog);
}

fn loadFromCatalog(allocator: std.mem.Allocator, catalog: *const ArtifactCatalog) !ModelManifest {
    const model_dir_path = catalog.model_dir_path;
    var manifest = ModelManifest{ .allocator = allocator };
    errdefer manifest.deinit();

    if (inferModelTypeFromPath(model_dir_path)) |model_type| {
        manifest.model_type = model_type;
        manifest.model_type_origin = .path;
    }

    // Try to parse config.json, then clip_config.json for CLIPCLAP-style repos.
    if (try catalog.readOptional("config.json")) |config_bytes| {
        defer allocator.free(config_bytes);
        try ignoreNonResourceMetadataError(parseConfigJson(&manifest, allocator, config_bytes));
    }
    if (manifest.native_arch_hint == .none and manifest.max_position_embeddings == 512 and manifest.hidden_size == 768) {
        if (try catalog.readOptional("clip_config.json")) |config_bytes| {
            defer allocator.free(config_bytes);
            try ignoreNonResourceMetadataError(parseConfigJson(&manifest, allocator, config_bytes));
        }
    }

    // Try to parse model_manifest.json
    if (try catalog.readOptional("model_manifest.json")) |manifest_bytes| {
        defer allocator.free(manifest_bytes);
        try ignoreNonResourceMetadataError(parseModelManifestJson(&manifest, allocator, manifest_bytes));
    }

    if (try catalog.readOptional("antfly_inference_bundle.json")) |bundle_bytes| {
        defer allocator.free(bundle_bytes);
        try ignoreNonResourceMetadataError(parseInferenceBundleJsonWithCatalog(&manifest, allocator, catalog, bundle_bytes));
    }
    if (shouldParseClipclapGgufVariant(catalog)) {
        try parseOptionalInferenceVariantsFile(&manifest, allocator, catalog);
    }

    // Try to parse gliner_config.json (for GLiNER NER models)
    if (try catalog.readOptional("gliner_config.json")) |gliner_bytes| {
        defer allocator.free(gliner_bytes);
        try ignoreNonResourceMetadataError(parseGlinerConfig(&manifest, allocator, gliner_bytes));
    }

    // Try to parse added_tokens.json (for GLiNER special token IDs)
    if (try catalog.readOptional("added_tokens.json")) |at_bytes| {
        defer allocator.free(at_bytes);
        try ignoreNonResourceMetadataError(parseAddedTokens(&manifest, at_bytes));
    }

    // Auto-detect ONNX files unless a mixed single-repo ClipClap checkout was
    // explicitly resolved to its GGUF pair. The GGUF pair embeds projection
    // weights, so falling through to the default ONNX files would mix variants.
    if (!manifest.isClipclapGgufBundle()) {
        if (manifest.onnx_path == null) manifest.onnx_path = try findFileInSubdirs(allocator, catalog, &onnx_candidates, &onnx_subdirs);
        if (manifest.visual_model_path == null) manifest.visual_model_path = try findFileInSubdirs(allocator, catalog, &visual_model_candidates, &onnx_subdirs);
        if (manifest.audio_model_path == null) manifest.audio_model_path = try findFileInSubdirs(allocator, catalog, &audio_model_candidates, &onnx_subdirs);
        if (manifest.text_projection_path == null) manifest.text_projection_path = try findFileInSubdirs(allocator, catalog, &text_projection_candidates, &onnx_subdirs);
        if (manifest.visual_projection_path == null) manifest.visual_projection_path = try findFileInSubdirs(allocator, catalog, &visual_projection_candidates, &onnx_subdirs);
        if (manifest.audio_projection_path == null) manifest.audio_projection_path = try findFileInSubdirs(allocator, catalog, &audio_projection_candidates, &onnx_subdirs);
    }

    // Auto-detect SafeTensors file
    if (manifest.safetensors_path == null) manifest.safetensors_path = try findFileInSubdirs(allocator, catalog, &safetensors_candidates, &.{""});
    if (manifest.safetensors_index_path == null) manifest.safetensors_index_path = try findFileInSubdirs(allocator, catalog, &safetensors_index_candidates, &.{""});
    if (manifest.gliner_head_gguf_path == null) manifest.gliner_head_gguf_path = try findFileInSubdirs(allocator, catalog, &.{"gliner_head.gguf"}, &.{""});
    if (manifest.gliner_head_safetensors_path == null) manifest.gliner_head_safetensors_path = try findFileInSubdirs(allocator, catalog, &.{"gliner_head.safetensors"}, &.{""});
    if (manifest.config_path == null) manifest.config_path = try findFileInSubdirs(allocator, catalog, &.{"config.json"}, &.{""});
    if (manifest.model_manifest_path == null) manifest.model_manifest_path = try findFileInSubdirs(allocator, catalog, &.{"model_manifest.json"}, &.{""});
    if (manifest.tokenizer_json_path == null) manifest.tokenizer_json_path = try findFileInSubdirs(allocator, catalog, &.{"tokenizer.json"}, &.{""});
    if (manifest.tokenizer_config_path == null) manifest.tokenizer_config_path = try findFileInSubdirs(allocator, catalog, &.{"tokenizer_config.json"}, &.{""});
    if (manifest.special_tokens_map_path == null) manifest.special_tokens_map_path = try findFileInSubdirs(allocator, catalog, &.{"special_tokens_map.json"}, &.{""});
    if (manifest.preprocessor_config_path == null) manifest.preprocessor_config_path = try findFileInSubdirs(allocator, catalog, &.{"preprocessor_config.json"}, &.{""});
    if (manifest.processor_config_path == null) manifest.processor_config_path = try findFileInSubdirs(allocator, catalog, &.{"processor_config.json"}, &.{""});

    // Auto-detect GGUF files. External multimodal projectors are GGUFs too,
    // but they are not decoder weights and must not be opened as the main model.
    try fillAutoDetectedGgufPaths(&manifest, allocator, catalog);

    // Auto-detect tokenizer
    if (catalog.exists("tokenizer.json") or
        catalog.exists("vocab.txt") or
        catalog.exists("vocab.json"))
    {
        manifest.tokenizer_type = .huggingface;
    } else if (catalog.exists("tokenizer.model")) {
        manifest.tokenizer_type = .sentencepiece;
    }

    // Load chat template (from chat_template.jinja file)
    if (try catalog.readOptional("chat_template.jinja")) |ct| {
        if (std.mem.trim(u8, ct, &.{ ' ', '\t', '\n', '\r' }).len > 0) {
            manifest.chat_template = ct;
        } else {
            allocator.free(ct);
        }
    }

    // Load special tokens from tokenizer_config.json
    if (try catalog.readOptional("tokenizer.json")) |tok_bytes| {
        defer allocator.free(tok_bytes);
        try ignoreNonResourceMetadataError(parseTokenizerJsonSpecialTokens(&manifest, allocator, tok_bytes));
    }
    if (try catalog.readOptional("tokenizer_config.json")) |tc_bytes| {
        defer allocator.free(tc_bytes);
        try ignoreNonResourceMetadataError(parseTokenizerConfig(&manifest, allocator, tc_bytes));
    }

    if (manifest.gguf_path) |gguf_path| {
        try ignoreNonResourceMetadataError(applyGgufTokenizerMetadata(&manifest, allocator, catalog, model_dir_path, gguf_path));
    }

    applyImplicitSparseOutputLayout(&manifest, catalog);
    try applyImplicitModelTypeHints(&manifest, model_dir_path);

    return manifest;
}

/// Load only the metadata needed to list a model in server discovery results.
///
/// This intentionally avoids tokenizer parsing and GGUF metadata inspection. It
/// still records enough artifact paths to hide obviously unloadable bundles and
/// to expose text/image/audio listing metadata.
pub fn loadListingFromDir(allocator: std.mem.Allocator, model_dir_path: []const u8) !ModelManifest {
    var manifest = ModelManifest{ .allocator = allocator };
    errdefer manifest.deinit();

    if (std.mem.endsWith(u8, model_dir_path, ".gguf")) {
        var direct = try DirectGgufArtifact.init(allocator, model_dir_path);
        defer direct.deinit();
        manifest.gguf_path = direct.takePath();
        return manifest;
    }

    var catalog = try ArtifactCatalog.initPublished(allocator, model_dir_path);
    defer catalog.deinit();

    if (inferModelTypeFromPath(model_dir_path)) |model_type| {
        manifest.model_type = model_type;
        manifest.model_type_origin = .path;
    }

    if (try catalog.readOptional("config.json")) |config_bytes| {
        defer allocator.free(config_bytes);
        try ignoreNonResourceMetadataError(parseListingConfigJson(&manifest, allocator, config_bytes));
    }
    if (manifest.native_arch_hint == .none and manifest.config_model_arch.len == 0) {
        if (try catalog.readOptional("clip_config.json")) |config_bytes| {
            defer allocator.free(config_bytes);
            try ignoreNonResourceMetadataError(parseListingConfigJson(&manifest, allocator, config_bytes));
        }
    }

    if (try catalog.readOptional("model_manifest.json")) |manifest_bytes| {
        defer allocator.free(manifest_bytes);
        try ignoreNonResourceMetadataError(parseModelManifestJson(&manifest, allocator, manifest_bytes));
    }

    if (try catalog.readOptional("antfly_inference_bundle.json")) |bundle_bytes| {
        defer allocator.free(bundle_bytes);
        try ignoreNonResourceMetadataError(parseInferenceBundleJsonWithCatalog(&manifest, allocator, &catalog, bundle_bytes));
    }
    try parseOptionalInferenceVariantsFile(&manifest, allocator, &catalog);

    if (try catalog.readOptional("gliner_config.json")) |gliner_bytes| {
        defer allocator.free(gliner_bytes);
        try ignoreNonResourceMetadataError(parseGlinerConfig(&manifest, allocator, gliner_bytes));
    }
    if (try catalog.readOptional("added_tokens.json")) |at_bytes| {
        defer allocator.free(at_bytes);
        try ignoreNonResourceMetadataError(parseAddedTokens(&manifest, at_bytes));
    }
    try applyListingGlinerHint(&manifest, allocator, &catalog);

    if (!manifest.isClipclapGgufBundle()) {
        if (manifest.onnx_path == null) manifest.onnx_path = try findFileInSubdirs(allocator, &catalog, &onnx_candidates, &onnx_subdirs);
        if (manifest.visual_model_path == null) manifest.visual_model_path = try findFileInSubdirs(allocator, &catalog, &visual_model_candidates, &onnx_subdirs);
        if (manifest.audio_model_path == null) manifest.audio_model_path = try findFileInSubdirs(allocator, &catalog, &audio_model_candidates, &onnx_subdirs);
        if (manifest.text_projection_path == null) manifest.text_projection_path = try findFileInSubdirs(allocator, &catalog, &text_projection_candidates, &onnx_subdirs);
        if (manifest.visual_projection_path == null) manifest.visual_projection_path = try findFileInSubdirs(allocator, &catalog, &visual_projection_candidates, &onnx_subdirs);
        if (manifest.audio_projection_path == null) manifest.audio_projection_path = try findFileInSubdirs(allocator, &catalog, &audio_projection_candidates, &onnx_subdirs);
    }

    if (manifest.safetensors_path == null) manifest.safetensors_path = try findFileInSubdirs(allocator, &catalog, &safetensors_candidates, &.{""});
    if (manifest.safetensors_index_path == null) manifest.safetensors_index_path = try findFileInSubdirs(allocator, &catalog, &safetensors_index_candidates, &.{""});
    if (manifest.gliner_head_gguf_path == null) manifest.gliner_head_gguf_path = try findFileInSubdirs(allocator, &catalog, &.{"gliner_head.gguf"}, &.{""});
    if (manifest.gliner_head_safetensors_path == null) manifest.gliner_head_safetensors_path = try findFileInSubdirs(allocator, &catalog, &.{"gliner_head.safetensors"}, &.{""});
    if (manifest.config_path == null) manifest.config_path = try findFileInSubdirs(allocator, &catalog, &.{"config.json"}, &.{""});
    if (manifest.model_manifest_path == null) manifest.model_manifest_path = try findFileInSubdirs(allocator, &catalog, &.{"model_manifest.json"}, &.{""});
    if (manifest.tokenizer_json_path == null) manifest.tokenizer_json_path = try findFileInSubdirs(allocator, &catalog, &.{"tokenizer.json"}, &.{""});
    if (manifest.tokenizer_config_path == null) manifest.tokenizer_config_path = try findFileInSubdirs(allocator, &catalog, &.{"tokenizer_config.json"}, &.{""});
    if (manifest.preprocessor_config_path == null) manifest.preprocessor_config_path = try findFileInSubdirs(allocator, &catalog, &.{"preprocessor_config.json"}, &.{""});
    if (manifest.processor_config_path == null) manifest.processor_config_path = try findFileInSubdirs(allocator, &catalog, &.{"processor_config.json"}, &.{""});
    try fillAutoDetectedGgufPaths(&manifest, allocator, &catalog);

    applyImplicitSparseOutputLayout(&manifest, &catalog);
    try applyImplicitModelTypeHints(&manifest, model_dir_path);

    return manifest;
}

fn applyListingGlinerHint(manifest: *ModelManifest, allocator: std.mem.Allocator, catalog: *const ArtifactCatalog) !void {
    if (manifest.gliner_model_type.len > 0) return;
    if (!std.mem.eql(u8, manifest.config_model_arch, "extractor") and !hasGlinerPathHint(catalog.model_dir_path)) return;

    if (try catalog.readOptional("special_tokens_map.json")) |tokens_bytes| {
        defer allocator.free(tokens_bytes);
        if (!listingSpecialTokensMapHasGlinerMarkers(tokens_bytes)) return;
    } else {
        return;
    }

    manifest.gliner_model_type = try allocator.dupe(u8, "gliner2");
}

fn listingSpecialTokensMapHasGlinerMarkers(json_bytes: []const u8) bool {
    return std.mem.indexOf(u8, json_bytes, "\"[P]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[C]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[E]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[R]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[SEP_TEXT]\"") != null;
}

fn applyImplicitSparseOutputLayout(manifest: *ModelManifest, catalog: *const ArtifactCatalog) void {
    if (manifest.sparse_3d_output_layout != null) return;
    if (catalog.exists("1_SpladePooling/config.json")) {
        manifest.sparse_3d_output_layout = .batch_seq;
    }
}

fn applyImplicitModelTypeHints(manifest: *ModelManifest, model_dir_path: []const u8) !void {
    if (inferGlinerModelType(manifest, model_dir_path)) |gliner_type| {
        if (manifest.gliner_model_type.len > 0 and !std.mem.eql(u8, manifest.gliner_model_type, gliner_type)) {
            manifest.allocator.free(manifest.gliner_model_type);
            manifest.gliner_model_type = "";
        }
        if (manifest.gliner_model_type.len == 0) {
            manifest.gliner_model_type = try manifest.allocator.dupe(u8, gliner_type);
        }
    }

    if (hasRerankPathHint(model_dir_path) and (manifest.model_type == .embedder or manifest.model_type == .classifier)) {
        manifest.model_type = .reranker;
        manifest.model_type_origin = .path;
        return;
    }

    if (inferModelTypeFromTasks(manifest.tasks)) |task_model_type| {
        manifest.model_type = task_model_type;
        manifest.model_type_origin = .tasks;
        return;
    }

    if (manifest.gliner_model_type.len > 0) {
        manifest.model_type = .recognizer;
        if (manifest.inference_bundle_family.len > 0) {
            manifest.model_type_origin = .bundle;
        } else if (manifest.model_type_origin != .config) {
            manifest.model_type_origin = .heuristic;
        }
        return;
    }

    if (manifest.native_arch_hint == .whisper) {
        manifest.model_type = .transcriber;
        manifest.model_type_origin = .config;
        return;
    }
    if (manifest.model_type != .embedder) return;
    if (manifest.native_arch_hint == .florence or
        std.mem.eql(u8, manifest.config_model_arch, "vision-encoder-decoder"))
    {
        manifest.model_type = .reader;
        manifest.model_type_origin = .config;
        return;
    }
    if (manifest.config_model_arch.len > 0 and gpt.isGenerativeModel(manifest.config_model_arch)) {
        manifest.model_type = .generator;
        manifest.model_type_origin = .config;
    }
}

fn inferModelTypeFromTasks(tasks: []const []const u8) ?ModelType {
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "extract")) return .recognizer;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "rerank")) return .reranker;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "classify")) return .classifier;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "read")) return .reader;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "transcribe")) return .transcriber;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "rewrite")) return .rewriter;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "chunk")) return .chunker;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "generate")) return .generator;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "embed")) return .embedder;
    }
    return null;
}

fn inferGlinerModelType(manifest: *const ModelManifest, model_dir_path: []const u8) ?[]const u8 {
    if (manifest.gliner_model_type.len > 0) return manifest.gliner_model_type;

    const has_gliner_special_tokens = manifest.gliner_token_p != 0 and
        manifest.gliner_token_c != 0 and
        manifest.gliner_token_e != 0 and
        manifest.gliner_token_r != 0 and
        manifest.gliner_token_sep_text != 0;

    if (std.mem.eql(u8, manifest.config_model_arch, "extractor") and has_gliner_special_tokens) {
        return "gliner2";
    }
    if (hasGlinerPathHint(model_dir_path) and has_gliner_special_tokens) {
        return "gliner2";
    }
    return null;
}

fn hasGlinerPathHint(model_dir_path: []const u8) bool {
    var it = std.mem.tokenizeAny(u8, model_dir_path, "/\\");
    while (it.next()) |component| {
        if (containsAsciiIgnoreCase(component, "gliner")) return true;
    }
    return false;
}

fn hasRerankPathHint(model_dir_path: []const u8) bool {
    var it = std.mem.tokenizeAny(u8, model_dir_path, "/\\");
    while (it.next()) |component| {
        if (containsAsciiIgnoreCase(component, "rerank")) return true;
    }
    return false;
}

fn containsAsciiIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (haystack.len < needle.len) return false;

    var start: usize = 0;
    while (start + needle.len <= haystack.len) : (start += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[start .. start + needle.len], needle)) return true;
    }
    return false;
}

fn inferModelTypeFromPath(model_dir_path: []const u8) ?ModelType {
    var it = std.mem.tokenizeAny(u8, model_dir_path, "/\\");
    while (it.next()) |component| {
        if (std.mem.eql(u8, component, "embedders")) return .embedder;
        if (std.mem.eql(u8, component, "rerankers")) return .reranker;
        if (std.mem.eql(u8, component, "chunkers")) return .chunker;
        if (std.mem.eql(u8, component, "generators")) return .generator;
        if (std.mem.eql(u8, component, "extractors")) return .recognizer;
        if (std.mem.eql(u8, component, "classifiers")) return .classifier;
        if (std.mem.eql(u8, component, "rewriters")) return .rewriter;
        if (std.mem.eql(u8, component, "readers")) return .reader;
        if (std.mem.eql(u8, component, "transcribers")) return .transcriber;
    }
    return null;
}

fn applyGgufTokenizerMetadata(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    gguf_path: []const u8,
) !void {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    var parsed = try gguf_format.parse(allocator, region.data);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);

    // Architecture metadata belongs to the selected weight artifact. A
    // colocated GGUF export may still provide a tokenizer fallback, but it
    // must not overwrite the config for higher-precedence safetensors.
    if (manifest.usesGgufWeights()) {
        if (bert.parseGgufMetadata(view)) |config| {
            manifest.hidden_size = config.hidden_size;
            manifest.intermediate_size = config.intermediate_size;
            manifest.max_position_embeddings = config.max_position_embeddings;
            manifest.num_hidden_layers = config.num_hidden_layers;
            manifest.num_attention_heads = config.num_attention_heads;
            manifest.bert_vocab_size = config.vocab_size;
            manifest.bert_type_vocab_size = config.type_vocab_size;
            manifest.bert_layer_norm_eps = config.layer_norm_eps;
            manifest.bert_model_type = config.model_type;
            manifest.bert_pad_token_id = config.pad_token_id;
        }
        if (view.getU64("bert.pooling_type")) |pooling_type| {
            manifest.pooling = switch (pooling_type) {
                1 => .mean,
                2 => .cls,
                3 => .last,
                else => manifest.pooling,
            };
        }
    }

    const gguf_model_name = view.getString("tokenizer.ggml.model");
    if (gguf_model_name) |model_name| {
        if (artifactExists(catalog, allocator, model_dir_path, "tokenizer.model")) {
            manifest.tokenizer_type = .sentencepiece;
        } else if (artifactExists(catalog, allocator, model_dir_path, "tokenizer.json")) {
            manifest.tokenizer_type = .huggingface;
        } else if (supportsGgufHuggingFaceFallback(model_name) and hasGgufHuggingFaceMetadata(&parsed)) {
            manifest.tokenizer_type = .huggingface;
        } else if (supportsGgufSentencePieceFallback(model_name) and hasGgufSentencePieceMetadata(&parsed)) {
            manifest.tokenizer_type = .sentencepiece;
        } else {
            manifest.tokenizer_type = null;
        }
    } else if (artifactExists(catalog, allocator, model_dir_path, "tokenizer.model")) {
        manifest.tokenizer_type = .sentencepiece;
    }

    if (view.getBool("tokenizer.ggml.add_bos_token")) |value| {
        manifest.add_bos_token = value;
    }
    if (view.getBool("tokenizer.ggml.add_eos_token")) |value| {
        manifest.add_eos_token = value;
    }
    if (view.getString("tokenizer.chat_template")) |value| {
        if (std.mem.trim(u8, value, &.{ ' ', '\t', '\n', '\r' }).len > 0) {
            const selected = if (gguf_model_name) |model_name|
                if (shouldUseBuiltInGemma4GgufChatTemplate(model_name, value)) gemma4_chat_template else value
            else
                value;
            if (manifest.chat_template) |old| allocator.free(old);
            manifest.chat_template = try allocator.dupe(u8, selected);
        }
    }

    applyGgufSpecialTokenString(allocator, &parsed, "tokenizer.ggml.bos_token_id", &manifest.bos_token);
    applyGgufSpecialTokenString(allocator, &parsed, "tokenizer.ggml.eos_token_id", &manifest.eos_token);
    applyGgufSpecialTokenString(allocator, &parsed, "tokenizer.ggml.unknown_token_id", &manifest.unk_token);
    applyGgufSpecialTokenString(allocator, &parsed, "tokenizer.ggml.padding_token_id", &manifest.pad_token);
}

fn artifactExists(
    catalog: ?*const ArtifactCatalog,
    allocator: std.mem.Allocator,
    model_dir_path: []const u8,
    relative_path: []const u8,
) bool {
    if (catalog) |value| return value.exists(relative_path);
    return c_file.fileExistsInDir(allocator, model_dir_path, relative_path);
}

fn gemma4ChatTemplateRequiresBuiltInFallback(chat_template: []const u8) bool {
    return std.mem.indexOf(u8, chat_template, "macro format_parameters") != null or
        std.mem.indexOf(u8, chat_template, "namespace(") != null or
        std.mem.indexOf(u8, chat_template, "{% set captured_content") != null or
        std.mem.indexOf(u8, chat_template, "{%- set captured_content") != null;
}

fn shouldUseBuiltInGemma4GgufChatTemplate(model_name: []const u8, chat_template: []const u8) bool {
    return std.mem.eql(u8, model_name, "gemma4") and gemma4ChatTemplateRequiresBuiltInFallback(chat_template);
}

fn supportsGgufSentencePieceFallback(model_name: []const u8) bool {
    return std.mem.eql(u8, model_name, "llama") or std.mem.startsWith(u8, model_name, "gemma");
}

fn supportsGgufHuggingFaceFallback(model_name: []const u8) bool {
    return std.mem.eql(u8, model_name, "gpt2") or
        std.mem.eql(u8, model_name, "gemma4") or
        std.mem.eql(u8, model_name, "t5");
}

fn hasGgufSentencePieceMetadata(parsed: *const gguf_format.File) bool {
    const tokens = findMetadataEntry(parsed, "tokenizer.ggml.tokens") orelse return false;
    const scores = findMetadataEntry(parsed, "tokenizer.ggml.scores") orelse return false;
    const token_types = findMetadataEntry(parsed, "tokenizer.ggml.token_type") orelse return false;

    return tokens.value == .array and
        scores.value == .array and
        token_types.value == .array and
        tokens.value.array.element_type == .string and
        (scores.value.array.element_type == .f32 or scores.value.array.element_type == .f64) and
        (token_types.value.array.element_type == .i32 or
            token_types.value.array.element_type == .i64 or
            token_types.value.array.element_type == .u32 or
            token_types.value.array.element_type == .u64);
}

fn hasGgufHuggingFaceMetadata(parsed: *const gguf_format.File) bool {
    const tokens = findMetadataEntry(parsed, "tokenizer.ggml.tokens") orelse return false;
    if (tokens.value != .array or tokens.value.array.element_type != .string) return false;
    if (findMetadataEntry(parsed, "tokenizer.ggml.merges")) |merges| {
        if (merges.value == .array and merges.value.array.element_type == .string) return true;
    }
    const scores = findMetadataEntry(parsed, "tokenizer.ggml.scores") orelse return false;
    const token_types = findMetadataEntry(parsed, "tokenizer.ggml.token_type") orelse return false;
    return scores.value == .array and
        token_types.value == .array and
        (scores.value.array.element_type == .f32 or scores.value.array.element_type == .f64) and
        (token_types.value.array.element_type == .i32 or
            token_types.value.array.element_type == .i64 or
            token_types.value.array.element_type == .u32 or
            token_types.value.array.element_type == .u64);
}

fn applyGgufSpecialTokenString(
    allocator: std.mem.Allocator,
    parsed: *const gguf_format.File,
    id_key: []const u8,
    target: *[]const u8,
) void {
    const view = gguf_metadata.View.init(parsed);
    const token_id_u64 = view.getU64(id_key) orelse return;
    const token_id: usize = @intCast(token_id_u64);

    const entry = findMetadataEntry(parsed, "tokenizer.ggml.tokens") orelse return;
    const arr = switch (entry.value) {
        .array => |value| value,
        else => return,
    };
    if (arr.element_type != .string or token_id >= arr.values.len) return;
    const token = switch (arr.values[token_id]) {
        .string => |value| value,
        else => return,
    };
    if (target.*.len > 0) allocator.free(target.*);
    target.* = allocator.dupe(u8, token) catch return;
}

fn findMetadataEntry(parsed: *const gguf_format.File, key: []const u8) ?*const gguf_format.MetadataEntry {
    for (parsed.metadata) |*entry| {
        if (std.mem.eql(u8, entry.key, key)) return entry;
    }
    return null;
}

fn findFileInSubdirs(
    allocator: std.mem.Allocator,
    catalog: *const ArtifactCatalog,
    candidates: []const []const u8,
    subdirs: []const []const u8,
) !?[]const u8 {
    for (subdirs) |subdir| {
        for (candidates) |candidate| {
            const relative_path = if (subdir.len > 0)
                try std.fmt.allocPrint(allocator, "{s}/{s}", .{ subdir, candidate })
            else
                try allocator.dupe(u8, candidate);
            defer allocator.free(relative_path);

            if (try catalog.resolve(relative_path)) |path| {
                return path;
            }
        }
    }
    return null;
}

fn findFirstExtensionInDir(allocator: std.mem.Allocator, base_dir: []const u8, extension: []const u8) !?[]const u8 {
    if (!c_file.link_libc) {
        var dir = Dir.cwd().openDir(std.Options.debug_io, base_dir, .{ .iterate = true }) catch return null;
        defer dir.close(std.Options.debug_io);
        var iter = dir.iterate();
        while (iter.next(std.Options.debug_io) catch null) |entry| {
            if (entry.name.len == 0 or entry.name[0] == '.') continue;
            if (!std.mem.endsWith(u8, entry.name, extension)) continue;
            return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base_dir, entry.name });
        }
        return null;
    }

    const base_dir_z = try allocator.dupeSentinel(u8, base_dir, 0);
    defer allocator.free(base_dir_z);

    const dir = c_file.c.opendir(base_dir_z.ptr);
    if (dir == null) return null;
    defer _ = c_file.c.closedir(dir);

    while (c_file.c.readdir(dir)) |entry| {
        const name_z: [*:0]const u8 = @ptrCast(&entry.*.d_name);
        const name = std.mem.span(name_z);
        if (name.len == 0 or name[0] == '.') continue;
        if (!std.mem.endsWith(u8, name, extension)) continue;
        return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base_dir, name });
    }
    return null;
}

fn isGgufProjectorFileName(name: []const u8) bool {
    if (!std.mem.endsWith(u8, name, ".gguf")) return false;
    const ext = ".gguf";
    const stem = name[0 .. name.len - ext.len];
    return std.mem.eql(u8, stem, "mmproj") or
        std.mem.startsWith(u8, stem, "mmproj-") or
        std.mem.startsWith(u8, stem, "mmproj_") or
        std.mem.endsWith(u8, stem, "-mmproj") or
        std.mem.endsWith(u8, stem, "_mmproj");
}

fn isGlinerHeadGgufFileName(name: []const u8) bool {
    return std.mem.eql(u8, name, "gliner_head.gguf") or
        std.mem.eql(u8, name, "gliner2-head.gguf") or
        (std.mem.startsWith(u8, name, "gliner2-head.") and std.mem.endsWith(u8, name, ".gguf"));
}

const projector_quant_preference = [_][]const u8{
    "Q8_0", "Q6_K", "Q5_K_M", "Q4_K_M", "F16", "BF16",
};

fn fileNameHasDelimitedTokenIgnoreCase(name: []const u8, token: []const u8) bool {
    if (token.len == 0 or name.len < token.len) return false;
    var start: usize = 0;
    while (start + token.len <= name.len) : (start += 1) {
        if (!std.ascii.eqlIgnoreCase(name[start .. start + token.len], token)) continue;
        const left_boundary = start == 0 or switch (name[start - 1]) {
            '-', '_', '.' => true,
            else => false,
        };
        const end = start + token.len;
        const right_boundary = end == name.len or switch (name[end]) {
            '-', '_', '.' => true,
            else => false,
        };
        if (left_boundary and right_boundary) return true;
    }
    return false;
}

fn projectorPreferenceRank(name: []const u8) u8 {
    for (projector_quant_preference, 0..) |quant, rank| {
        if (fileNameHasDelimitedTokenIgnoreCase(name, quant)) return @intCast(rank);
    }
    return std.math.maxInt(u8);
}

const DiscoveredGgufPaths = struct {
    decoder: ?[]u8 = null,
    projector: ?[]u8 = null,

    fn deinit(self: *DiscoveredGgufPaths, allocator: std.mem.Allocator) void {
        if (self.decoder) |path| allocator.free(path);
        if (self.projector) |path| allocator.free(path);
        self.* = undefined;
    }
};

const GgufSelection = struct {
    paths: DiscoveredGgufPaths = .{},
    decoder_key: ?[]u8 = null,
    projector_key: ?[]u8 = null,
    decoder_depth: usize = std.math.maxInt(usize),
    projector_rank: u8 = std.math.maxInt(u8),
    projector_depth: usize = std.math.maxInt(usize),

    fn deinit(self: *GgufSelection, allocator: std.mem.Allocator) void {
        if (self.decoder_key) |key| allocator.free(key);
        if (self.projector_key) |key| allocator.free(key);
        self.decoder_key = null;
        self.projector_key = null;
    }

    fn candidatePath(
        allocator: std.mem.Allocator,
        base_dir: ?[]const u8,
        path: []const u8,
    ) ![]u8 {
        if (base_dir) |root| return std.fs.path.join(allocator, &.{ root, path });
        return allocator.dupe(u8, path);
    }

    fn consider(
        self: *GgufSelection,
        allocator: std.mem.Allocator,
        name: []const u8,
        sort_key: []const u8,
        depth: usize,
        base_dir: ?[]const u8,
        resolved_path: []const u8,
    ) !void {
        if (!std.mem.endsWith(u8, name, ".gguf") or isGlinerHeadGgufFileName(name)) return;

        if (isGgufProjectorFileName(name)) {
            const rank = projectorPreferenceRank(name);
            const replace = self.paths.projector == null or rank < self.projector_rank or
                (rank == self.projector_rank and depth < self.projector_depth) or
                (rank == self.projector_rank and depth == self.projector_depth and
                    std.mem.lessThan(u8, sort_key, self.projector_key.?));
            if (!replace) return;

            const owned_path = try candidatePath(allocator, base_dir, resolved_path);
            errdefer allocator.free(owned_path);
            const owned_key = try allocator.dupe(u8, sort_key);
            if (self.paths.projector) |old_path| allocator.free(old_path);
            if (self.projector_key) |old_key| allocator.free(old_key);
            self.paths.projector = owned_path;
            self.projector_key = owned_key;
            self.projector_rank = rank;
            self.projector_depth = depth;
            return;
        }

        const replace = self.paths.decoder == null or depth < self.decoder_depth or
            (depth == self.decoder_depth and std.mem.lessThan(u8, sort_key, self.decoder_key.?));
        if (!replace) return;

        const owned_path = try candidatePath(allocator, base_dir, resolved_path);
        errdefer allocator.free(owned_path);
        const owned_key = try allocator.dupe(u8, sort_key);
        if (self.paths.decoder) |old_path| allocator.free(old_path);
        if (self.decoder_key) |old_key| allocator.free(old_key);
        self.paths.decoder = owned_path;
        self.decoder_key = owned_key;
        self.decoder_depth = depth;
    }
};

fn posixBasename(path: []const u8) []const u8 {
    const start = if (std.mem.lastIndexOfScalar(u8, path, '/')) |index| index + 1 else 0;
    return path[start..];
}

fn resolvedWalkerEntryKind(io: std.Io, entry: Dir.Walker.Entry) !?std.Io.File.Kind {
    if (entry.kind != .unknown) return entry.kind;
    const stat = entry.dir.statFile(io, entry.basename, .{ .follow_symlinks = false }) catch |err| switch (err) {
        // Directory iteration is only a snapshot. A concurrent cleanup may
        // remove an entry before filesystems that report DT_UNKNOWN can stat it.
        error.FileNotFound => return null,
        else => return err,
    };
    return stat.kind;
}

fn discoverGgufPathsWithCatalog(allocator: std.mem.Allocator, catalog: *const ArtifactCatalog) !DiscoveredGgufPaths {
    const io = std.Options.debug_io;
    const base_dir = catalog.model_dir_path;
    var selection: GgufSelection = .{};
    defer selection.deinit(allocator);
    errdefer selection.paths.deinit(allocator);

    if (catalog.receipt) |*validated| {
        for (validated.artifacts) |artifact| {
            try selection.consider(
                allocator,
                posixBasename(artifact.path),
                artifact.path,
                std.mem.countScalar(u8, artifact.path, '/') + 1,
                null,
                artifact.canonical_path,
            );
        }
        return selection.paths;
    }

    var dir = Dir.cwd().openDir(io, base_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return .{},
        else => return err,
    };
    defer dir.close(io);

    var walker = try dir.walkSelectively(allocator);
    defer walker.deinit();

    while (try walker.next(io)) |entry| {
        const name = entry.basename;
        if (name.len == 0 or name[0] == '.') continue;
        const kind = (try resolvedWalkerEntryKind(io, entry)) orelse continue;
        if (kind == .directory) {
            var directory_entry = entry;
            directory_entry.kind = .directory;
            try walker.enter(io, directory_entry);
            continue;
        }
        if (kind != .file and kind != .sym_link) continue;
        try selection.consider(
            allocator,
            name,
            entry.path,
            entry.depth(),
            base_dir,
            entry.path,
        );
    }
    return selection.paths;
}

fn discoverGgufPaths(allocator: std.mem.Allocator, base_dir: []const u8) !DiscoveredGgufPaths {
    var catalog = try ArtifactCatalog.initPublished(allocator, base_dir);
    defer catalog.deinit();
    return discoverGgufPathsWithCatalog(allocator, &catalog);
}

fn fillAutoDetectedGgufPaths(manifest: *ModelManifest, allocator: std.mem.Allocator, catalog: *const ArtifactCatalog) !void {
    if (manifest.gguf_path != null and manifest.gguf_projector_path != null) return;
    var discovered = try discoverGgufPathsWithCatalog(allocator, catalog);
    defer discovered.deinit(allocator);
    if (manifest.gguf_path == null) {
        manifest.gguf_path = discovered.decoder;
        discovered.decoder = null;
    }
    if (manifest.gguf_projector_path == null) {
        manifest.gguf_projector_path = discovered.projector;
        discovered.projector = null;
    }
}

fn findFirstGgufInDir(allocator: std.mem.Allocator, base_dir: []const u8, want_projector: bool) !?[]const u8 {
    var discovered = try discoverGgufPaths(allocator, base_dir);
    defer discovered.deinit(allocator);
    const result = if (want_projector) discovered.projector else discovered.decoder;
    if (want_projector) {
        discovered.projector = null;
    } else {
        discovered.decoder = null;
    }
    return result;
}

fn parseConfigJson(manifest: *ModelManifest, allocator: std.mem.Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    const jina_v5_embedding_config = isJinaV5TextEmbeddingConfig(&obj);

    if (obj.get("hidden_size")) |v| {
        if (jsonU32(v)) |val| manifest.hidden_size = val;
    }
    if (obj.get("intermediate_size")) |v| {
        if (jsonU32(v)) |val| manifest.intermediate_size = val;
    }
    if (obj.get("max_position_embeddings")) |v| {
        if (jsonU32(v)) |val| manifest.max_position_embeddings = val;
    }
    if (obj.get("pad_token_id")) |v| {
        if (v == .integer) manifest.bert_pad_token_id = v.integer;
    }
    if (obj.get("num_hidden_layers")) |v| {
        if (jsonU32(v)) |val| manifest.num_hidden_layers = val;
    }
    if (obj.get("num_attention_heads")) |v| {
        if (jsonU32(v)) |val| manifest.num_attention_heads = val;
    }
    if (obj.get("vocab_size")) |v| {
        if (jsonU32(v)) |val| manifest.bert_vocab_size = val;
    }
    if (obj.get("type_vocab_size")) |v| {
        if (jsonU32(v)) |val| manifest.bert_type_vocab_size = val;
    }
    if (obj.get("layer_norm_eps")) |v| {
        manifest.bert_layer_norm_eps = switch (v) {
            .float => |value| @floatCast(value),
            .integer => |value| @floatFromInt(value),
            else => manifest.bert_layer_norm_eps,
        };
    }

    if (obj.get("num_labels")) |v| {
        if (jsonU32(v)) |val| manifest.num_labels = val;
    }
    if (obj.get("max_width")) |v| {
        if (jsonU32(v)) |val| manifest.gliner_max_width = val;
    }

    // Parse id2label: {"0": "O", "1": "B-PER", ...}
    if (obj.get("id2label")) |v| {
        if (v == .object) {
            const map = v.object;
            if (map.count() > 0) {
                const count = map.count();
                const labels = try allocator.alloc([]const u8, count);
                // Initialize all to empty string literal (not heap-allocated)
                for (labels) |*l| l.* = "";

                var ok = true;
                errdefer {
                    for (labels) |l| {
                        if (l.len > 0) allocator.free(l);
                    }
                    allocator.free(labels);
                }

                var it = map.iterator();
                while (it.next()) |entry| {
                    const key = entry.key_ptr.*;
                    const idx = std.fmt.parseInt(usize, key, 10) catch continue;
                    if (idx < count) {
                        if (entry.value_ptr.* == .string) {
                            labels[idx] = allocator.dupe(u8, entry.value_ptr.string) catch {
                                ok = false;
                                break;
                            };
                        }
                    }
                }
                if (!ok) return error.OutOfMemory;
                manifest.id2label = labels;
                if (manifest.num_labels == 0) manifest.num_labels = @intCast(count);
            }
        }
    }

    if (obj.get("architectures")) |v| {
        if (v == .array) {
            for (v.array.items) |item| {
                if (item != .string) continue;
                if (inferModelTypeFromArchitectureName(item.string)) |inferred| {
                    manifest.model_type = inferred;
                    manifest.model_type_origin = .config;
                    break;
                }
            }
        }
    }

    if (obj.get("model_type")) |v| {
        if (v == .string) {
            const s = v.string;
            const config_model_arch = try allocator.dupe(u8, s);
            if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
            manifest.config_model_arch = config_model_arch;
            // Even when `model_type` describes an encoder family and leaves the
            // enum at its embedder value, it is explicit role evidence rather
            // than the neutral ModelManifest default.
            manifest.model_type_origin = .config;
            if (std.mem.eql(u8, s, "roberta") or std.mem.eql(u8, s, "xlm-roberta")) {
                manifest.bert_model_type = .roberta;
            } else if (std.mem.eql(u8, s, "distilbert")) {
                manifest.bert_model_type = .distilbert;
            } else if (std.mem.eql(u8, s, "whisper")) {
                manifest.native_arch_hint = .whisper;
            } else if (std.mem.eql(u8, s, "florence2") or
                std.mem.eql(u8, s, "florence-2") or
                std.mem.startsWith(u8, s, "florence"))
            {
                manifest.native_arch_hint = .florence;
            } else if (std.mem.eql(u8, s, "clip") or
                std.mem.eql(u8, s, "clip_text_model") or
                std.mem.eql(u8, s, "clip_vision_model") or
                std.mem.eql(u8, s, "siglip") or
                std.mem.eql(u8, s, "siglip_text_model"))
            {
                manifest.native_arch_hint = .clip;
            } else if (std.mem.eql(u8, s, "clap")) {
                manifest.native_arch_hint = .clap;
            } else if (std.mem.eql(u8, s, "layoutlmv3")) {
                manifest.native_arch_hint = .layoutlmv3;
                if (manifest.model_type == .embedder) manifest.model_type = .classifier;
            } else if (std.mem.eql(u8, s, "jina_embeddings_v5")) {
                manifest.model_type = .embedder;
            }
        }
    }

    if (jina_v5_embedding_config) {
        manifest.model_type = .embedder;
        manifest.model_type_origin = .config;
        manifest.pooling = .last;
        manifest.normalize = true;
        if (manifest.embedding_text_prefix.len > 0) allocator.free(manifest.embedding_text_prefix);
        manifest.embedding_text_prefix = try allocator.dupe(u8, "Document: ");
    }

    // For CLIP/CLAP/multimodal models, text_config contains the text encoder's
    // max_position_embeddings which may differ from the top-level value.
    // If text_config exists, prefer its max_position_embeddings for text encoding.
    if (obj.get("text_config")) |tc| {
        if (tc == .object) {
            if (tc.object.get("max_position_embeddings")) |v| {
                if (jsonU32(v)) |val| manifest.max_position_embeddings = val;
            }
        }
    }
}

fn parseListingConfigJson(manifest: *ModelManifest, allocator: std.mem.Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;

    if (obj.get("architectures")) |v| {
        if (v == .array) {
            for (v.array.items) |item| {
                if (item != .string) continue;
                if (inferModelTypeFromArchitectureName(item.string)) |inferred| {
                    manifest.model_type = inferred;
                    manifest.model_type_origin = .config;
                    break;
                }
            }
        }
    }

    if (obj.get("model_type")) |v| {
        if (v == .string) {
            const s = v.string;
            const config_model_arch = try allocator.dupe(u8, s);
            if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
            manifest.config_model_arch = config_model_arch;
            manifest.model_type_origin = .config;
            if (std.mem.eql(u8, s, "whisper")) {
                manifest.native_arch_hint = .whisper;
            } else if (std.mem.eql(u8, s, "florence2") or
                std.mem.eql(u8, s, "florence-2") or
                std.mem.startsWith(u8, s, "florence"))
            {
                manifest.native_arch_hint = .florence;
            } else if (std.mem.eql(u8, s, "clip") or
                std.mem.eql(u8, s, "clip_text_model") or
                std.mem.eql(u8, s, "clip_vision_model") or
                std.mem.eql(u8, s, "siglip") or
                std.mem.eql(u8, s, "siglip_text_model"))
            {
                manifest.native_arch_hint = .clip;
            } else if (std.mem.eql(u8, s, "clap")) {
                manifest.native_arch_hint = .clap;
            } else if (std.mem.eql(u8, s, "layoutlmv3")) {
                manifest.native_arch_hint = .layoutlmv3;
                if (manifest.model_type == .embedder) manifest.model_type = .classifier;
            } else if (std.mem.eql(u8, s, "jina_embeddings_v5")) {
                manifest.model_type = .embedder;
            }
        }
    }

    if (isJinaV5TextEmbeddingConfig(&obj)) {
        manifest.model_type = .embedder;
        manifest.model_type_origin = .config;
    }
}

fn jsonStringArrayContains(value: std.json.Value, needle: []const u8) bool {
    if (value != .array) return false;
    for (value.array.items) |item| {
        if (item == .string and std.mem.eql(u8, item.string, needle)) return true;
    }
    return false;
}

fn isJinaV5TextEmbeddingConfig(obj: *const std.json.ObjectMap) bool {
    if (obj.get("model_type")) |v| {
        if (v == .string and std.mem.eql(u8, v.string, "jina_embeddings_v5")) return true;
    }

    const task_names = obj.get("task_names") orelse return false;
    if (!jsonStringArrayContains(task_names, "retrieval") or
        !jsonStringArrayContains(task_names, "text-matching") or
        !jsonStringArrayContains(task_names, "clustering"))
    {
        return false;
    }

    const arch = obj.get("architectures") orelse return false;
    return jsonStringArrayContains(arch, "Qwen3Model") or
        jsonStringArrayContains(arch, "JinaEmbeddingsV5Model");
}

fn deinitOwnedStringArray(allocator: std.mem.Allocator, items: [][]const u8) void {
    if (items.len == 0) return;
    for (items) |item| allocator.free(item);
    allocator.free(items);
}

fn dupeJsonStringArray(allocator: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return &.{};

    var items = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (items.items) |item| allocator.free(item);
        items.deinit(allocator);
    }

    for (value.array.items) |item| {
        if (item != .string) continue;
        const owned = try allocator.dupe(u8, item.string);
        items.append(allocator, owned) catch |err| {
            allocator.free(owned);
            return err;
        };
    }

    if (items.items.len == 0) return &.{};
    return try items.toOwnedSlice(allocator);
}

fn replaceOwnedStringArray(
    allocator: std.mem.Allocator,
    target: *[][]const u8,
    replacement: [][]const u8,
) void {
    deinitOwnedStringArray(allocator, target.*);
    target.* = replacement;
}

fn parseModelManifestJson(manifest: *ModelManifest, allocator: std.mem.Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;

    if (obj.get("type")) |v| {
        if (v == .string) {
            const s = v.string;
            inline for (.{ "embedder", "reranker", "chunker", "generator", "recognizer", "rewriter", "classifier", "reader", "transcriber" }) |name| {
                if (std.mem.eql(u8, s, name)) {
                    manifest.model_type = @field(ModelType, name);
                    manifest.model_type_origin = .manifest;
                }
            }
        }
    }

    if (obj.get("tasks")) |v| {
        const tasks = try dupeJsonStringArray(allocator, v);
        if (tasks.len > 0) {
            replaceOwnedStringArray(allocator, &manifest.tasks, tasks);
        }
    }

    // Parse capabilities array
    if (obj.get("capabilities")) |v| {
        const capabilities = try dupeJsonStringArray(allocator, v);
        if (capabilities.len > 0) {
            replaceOwnedStringArray(allocator, &manifest.capabilities, capabilities);
        }
    }

    if (obj.get("inputs")) |v| {
        const inputs = try dupeJsonStringArray(allocator, v);
        if (inputs.len > 0) {
            replaceOwnedStringArray(allocator, &manifest.inputs, inputs);
        }
    }

    if (obj.get("sparse_3d_output_layout")) |v| {
        if (v == .string) manifest.sparse_3d_output_layout = parseSparse3DOutputLayout(v.string);
    } else if (obj.get("sparse_output_layout")) |v| {
        if (v == .string) manifest.sparse_3d_output_layout = parseSparse3DOutputLayout(v.string);
    }
}

fn parseSparse3DOutputLayout(value: []const u8) ?Sparse3DOutputLayout {
    if (std.mem.eql(u8, value, "batch_seq")) return .batch_seq;
    if (std.mem.eql(u8, value, "seq_batch")) return .seq_batch;
    if (std.mem.eql(u8, value, "batch_sequence")) return .batch_seq;
    if (std.mem.eql(u8, value, "sequence_batch")) return .seq_batch;
    return null;
}

fn parseGlinerConfig(manifest: *ModelManifest, allocator: std.mem.Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;

    if (obj.get("max_width")) |v| {
        if (jsonU32(v)) |val| manifest.gliner_max_width = val;
    }
    if (obj.get("max_len")) |v| {
        if (jsonU32(v)) |val| manifest.max_position_embeddings = val;
    }
    if (obj.get("threshold")) |v| {
        if (v == .float) manifest.gliner_threshold = @floatCast(v.float);
    }
    if (obj.get("flat_ner")) |v| {
        if (v == .bool) manifest.gliner_flat_ner = v.bool;
    }
    if (obj.get("model_type")) |v| {
        if (v == .string and v.string.len > 0) {
            const gliner_model_type = try allocator.dupe(u8, v.string);
            if (manifest.gliner_model_type.len > 0) allocator.free(manifest.gliner_model_type);
            manifest.gliner_model_type = gliner_model_type;
            manifest.model_type_origin = .config;
        }
    }
    if (obj.get("default_labels")) |v| {
        const labels = try dupeJsonStringArray(allocator, v);
        if (labels.len > 0) {
            replaceOwnedStringArray(allocator, &manifest.gliner_default_labels, labels);
        }
    }
    if (obj.get("relation_labels")) |v| {
        const labels = try dupeJsonStringArray(allocator, v);
        if (labels.len > 0) {
            replaceOwnedStringArray(allocator, &manifest.gliner_relation_labels, labels);
        }
    }
    if (obj.get("relation_threshold")) |v| {
        if (v == .float) manifest.gliner_relation_threshold = @floatCast(v.float);
    }
    if (manifest.gliner_relation_labels.len == 0) {
        if (obj.get("tasks")) |tasks_v| {
            if (tasks_v == .object) {
                if (tasks_v.object.get("relations")) |relations_v| {
                    if (relations_v == .object) {
                        if (relations_v.object.get("default_relation_labels")) |labels_v| {
                            const labels = try dupeJsonStringArray(allocator, labels_v);
                            if (labels.len > 0) {
                                replaceOwnedStringArray(allocator, &manifest.gliner_relation_labels, labels);
                            }
                        }
                        if (manifest.gliner_relation_threshold == 0) {
                            if (relations_v.object.get("threshold")) |threshold_v| {
                                if (threshold_v == .float) manifest.gliner_relation_threshold = @floatCast(threshold_v.float);
                            }
                        }
                    }
                }
            }
        }
    }
}

fn parseInferenceBundleJson(manifest: *ModelManifest, allocator: std.mem.Allocator, model_dir_path: []const u8, json_bytes: []const u8) !void {
    return parseInferenceBundleJsonInternal(manifest, allocator, null, model_dir_path, json_bytes);
}

fn parseInferenceBundleJsonWithCatalog(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: *const ArtifactCatalog,
    json_bytes: []const u8,
) !void {
    return parseInferenceBundleJsonInternal(manifest, allocator, catalog, catalog.model_dir_path, json_bytes);
}

fn parseInferenceBundleJsonInternal(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    json_bytes: []const u8,
) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    const family_value = obj.get("family") orelse return;
    if (family_value != .string or family_value.string.len == 0) return;
    const bundle_family = family_value.string;

    if (std.mem.eql(u8, bundle_family, "gliner2_split_bundle/v1")) {
        const encoder = obj.get("encoder");
        const head = obj.get("head");
        if (encoder == null or head == null or
            encoder.? != .string or encoder.?.string.len == 0 or
            head.? != .string or head.?.string.len == 0)
        {
            // Unmanaged local directories may intentionally contain a marker
            // for an incomplete bundle. Managed publications must never let a
            // receipt-backed bundle select artifacts absent from the receipt.
            if (catalog != null) return;
            const owned_family = try allocator.dupe(u8, bundle_family);
            errdefer allocator.free(owned_family);
            const wrapper = obj.get("wrapper");
            const owned_wrapper = if (wrapper != null and wrapper.? == .string and wrapper.?.string.len > 0)
                try allocator.dupe(u8, wrapper.?.string)
            else
                null;
            errdefer if (owned_wrapper) |value| allocator.free(value);
            try setManifestInputs(allocator, manifest, &.{"text"});
            replaceOwnedString(allocator, &manifest.inference_bundle_family, owned_family);
            if (owned_wrapper) |value| replaceOwnedString(allocator, &manifest.gliner_model_type, value);
            manifest.model_type = .recognizer;
            manifest.model_type_origin = .bundle;
            return;
        }
        const encoder_path = try resolveBundlePath(allocator, catalog, model_dir_path, encoder.?.string);
        errdefer allocator.free(encoder_path);
        const head_path = try resolveBundlePath(allocator, catalog, model_dir_path, head.?.string);
        errdefer allocator.free(head_path);
        const owned_family = try allocator.dupe(u8, bundle_family);
        errdefer allocator.free(owned_family);
        const wrapper = obj.get("wrapper");
        const owned_wrapper = if (wrapper != null and wrapper.? == .string and wrapper.?.string.len > 0)
            try allocator.dupe(u8, wrapper.?.string)
        else
            null;
        errdefer if (owned_wrapper) |value| allocator.free(value);
        try setManifestInputs(allocator, manifest, &.{"text"});

        replaceOwnedString(allocator, &manifest.inference_bundle_family, owned_family);
        if (owned_wrapper) |value| replaceOwnedString(allocator, &manifest.gliner_model_type, value);
        setOptionalPath(allocator, &manifest.gguf_path, encoder_path);
        if (std.mem.endsWith(u8, head.?.string, ".gguf")) {
            setOptionalPath(allocator, &manifest.gliner_head_gguf_path, head_path);
        } else {
            setOptionalPath(allocator, &manifest.gliner_head_safetensors_path, head_path);
        }
        manifest.model_type = .recognizer;
        manifest.model_type_origin = .bundle;
        return;
    }
    if (std.mem.eql(u8, bundle_family, "clipclap_gguf_bundle/v1")) {
        const clip = obj.get("clip") orelse return;
        const clap = obj.get("clap") orelse return;
        if (clip != .string or clip.string.len == 0 or clap != .string or clap.string.len == 0) return;
        const clip_path = try resolveBundlePath(allocator, catalog, model_dir_path, clip.string);
        errdefer allocator.free(clip_path);
        const clap_path = try resolveBundlePath(allocator, catalog, model_dir_path, clap.string);
        errdefer allocator.free(clap_path);
        const owned_family = try allocator.dupe(u8, bundle_family);
        errdefer allocator.free(owned_family);
        const config_model_arch = try allocator.dupe(u8, "clipclap");
        errdefer allocator.free(config_model_arch);
        try setManifestInputs(allocator, manifest, &.{ "text", "image", "audio" });

        replaceOwnedString(allocator, &manifest.inference_bundle_family, owned_family);
        setOptionalPath(allocator, &manifest.gguf_path, clip_path);
        setOptionalPath(allocator, &manifest.audio_model_path, clap_path);
        manifest.native_arch_hint = .clip;
        manifest.model_type_origin = .bundle;
        replaceOwnedString(allocator, &manifest.config_model_arch, config_model_arch);
        return;
    }
    if (std.mem.eql(u8, bundle_family, "florence2_gguf_bundle/v1")) {
        const model = obj.get("model") orelse obj.get("gguf") orelse return;
        if (model == .string and model.string.len > 0) {
            try applyFlorence2GgufBundle(
                manifest,
                allocator,
                try resolveBundlePath(allocator, catalog, model_dir_path, model.string),
            );
        }
        return;
    }

    const owned_family = try allocator.dupe(u8, bundle_family);
    replaceOwnedString(allocator, &manifest.inference_bundle_family, owned_family);
}

fn completeClipclapDefaultOnnxPresent(catalog: *const ArtifactCatalog) bool {
    const required = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (&required) |name| {
        if (!catalog.exists(name)) return false;
    }
    return true;
}

fn shouldUseClipclapGgufVariant(catalog: *const ArtifactCatalog) bool {
    return !completeClipclapDefaultOnnxPresent(catalog);
}

fn shouldParseClipclapGgufVariant(catalog: *const ArtifactCatalog) bool {
    if (shouldUseClipclapGgufVariant(catalog)) return true;
    if (build_options.enable_cuda and !build_options.enable_onnx) {
        return catalog.exists("antfly_inference_variants.json");
    }
    return false;
}

fn parseInferenceVariantsJson(manifest: *ModelManifest, allocator: std.mem.Allocator, model_dir_path: []const u8, json_bytes: []const u8) !void {
    return parseInferenceVariantsJsonInternal(manifest, allocator, null, model_dir_path, json_bytes);
}

fn parseInferenceVariantsJsonWithCatalog(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: *const ArtifactCatalog,
    json_bytes: []const u8,
) !void {
    return parseInferenceVariantsJsonInternal(manifest, allocator, catalog, catalog.model_dir_path, json_bytes);
}

fn parseInferenceVariantsJsonInternal(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    json_bytes: []const u8,
) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    const variants_family = obj.get("family") orelse return;
    if (variants_family != .string) return;
    if (std.mem.eql(u8, variants_family.string, "florence2_variants/v1")) {
        return parseFlorence2InferenceVariantsJson(manifest, allocator, catalog, model_dir_path, obj);
    }
    if (std.mem.eql(u8, variants_family.string, "gliner2_variants/v1")) {
        return parseGliner2InferenceVariantsJson(manifest, allocator, catalog, model_dir_path, obj);
    }
    if (std.mem.eql(u8, variants_family.string, "florence_variants/v1") or
        std.mem.eql(u8, variants_family.string, "florence2_variants/v1"))
    {
        return parseFlorence2InferenceVariantsJson(manifest, allocator, catalog, model_dir_path, obj);
    }
    if (!std.mem.eql(u8, variants_family.string, "clipclap_variants/v1")) return;
    const variants = obj.get("variants") orelse return;
    if (variants != .array) return;

    var selected: ?ResolvedClipclapGgufPair = null;
    errdefer if (selected) |*pair| pair.deinit(allocator);
    for (variants.array.items) |variant| {
        if (!isClipclapGgufVariant(variant)) continue;
        var pair = (try resolveExistingClipclapGgufVariant(allocator, catalog, model_dir_path, variant)) orelse continue;
        if (variant.object.get("format")) |format| {
            if (format == .string and std.mem.eql(u8, format.string, "Q4_K")) {
                if (selected) |*old| old.deinit(allocator);
                selected = pair;
                break;
            }
        }
        if (selected == null) {
            selected = pair;
        } else {
            pair.deinit(allocator);
        }
    }

    var pair = selected orelse return;
    selected = null;
    errdefer pair.deinit(allocator);

    const family = try allocator.dupe(u8, "clipclap_gguf_bundle/v1");
    errdefer allocator.free(family);
    const arch = try allocator.dupe(u8, "clipclap");
    errdefer allocator.free(arch);

    if (manifest.inference_bundle_family.len > 0) allocator.free(manifest.inference_bundle_family);
    manifest.inference_bundle_family = family;
    setOptionalPath(allocator, &manifest.gguf_path, pair.clip_path);
    pair.clip_path = "";
    setOptionalPath(allocator, &manifest.audio_model_path, pair.clap_path);
    pair.clap_path = "";
    manifest.native_arch_hint = .clip;
    manifest.model_type_origin = .bundle;
    if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
    manifest.config_model_arch = arch;
    try setManifestInputs(allocator, manifest, &.{ "text", "image", "audio" });
}

fn parseOptionalInferenceVariantsFile(manifest: *ModelManifest, allocator: std.mem.Allocator, catalog: *const ArtifactCatalog) !void {
    const variants_bytes = (try catalog.readOptional("antfly_inference_variants.json")) orelse return;
    defer allocator.free(variants_bytes);
    try ignoreNonResourceMetadataError(parseInferenceVariantsJsonWithCatalog(manifest, allocator, catalog, variants_bytes));
}

fn parseGliner2InferenceVariantsJson(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    obj: std.json.ObjectMap,
) !void {
    const variants = obj.get("variants") orelse return;
    if (variants != .array) return;

    var selected: ?ResolvedGliner2GgufPair = null;
    errdefer if (selected) |*pair| pair.deinit(allocator);
    for (variants.array.items) |variant| {
        if (!isGliner2GgufVariant(variant)) continue;
        var pair = (try resolveExistingGliner2GgufVariant(allocator, catalog, model_dir_path, variant)) orelse continue;
        if (variant.object.get("format")) |format| {
            if (format == .string and std.mem.eql(u8, format.string, "Q4_K")) {
                if (selected) |*old| old.deinit(allocator);
                selected = pair;
                break;
            }
        }
        if (selected == null) {
            selected = pair;
        } else {
            pair.deinit(allocator);
        }
    }

    var pair = selected orelse return;
    selected = null;
    errdefer pair.deinit(allocator);

    const family = try allocator.dupe(u8, "gliner2_split_bundle/v1");
    errdefer allocator.free(family);
    const wrapper = try allocator.dupe(u8, "gliner2");
    errdefer allocator.free(wrapper);

    if (manifest.inference_bundle_family.len > 0) allocator.free(manifest.inference_bundle_family);
    manifest.inference_bundle_family = family;
    if (manifest.gliner_model_type.len > 0) allocator.free(manifest.gliner_model_type);
    manifest.gliner_model_type = wrapper;
    setOptionalPath(allocator, &manifest.gguf_path, pair.encoder_path);
    pair.encoder_path = "";
    setOptionalPath(allocator, &manifest.gliner_head_gguf_path, pair.head_path);
    pair.head_path = "";
    manifest.model_type = .recognizer;
    manifest.model_type_origin = .bundle;
    try setManifestInputs(allocator, manifest, &.{"text"});
}

fn parseFlorence2InferenceVariantsJson(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    obj: std.json.ObjectMap,
) !void {
    const variants = obj.get("variants") orelse return;
    if (variants != .array) return;

    var selected: ?ResolvedFlorence2Gguf = null;
    errdefer if (selected) |*model| model.deinit(allocator);
    for (variants.array.items) |variant| {
        if (!isFlorence2GgufVariant(variant)) continue;
        var model = (try resolveExistingFlorence2GgufVariant(allocator, catalog, model_dir_path, variant)) orelse continue;
        if (variant.object.get("format")) |format| {
            if (format == .string and std.mem.eql(u8, format.string, "Q4_K")) {
                if (selected) |*old| old.deinit(allocator);
                selected = model;
                break;
            }
        }
        if (selected == null) {
            selected = model;
        } else {
            model.deinit(allocator);
        }
    }

    var model = selected orelse return;
    selected = null;
    errdefer model.deinit(allocator);

    try applyFlorence2GgufBundle(manifest, allocator, model.model_path);
    model.model_path = "";
}

fn applyFlorence2GgufBundle(
    manifest: *ModelManifest,
    allocator: std.mem.Allocator,
    gguf_path: []const u8,
) !void {
    var path = gguf_path;
    errdefer if (path.len > 0) allocator.free(path);

    var family = try allocator.dupe(u8, "florence2_gguf_bundle/v1");
    errdefer if (family.len > 0) allocator.free(family);
    var arch = try allocator.dupe(u8, "florence2");
    errdefer if (arch.len > 0) allocator.free(arch);

    if (manifest.inference_bundle_family.len > 0) allocator.free(manifest.inference_bundle_family);
    manifest.inference_bundle_family = family;
    family = "";
    setOptionalPath(allocator, &manifest.gguf_path, path);
    path = "";
    manifest.native_arch_hint = .florence;
    manifest.model_type = .reader;
    manifest.model_type_origin = .bundle;
    if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
    manifest.config_model_arch = arch;
    arch = "";
    try setManifestInputs(allocator, manifest, &.{ "text", "image" });
}

fn setManifestInputs(allocator: std.mem.Allocator, manifest: *ModelManifest, inputs: []const []const u8) !void {
    if (manifest.inputs.len > 0) {
        for (manifest.inputs) |input| allocator.free(input);
        allocator.free(manifest.inputs);
        manifest.inputs = &.{};
    }

    const owned = try allocator.alloc([]const u8, inputs.len);
    errdefer allocator.free(owned);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |input| allocator.free(input);
    }

    for (inputs, 0..) |input, i| {
        owned[i] = try allocator.dupe(u8, input);
        initialized += 1;
    }
    manifest.inputs = owned;
}

const ResolvedClipclapGgufPair = struct {
    clip_path: []const u8,
    clap_path: []const u8,

    fn deinit(self: *ResolvedClipclapGgufPair, allocator: std.mem.Allocator) void {
        if (self.clip_path.len > 0) allocator.free(self.clip_path);
        if (self.clap_path.len > 0) allocator.free(self.clap_path);
        self.* = .{ .clip_path = "", .clap_path = "" };
    }
};

const ResolvedGliner2GgufPair = struct {
    encoder_path: []const u8,
    head_path: []const u8,

    fn deinit(self: *ResolvedGliner2GgufPair, allocator: std.mem.Allocator) void {
        if (self.encoder_path.len > 0) allocator.free(self.encoder_path);
        if (self.head_path.len > 0) allocator.free(self.head_path);
        self.* = .{ .encoder_path = "", .head_path = "" };
    }
};

const ResolvedFlorence2Gguf = struct {
    model_path: []const u8,

    fn deinit(self: *ResolvedFlorence2Gguf, allocator: std.mem.Allocator) void {
        if (self.model_path.len > 0) allocator.free(self.model_path);
        self.* = .{ .model_path = "" };
    }
};

fn resolveExistingClipclapGgufVariant(
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    variant: std.json.Value,
) !?ResolvedClipclapGgufPair {
    const clip = variant.object.get("clip") orelse return null;
    const clap = variant.object.get("clap") orelse return null;
    if (clip != .string or clip.string.len == 0) return null;
    if (clap != .string or clap.string.len == 0) return null;

    const clip_path = resolveBundlePath(allocator, catalog, model_dir_path, clip.string) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    errdefer allocator.free(clip_path);
    const clap_path = resolveBundlePath(allocator, catalog, model_dir_path, clap.string) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    errdefer allocator.free(clap_path);
    return .{ .clip_path = clip_path, .clap_path = clap_path };
}

fn resolveExistingGliner2GgufVariant(
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    variant: std.json.Value,
) !?ResolvedGliner2GgufPair {
    const encoder = variant.object.get("encoder") orelse return null;
    const head = variant.object.get("head") orelse return null;
    if (encoder != .string or encoder.string.len == 0) return null;
    if (head != .string or head.string.len == 0) return null;

    const encoder_path = resolveBundlePath(allocator, catalog, model_dir_path, encoder.string) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    errdefer allocator.free(encoder_path);
    const head_path = resolveBundlePath(allocator, catalog, model_dir_path, head.string) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    errdefer allocator.free(head_path);
    return .{ .encoder_path = encoder_path, .head_path = head_path };
}

fn resolveExistingFlorence2GgufVariant(
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    variant: std.json.Value,
) !?ResolvedFlorence2Gguf {
    const model = variant.object.get("model") orelse variant.object.get("gguf") orelse return null;
    if (model != .string or model.string.len == 0) return null;

    const model_path = resolveBundlePath(allocator, catalog, model_dir_path, model.string) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    errdefer allocator.free(model_path);
    return .{ .model_path = model_path };
}

fn isClipclapGgufVariant(variant: std.json.Value) bool {
    if (variant != .object) return false;
    const target = variant.object.get("target") orelse return false;
    if (target != .string or !std.mem.eql(u8, target.string, "gguf")) return false;
    const clip = variant.object.get("clip") orelse return false;
    const clap = variant.object.get("clap") orelse return false;
    return clip == .string and clip.string.len > 0 and clap == .string and clap.string.len > 0;
}

fn isGliner2GgufVariant(variant: std.json.Value) bool {
    if (variant != .object) return false;
    const target = variant.object.get("target") orelse return false;
    if (target != .string or !std.mem.eql(u8, target.string, "gguf")) return false;
    const encoder = variant.object.get("encoder") orelse return false;
    const head = variant.object.get("head") orelse return false;
    return encoder == .string and encoder.string.len > 0 and head == .string and head.string.len > 0;
}

fn isFlorence2GgufVariant(variant: std.json.Value) bool {
    if (variant != .object) return false;
    const target = variant.object.get("target") orelse return false;
    if (target != .string or !std.mem.eql(u8, target.string, "gguf")) return false;
    const model = variant.object.get("model") orelse variant.object.get("gguf") orelse return false;
    return model == .string and model.string.len > 0;
}

fn resolveBundlePath(
    allocator: std.mem.Allocator,
    catalog: ?*const ArtifactCatalog,
    model_dir_path: []const u8,
    path: []const u8,
) ![]const u8 {
    if (catalog) |value| return (try value.resolve(path)) orelse error.FileNotFound;
    return managed_receipt.resolveContainedArtifactPath(
        allocator,
        std.Options.debug_io,
        model_dir_path,
        path,
    );
}

fn setOptionalPath(allocator: std.mem.Allocator, slot: *?[]const u8, value: []const u8) void {
    if (slot.*) |old| allocator.free(old);
    slot.* = value;
}

fn replaceOwnedString(allocator: std.mem.Allocator, slot: *[]const u8, value: []const u8) void {
    if (slot.*.len > 0) allocator.free(slot.*);
    slot.* = value;
}

fn parseAddedTokens(manifest: *ModelManifest, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, manifest.allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;

    if (obj.get("[P]")) |v| {
        if (v == .integer) manifest.gliner_token_p = @intCast(v.integer);
    }
    if (obj.get("[C]")) |v| {
        if (v == .integer) manifest.gliner_token_c = @intCast(v.integer);
    }
    if (obj.get("[E]")) |v| {
        if (v == .integer) manifest.gliner_token_e = @intCast(v.integer);
    }
    if (obj.get("[R]")) |v| {
        if (v == .integer) manifest.gliner_token_r = @intCast(v.integer);
    }
    if (obj.get("[SEP_TEXT]")) |v| {
        if (v == .integer) manifest.gliner_token_sep_text = @intCast(v.integer);
    }
}

fn setGlinerSpecialToken(manifest: *ModelManifest, content: []const u8, token_id: i32) void {
    if (std.mem.eql(u8, content, "[P]")) manifest.gliner_token_p = token_id;
    if (std.mem.eql(u8, content, "[C]")) manifest.gliner_token_c = token_id;
    if (std.mem.eql(u8, content, "[E]")) manifest.gliner_token_e = token_id;
    if (std.mem.eql(u8, content, "[R]")) manifest.gliner_token_r = token_id;
    if (std.mem.eql(u8, content, "[SEP_TEXT]")) manifest.gliner_token_sep_text = token_id;
}

fn parseTokenizerJsonSpecialTokens(manifest: *ModelManifest, allocator: std.mem.Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const obj = parsed.value.object;

    if (obj.get("added_tokens")) |tokens| {
        if (tokens == .array) {
            for (tokens.array.items) |entry| {
                if (entry != .object) continue;
                const id_val = entry.object.get("id") orelse continue;
                const content_val = entry.object.get("content") orelse continue;
                if (id_val != .integer or content_val != .string) continue;
                setGlinerSpecialToken(manifest, content_val.string, @intCast(id_val.integer));
            }
        }
    }

    if (obj.get("added_tokens_decoder")) |decoder| {
        if (decoder == .object) {
            var it = decoder.object.iterator();
            while (it.next()) |entry| {
                const token_id = std.fmt.parseInt(i32, entry.key_ptr.*, 10) catch continue;
                if (entry.value_ptr.* != .object) continue;
                const content_val = entry.value_ptr.object.get("content") orelse continue;
                if (content_val != .string) continue;
                setGlinerSpecialToken(manifest, content_val.string, token_id);
            }
        }
    }
}

test "native weight artifact selection has one deterministic precedence" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    manifest.gguf_path = try allocator.dupe(u8, "export.gguf");
    try std.testing.expectEqual(
        NativeWeightArtifactKind.gguf,
        manifest.nativeWeightArtifactKind().?,
    );
    try std.testing.expect(manifest.usesGgufWeights());

    manifest.safetensors_index_path = try allocator.dupe(u8, "model.safetensors.index.json");
    try std.testing.expectEqual(
        NativeWeightArtifactKind.sharded_safetensors,
        manifest.nativeWeightArtifactKind().?,
    );
    try std.testing.expect(!manifest.usesGgufWeights());

    manifest.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expectEqual(
        NativeWeightArtifactKind.safetensors,
        manifest.nativeWeightArtifactKind().?,
    );
}

test "explicit GGUF bundles retain their declared artifact route" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{
        .allocator = allocator,
        .inference_bundle_family = try allocator.dupe(u8, "florence2_gguf_bundle/v1"),
        .gguf_path = try allocator.dupe(u8, "model.gguf"),
        .safetensors_path = try allocator.dupe(u8, "model.safetensors"),
    };
    defer manifest.deinit();

    try std.testing.expectEqual(
        NativeWeightArtifactKind.gguf,
        manifest.nativeWeightArtifactKind().?,
    );
    try std.testing.expect(manifest.usesGgufWeights());
}

test "inferModelTypeFromPath detects classifier directory" {
    try std.testing.expectEqual(@as(?ModelType, .classifier), inferModelTypeFromPath("/tmp/models/classifiers/cross-encoder/nli-distilroberta-base"));
}

test "rerank model name overrides sequence classifier config" {
    var manifest = ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .classifier,
        .model_type_origin = .config,
    };
    try applyImplicitModelTypeHints(&manifest, "/tmp/models/mixedbread-ai/mxbai-rerank-base-v1");
    try std.testing.expectEqual(ModelType.reranker, manifest.model_type);
    try std.testing.expectEqual(ModelTypeOrigin.path, manifest.model_type_origin);
}

test "Whisper conditional generation config remains a transcriber" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseConfigJson(&manifest, allocator,
        \\{"architectures":["WhisperForConditionalGeneration"],"model_type":"whisper"}
    );
    try std.testing.expectEqual(ModelType.generator, manifest.model_type);
    try applyImplicitModelTypeHints(&manifest, "/tmp/models/openai/whisper-tiny");
    try std.testing.expectEqual(ModelType.transcriber, manifest.model_type);
}

test "inferModelTypeFromPath detects extractor directory" {
    try std.testing.expectEqual(@as(?ModelType, .recognizer), inferModelTypeFromPath("C:\\models\\extractors\\fastino\\gliner2-base-v1"));
}

test "parseModelManifestJson parses inputs array" {
    var manifest = ModelManifest{ .allocator = std.testing.allocator };
    defer manifest.deinit();

    try parseModelManifestJson(&manifest, std.testing.allocator,
        \\{"type":"recognizer","tasks":["extract"],"capabilities":["extraction"],"inputs":["text","image"],"sparse_3d_output_layout":"seq_batch"}
    );

    try std.testing.expect(manifest.hasTask("extract"));
    try std.testing.expect(manifest.hasCapability("extraction"));
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
    try std.testing.expectEqual(Sparse3DOutputLayout.seq_batch, manifest.sparse_3d_output_layout.?);
    try std.testing.expectEqual(ModelTypeOrigin.manifest, manifest.model_type_origin);
}

fn parseTokenizerConfig(manifest: *ModelManifest, allocator: std.mem.Allocator, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;

    if (obj.get("added_tokens_decoder")) |v| {
        if (v == .object) {
            var it = v.object.iterator();
            while (it.next()) |entry| {
                const token_id = std.fmt.parseInt(i32, entry.key_ptr.*, 10) catch continue;
                if (entry.value_ptr.* != .object) continue;
                const content_v = entry.value_ptr.object.get("content") orelse continue;
                if (content_v != .string) continue;
                setGlinerSpecialToken(manifest, content_v.string, token_id);
            }
        }
    }

    // Extract special tokens (can be string or {"content": "..."} object)
    manifest.bos_token = try extractToken(allocator, obj, "bos_token");
    manifest.eos_token = try extractToken(allocator, obj, "eos_token");
    manifest.unk_token = try extractToken(allocator, obj, "unk_token");
    manifest.pad_token = try extractToken(allocator, obj, "pad_token");
    if (obj.get("add_bos_token")) |v| {
        if (v == .bool) manifest.add_bos_token = v.bool;
    }
    if (obj.get("add_eos_token")) |v| {
        if (v == .bool) manifest.add_eos_token = v.bool;
    }

    // Chat template can also be in tokenizer_config.json
    if (manifest.chat_template == null) {
        if (obj.get("chat_template")) |v| {
            if (v == .string and v.string.len > 0) {
                manifest.chat_template = try allocator.dupe(u8, v.string);
            }
        }
    }

    // Gemma 4 models use <|turn> and may ship a tool-capable upstream
    // template that requires Jinja features outside our rendering subset.
    if (obj.get("sot_token")) |v| {
        if (v == .string and std.mem.eql(u8, v.string, "<|turn>")) {
            if (manifest.chat_template) |existing| {
                if (gemma4ChatTemplateRequiresBuiltInFallback(existing)) {
                    allocator.free(existing);
                    manifest.chat_template = try allocator.dupe(u8, gemma4_chat_template);
                }
            } else {
                manifest.chat_template = try allocator.dupe(u8, gemma4_chat_template);
            }
        }
    }
}

fn extractToken(allocator: std.mem.Allocator, obj: std.json.ObjectMap, key: []const u8) ![]const u8 {
    if (obj.get(key)) |v| {
        switch (v) {
            .string => |s| if (s.len > 0) return try allocator.dupe(u8, s),
            .object => |o| {
                if (o.get("content")) |cv| {
                    if (cv == .string and cv.string.len > 0)
                        return try allocator.dupe(u8, cv.string);
                }
            },
            else => {},
        }
    }
    return "";
}

fn inferModelTypeFromArchitectureName(arch_name: []const u8) ?ModelType {
    if (std.mem.endsWith(u8, arch_name, "ForTokenClassification")) return .recognizer;
    if (std.mem.endsWith(u8, arch_name, "ForSequenceClassification")) return .classifier;
    if (std.mem.eql(u8, arch_name, "VisionEncoderDecoderModel")) return .reader;
    if (std.mem.endsWith(u8, arch_name, "ForConditionalGeneration")) return .generator;
    if (std.mem.endsWith(u8, arch_name, "ForCausalLM")) return .generator;
    if (std.mem.endsWith(u8, arch_name, "LMHeadModel")) return .generator;
    return null;
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

// -- Tests --

test "manifest from config.json" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    const config_json =
        \\{"model_type": "bert", "hidden_size": 384, "max_position_embeddings": 256, "num_hidden_layers": 6, "vocab_size": 250002, "type_vocab_size": 1, "layer_norm_eps": 0.00001}
    ;
    try parseConfigJson(&manifest, allocator, config_json);

    try std.testing.expectEqual(@as(u32, 384), manifest.hidden_size);
    try std.testing.expectEqual(@as(u32, 256), manifest.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 6), manifest.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 250002), manifest.bert_vocab_size);
    try std.testing.expectEqual(@as(u32, 1), manifest.bert_type_vocab_size);
    try std.testing.expectApproxEqAbs(@as(f32, 0.00001), manifest.bert_layer_norm_eps, 0.0000001);
    try std.testing.expectEqual(bert.ModelType.bert, manifest.bert_model_type);
    try std.testing.expectEqualStrings("bert", manifest.config_model_arch);
    try std.testing.expectEqual(ModelTypeOrigin.config, manifest.model_type_origin);
}

test "RoBERTa manifest reserves padding position indices" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    const config_json =
        \\{"model_type": "roberta", "max_position_embeddings": 514, "pad_token_id": 1}
    ;
    try parseConfigJson(&manifest, allocator, config_json);

    try std.testing.expectEqual(bert.ModelType.roberta, manifest.bert_model_type);
    try std.testing.expectEqual(@as(i64, 1), manifest.bert_pad_token_id);
    try std.testing.expectEqual(@as(usize, 512), manifest.maxTextSequenceLength());
}

test "manifest treats jina embeddings v5 as qwen3 embedder with last pooling" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    const config_json =
        \\{
        \\  "architectures": ["JinaEmbeddingsV5Model"],
        \\  "task_names": ["retrieval", "text-matching", "clustering", "classification"],
        \\  "model_type": "jina_embeddings_v5",
        \\  "hidden_size": 1024,
        \\  "max_position_embeddings": 32768,
        \\  "num_hidden_layers": 28,
        \\  "num_attention_heads": 16
        \\}
    ;
    try parseConfigJson(&manifest, allocator, config_json);

    try std.testing.expectEqual(ModelType.embedder, manifest.model_type);
    try std.testing.expectEqual(PoolingStrategy.last, manifest.pooling);
    try std.testing.expect(manifest.normalize);
    try std.testing.expectEqualStrings("Document: ", manifest.embedding_text_prefix);
    try std.testing.expectEqualStrings("jina_embeddings_v5", manifest.config_model_arch);
    try std.testing.expectEqual(@as(u32, 32768), manifest.max_position_embeddings);
}

test "manifest treats merged jina qwen3 task repo as embedder" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    const config_json =
        \\{
        \\  "architectures": ["Qwen3Model"],
        \\  "task_names": ["retrieval", "text-matching", "clustering", "classification"],
        \\  "model_type": "qwen3",
        \\  "hidden_size": 1024
        \\}
    ;
    try parseConfigJson(&manifest, allocator, config_json);

    try std.testing.expectEqual(ModelType.embedder, manifest.model_type);
    try std.testing.expectEqual(PoolingStrategy.last, manifest.pooling);
    try std.testing.expectEqualStrings("Document: ", manifest.embedding_text_prefix);
}

test "load sparse fixture preserves max position embeddings" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const models_dir = if (std.c.getenv("ANTFLY_INFERENCE_MODELS_DIR")) |value|
        std.mem.span(value)
    else blk: {
        const home = std.c.getenv("HOME") orelse return error.SkipZigTest;
        break :blk try std.fs.path.join(allocator, &.{ std.mem.span(home), ".antfly", "inference", "models" });
    };
    defer if (std.c.getenv("ANTFLY_INFERENCE_MODELS_DIR") == null) allocator.free(models_dir);
    const model_dir = try std.fs.path.join(allocator, &.{ models_dir, "sparse-encoder-testing", "splade-bert-tiny-nq-onnx" });
    defer allocator.free(model_dir);

    Dir.cwd().access(io, model_dir, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(@as(u32, 512), manifest.max_position_embeddings);
}

test "loadFromDir infers SPLADE sparse output layout from pooling sidecar" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model/1_SpladePooling");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/config.json", .data = "{\"model_type\":\"bert\"}" });
    try tmp.dir.writeFile(io, .{ .sub_path = "model/1_SpladePooling/config.json", .data = "{}" });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(dir_path);

    var manifest = try loadFromDir(allocator, dir_path);
    defer manifest.deinit();

    try std.testing.expectEqual(Sparse3DOutputLayout.batch_seq, manifest.sparse_3d_output_layout.?);
}

test "manifest from model_manifest.json" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    const manifest_json =
        \\{"type": "reranker", "name": "test-model"}
    ;
    try parseModelManifestJson(&manifest, allocator, manifest_json);

    try std.testing.expectEqual(ModelType.reranker, manifest.model_type);
}

test "manifest detects gliner gguf head sidecar" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-gliner-head");
    defer allocator.free(dir_path);
    defer compat.cwd().deleteTree(compat.io(), dir_path) catch {};

    const head_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner_head.gguf" });
    defer allocator.free(head_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = head_path, .data = "" });

    var manifest = try loadFromDir(allocator, dir_path);
    defer manifest.deinit();
    try std.testing.expect(manifest.gliner_head_gguf_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gliner_head_gguf_path.?, "gliner_head.gguf"));
}

test "manifest reads gliner special tokens from tokenizer json" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-gliner-tokenizer-json");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const gliner_config_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner_config.json" });
    defer allocator.free(gliner_config_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = gliner_config_path, .data = "{\"model_type\":\"gliner2\"}" });

    const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data =
        \\{"version":"1.0","added_tokens":[
        \\{"id":32000,"content":"[P]"},
        \\{"id":32001,"content":"[E]"},
        \\{"id":32002,"content":"[SEP_TEXT]"},
        \\{"id":32003,"content":"[C]"},
        \\{"id":32004,"content":"[R]"}],
        \\"model":{"type":"BPE","vocab":{},"merges":[]}}
        ,
    });

    var manifest = try loadFromDir(allocator, dir_path);
    defer manifest.deinit();
    try std.testing.expectEqual(@as(i32, 32000), manifest.gliner_token_p);
    try std.testing.expectEqual(@as(i32, 32001), manifest.gliner_token_e);
    try std.testing.expectEqual(@as(i32, 32002), manifest.gliner_token_sep_text);
    try std.testing.expectEqual(@as(i32, 32003), manifest.gliner_token_c);
    try std.testing.expectEqual(@as(i32, 32004), manifest.gliner_token_r);
    try std.testing.expectEqualStrings("gliner2", manifest.gliner_model_type);
}

test "manifest detects incomplete colqwen bundle" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-colqwen-incomplete");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = config_path, .data = "{\"model_type\":\"qwen2\"}" });

    const model_manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "model_manifest.json" });
    defer allocator.free(model_manifest_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = model_manifest_path,
        .data = "{\"type\":\"reranker\",\"capabilities\":[\"colqwen\",\"multimodal_late_interaction\"],\"inputs\":[\"text\",\"image\"]}",
    });

    const bundle_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_bundle.json" });
    defer allocator.free(bundle_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = bundle_path, .data = "{\"family\":\"colqwen2_gguf_bundle/v1\"}" });

    const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data = "{\"version\":\"1.0\",\"model\":{\"type\":\"BPE\",\"vocab\":{},\"merges\":[]}}",
    });

    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer_config.json" });
    defer allocator.free(tokenizer_config_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_config_path, .data = "{\"model_max_length\":16}" });

    const preprocessor_path = try std.fs.path.join(allocator, &.{ dir_path, "preprocessor_config.json" });
    defer allocator.free(preprocessor_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = preprocessor_path, .data = "{\"patch_size\":14}" });

    const gguf_path = try std.fs.path.join(allocator, &.{ dir_path, "model.gguf" });
    defer allocator.free(gguf_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = gguf_path, .data = "GGUFstub" });

    var manifest = try loadFromDir(allocator, dir_path);
    defer manifest.deinit();
    try std.testing.expect(manifest.isColqwenBundle());
    try std.testing.expect(manifest.hasIncompleteColqwenBundle());
}

test "manifest parses Antfly inference bundle marker" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceBundleJson(&manifest, allocator, ".",
        \\{"family":"gliner2_split_bundle/v1","wrapper":"gliner2"}
    );

    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", manifest.inference_bundle_family);
    try std.testing.expectEqualStrings("gliner2", manifest.gliner_model_type);
}

test "manifest parses clipclap gguf bundle marker" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "clipclap-q4_k");
    try tmp.dir.writeFile(io, .{ .sub_path = "clipclap-q4_k/clip.gguf", .data = "clip" });
    try tmp.dir.writeFile(io, .{ .sub_path = "clipclap-q4_k/clap.gguf", .data = "clap" });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "clipclap-q4_k" });
    defer allocator.free(model_dir);
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceBundleJson(&manifest, allocator, model_dir,
        \\{"family":"clipclap_gguf_bundle/v1","clip":"clip.gguf","clap":"clap.gguf","inputs":["text","image","audio"],"projections_embedded":true}
    );

    try std.testing.expect(manifest.isClipclapGgufBundle());
    try std.testing.expectEqual(ModelTypeOrigin.bundle, manifest.model_type_origin);
    try std.testing.expectEqual(NativeArchHint.clip, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("clipclap", manifest.config_model_arch);
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(manifest.audio_model_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_path.?, "/clipclap-q4_k/clip.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.audio_model_path.?, "/clipclap-q4_k/clap.gguf"));
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
    try std.testing.expect(manifest.hasInput("audio"));
}

test "manifest parses florence2 gguf bundle marker" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "florence2-q4_k");
    try tmp.dir.writeFile(io, .{ .sub_path = "florence2-q4_k/florence-2-base.Q4_K.gguf", .data = "model" });
    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "florence2-q4_k" });
    defer allocator.free(model_dir);
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceBundleJson(&manifest, allocator, model_dir,
        \\{"family":"florence2_gguf_bundle/v1","model":"florence-2-base.Q4_K.gguf","inputs":["text","image"]}
    );

    try std.testing.expect(manifest.isFlorence2GgufBundle());
    try std.testing.expect(manifest.hasIncompleteFlorence2GgufBundle());
    try std.testing.expectEqual(ModelType.reader, manifest.model_type);
    try std.testing.expectEqual(NativeArchHint.florence, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("florence2", manifest.config_model_arch);
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_path.?, "/florence2-q4_k/florence-2-base.Q4_K.gguf"));
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
}

test "manifest discovers clip onnx variants and prefers f16 over i8" {
    const allocator = std.testing.allocator;
    const model_dir = try testScratchDir(allocator, "manifest-clip-onnx-f16-preferred");
    defer {
        compat.cwd().deleteTree(compat.io(), model_dir) catch {};
        allocator.free(model_dir);
    }

    const files = [_][]const u8{
        "text_model_i8.onnx",
        "text_model_f16.onnx",
        "visual_model_i8.onnx",
        "visual_model_f16.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ model_dir, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expect(manifest.onnx_path != null);
    try std.testing.expect(manifest.visual_model_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.onnx_path.?, "/text_model_f16.onnx"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.visual_model_path.?, "/visual_model_f16.onnx"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.text_projection_path.?, "/text_projection.onnx"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.visual_projection_path.?, "/visual_projection.onnx"));
}

test "manifest prefers split clip text model over combined model" {
    const allocator = std.testing.allocator;
    const model_dir = try testScratchDir(allocator, "manifest-clip-text-model-before-combined");
    defer {
        compat.cwd().deleteTree(compat.io(), model_dir) catch {};
        allocator.free(model_dir);
    }

    const files = [_][]const u8{
        "model.onnx",
        "text_model.onnx",
        "vision_model.onnx",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ model_dir, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expect(manifest.onnx_path != null);
    try std.testing.expect(manifest.visual_model_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.onnx_path.?, "/text_model.onnx"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.visual_model_path.?, "/vision_model.onnx"));
}

test "manifest discovers clip i8 onnx fallback variants" {
    const allocator = std.testing.allocator;
    const model_dir = try testScratchDir(allocator, "manifest-clip-onnx-i8-fallback");
    defer {
        compat.cwd().deleteTree(compat.io(), model_dir) catch {};
        allocator.free(model_dir);
    }

    const files = [_][]const u8{
        "text_model_i8.onnx",
        "visual_model_i8.onnx",
    };
    for (files) |file_name| {
        const path = try std.fs.path.join(allocator, &.{ model_dir, file_name });
        defer allocator.free(path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = "" });
    }

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expect(manifest.onnx_path != null);
    try std.testing.expect(manifest.visual_model_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.onnx_path.?, "/text_model_i8.onnx"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.visual_model_path.?, "/visual_model_i8.onnx"));
}

test "manifest parses clipclap variants gguf pair" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-clipclap-variants-gguf");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const clip_path = try std.fs.path.join(allocator, &.{ dir_path, "clipclap-clip.Q4_K.gguf" });
    defer allocator.free(clip_path);
    const clap_path = try std.fs.path.join(allocator, &.{ dir_path, "clipclap-clap.Q4_K.gguf" });
    defer allocator.free(clap_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = clip_path, .data = "clip" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = clap_path, .data = "clap" });

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "clipclap_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "clip": "clipclap-clip.Q4_K.gguf",
        \\      "clap": "clipclap-clap.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(manifest.isClipclapGgufBundle());
    try std.testing.expectEqual(NativeArchHint.clip, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("clipclap", manifest.config_model_arch);
    try expectCanonicalPath(allocator, clip_path, manifest.gguf_path.?);
    try expectCanonicalPath(allocator, clap_path, manifest.audio_model_path.?);
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
    try std.testing.expect(manifest.hasInput("audio"));
}

test "manifest loads canonical antfly clipclap variants before first gguf fallback" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-clipclap-canonical-variants");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const clip_path = try std.fs.path.join(allocator, &.{ dir_path, "clipclap-clip.Q4_K.gguf" });
    defer allocator.free(clip_path);
    const clap_path = try std.fs.path.join(allocator, &.{ dir_path, "clipclap-clap.Q4_K.gguf" });
    defer allocator.free(clap_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = clip_path, .data = "GGUFstub" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = clap_path, .data = "GGUFstub" });

    const model_manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "model_manifest.json" });
    defer allocator.free(model_manifest_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = model_manifest_path,
        .data = "{\"type\":\"embedder\",\"tasks\":[\"embed\"],\"inputs\":[\"text\",\"image\",\"audio\"]}",
    });

    const clip_config_path = try std.fs.path.join(allocator, &.{ dir_path, "clip_config.json" });
    defer allocator.free(clip_config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = clip_config_path,
        .data = "{\"model_type\":\"clipclap\",\"text_config\":{\"max_position_embeddings\":77}}",
    });

    const variants_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(variants_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = variants_path,
        .data =
        \\{
        \\  "family": "clipclap_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "clip": "clipclap-clip.Q4_K.gguf",
        \\      "clap": "clipclap-clap.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
        ,
    });

    var manifest = try loadFromDir(allocator, dir_path);
    defer manifest.deinit();

    try std.testing.expect(manifest.isClipclapGgufBundle());
    try std.testing.expectEqual(NativeArchHint.clip, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("clipclap", manifest.config_model_arch);
    try expectCanonicalPath(allocator, clip_path, manifest.gguf_path.?);
    try expectCanonicalPath(allocator, clap_path, manifest.audio_model_path.?);
}

test "manifest ignores stale clipclap variants with missing gguf files" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-clipclap-stale-variants");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "clipclap_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "clip": "clipclap-clip.Q4_K.gguf",
        \\      "clap": "clipclap-clap.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(!manifest.isClipclapGgufBundle());
    try std.testing.expectEqual(@as(?[]const u8, null), manifest.gguf_path);
    try std.testing.expectEqual(@as(?[]const u8, null), manifest.audio_model_path);
}

test "manifest falls back to first existing clipclap variant when preferred pair is stale" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-clipclap-variants-fallback");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const clip_path = try std.fs.path.join(allocator, &.{ dir_path, "clipclap-clip.Q8_0.gguf" });
    defer allocator.free(clip_path);
    const clap_path = try std.fs.path.join(allocator, &.{ dir_path, "clipclap-clap.Q8_0.gguf" });
    defer allocator.free(clap_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = clip_path, .data = "clip" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = clap_path, .data = "clap" });

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "clipclap_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q8_0",
        \\      "target": "gguf",
        \\      "format": "Q8_0",
        \\      "clip": "clipclap-clip.Q8_0.gguf",
        \\      "clap": "clipclap-clap.Q8_0.gguf"
        \\    },
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "clip": "clipclap-clip.Q4_K.gguf",
        \\      "clap": "clipclap-clap.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(manifest.isClipclapGgufBundle());
    try expectCanonicalPath(allocator, clip_path, manifest.gguf_path.?);
    try expectCanonicalPath(allocator, clap_path, manifest.audio_model_path.?);
}

test "manifest parses gliner2 variants gguf pair" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-gliner2-variants-gguf");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const encoder_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner2-encoder.Q4_K.gguf" });
    defer allocator.free(encoder_path);
    const head_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner2-head.Q4_K.gguf" });
    defer allocator.free(head_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = encoder_path, .data = "GGUFstub" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = head_path, .data = "GGUFstub" });

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "gliner2_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "encoder": "gliner2-encoder.Q4_K.gguf",
        \\      "head": "gliner2-head.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(manifest.isSplitGlinerBundle());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", manifest.inference_bundle_family);
    try std.testing.expectEqualStrings("gliner2", manifest.gliner_model_type);
    try expectCanonicalPath(allocator, encoder_path, manifest.gguf_path.?);
    try expectCanonicalPath(allocator, head_path, manifest.gliner_head_gguf_path.?);
    try std.testing.expect(manifest.hasInput("text"));
}

test "manifest parses florence2 variants gguf model" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-florence2-variants-gguf");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const q4_path = try std.fs.path.join(allocator, &.{ dir_path, "florence-2-base.Q4_K.gguf" });
    defer allocator.free(q4_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = q4_path, .data = "GGUFstub" });

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "florence2_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "model": "florence-2-base.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(manifest.isFlorence2GgufBundle());
    try std.testing.expectEqual(ModelType.reader, manifest.model_type);
    try std.testing.expectEqual(NativeArchHint.florence, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("florence2", manifest.config_model_arch);
    try expectCanonicalPath(allocator, q4_path, manifest.gguf_path.?);
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
}

test "manifest parses lowercase florence variants gguf model" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-florence-lowercase-variants-gguf");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const q8_path = try std.fs.path.join(allocator, &.{ dir_path, "florence2.Q8_0.gguf" });
    defer allocator.free(q8_path);
    const q4_path = try std.fs.path.join(allocator, &.{ dir_path, "florence2.Q4_K.gguf" });
    defer allocator.free(q4_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = q8_path, .data = "GGUFstub" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = q4_path, .data = "GGUFstub" });

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "florence_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q8_0",
        \\      "target": "gguf",
        \\      "format": "Q8_0",
        \\      "model": "florence2.Q8_0.gguf"
        \\    },
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "model": "florence2.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(manifest.isFlorence2GgufBundle());
    try std.testing.expectEqual(ModelType.reader, manifest.model_type);
    try std.testing.expectEqual(NativeArchHint.florence, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("florence2", manifest.config_model_arch);
    try expectCanonicalPath(allocator, q4_path, manifest.gguf_path.?);
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
}

test "manifest loads canonical antfly florence2 variants before first gguf fallback" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-florence2-canonical-variants");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const q8_path = try std.fs.path.join(allocator, &.{ dir_path, "florence-2-base.Q8_0.gguf" });
    defer allocator.free(q8_path);
    const q4_path = try std.fs.path.join(allocator, &.{ dir_path, "florence-2-base.Q4_K.gguf" });
    defer allocator.free(q4_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = q8_path, .data = "GGUFstub" });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = q4_path, .data = "GGUFstub" });

    const config_path = try std.fs.path.join(allocator, &.{ dir_path, "config.json" });
    defer allocator.free(config_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data = "{\"model_type\":\"florence2\",\"text_config\":{\"d_model\":768},\"vision_config\":{\"image_size\":768}}",
    });
    const model_manifest_path = try std.fs.path.join(allocator, &.{ dir_path, "model_manifest.json" });
    defer allocator.free(model_manifest_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = model_manifest_path,
        .data = "{\"type\":\"reader\",\"tasks\":[\"read\"],\"inputs\":[\"text\",\"image\"]}",
    });
    const tokenizer_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_path, .data = "{}" });
    const tokenizer_config_path = try std.fs.path.join(allocator, &.{ dir_path, "tokenizer_config.json" });
    defer allocator.free(tokenizer_config_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = tokenizer_config_path, .data = "{}" });
    const preprocessor_path = try std.fs.path.join(allocator, &.{ dir_path, "preprocessor_config.json" });
    defer allocator.free(preprocessor_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = preprocessor_path, .data = "{\"size\":{\"height\":768,\"width\":768}}" });

    const variants_path = try std.fs.path.join(allocator, &.{ dir_path, "antfly_inference_variants.json" });
    defer allocator.free(variants_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = variants_path,
        .data =
        \\{
        \\  "family": "florence2_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q8_0",
        \\      "target": "gguf",
        \\      "format": "Q8_0",
        \\      "model": "florence-2-base.Q8_0.gguf"
        \\    },
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "model": "florence-2-base.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
        ,
    });

    var manifest = try loadFromDir(allocator, dir_path);
    defer manifest.deinit();

    try std.testing.expect(manifest.isFlorence2GgufBundle());
    try std.testing.expect(!manifest.hasIncompleteFlorence2GgufBundle());
    try std.testing.expectEqual(ModelType.reader, manifest.model_type);
    try std.testing.expectEqual(NativeArchHint.florence, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("florence2", manifest.config_model_arch);
    try expectCanonicalPath(allocator, q4_path, manifest.gguf_path.?);
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
}

test "manifest ignores stale florence2 variants with missing gguf files" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-florence2-stale-variants");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceVariantsJson(&manifest, allocator, dir_path,
        \\{
        \\  "family": "florence2_variants/v1",
        \\  "variants": [
        \\    {
        \\      "id": "gguf-Q4_K",
        \\      "target": "gguf",
        \\      "format": "Q4_K",
        \\      "model": "florence-2-base.Q4_K.gguf"
        \\    }
        \\  ]
        \\}
    );

    try std.testing.expect(!manifest.isFlorence2GgufBundle());
    try std.testing.expectEqual(@as(?[]const u8, null), manifest.gguf_path);
}

test "manifest uses clipclap variants when default ONNX bundle is partial" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-clipclap-partial-onnx");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const onnx_path = try std.fs.path.join(allocator, &.{ dir_path, "text_model.onnx" });
    defer allocator.free(onnx_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = onnx_path, .data = "" });

    var catalog = try ArtifactCatalog.initPublished(allocator, dir_path);
    defer catalog.deinit();
    try std.testing.expect(shouldUseClipclapGgufVariant(&catalog));
}

test "manifest keeps default clipclap ONNX when six model files are present" {
    const allocator = std.testing.allocator;
    const dir_path = try testScratchDir(allocator, "manifest-clipclap-complete-onnx");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }

    const onnx_files = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (onnx_files) |file_name| {
        const file_path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
        defer allocator.free(file_path);
        try compat.cwd().writeFile(compat.io(), .{ .sub_path = file_path, .data = "" });
    }

    var catalog = try ArtifactCatalog.initPublished(allocator, dir_path);
    defer catalog.deinit();
    try std.testing.expect(!shouldUseClipclapGgufVariant(&catalog));
}

test "manifest distilbert detection" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    const config_json =
        \\{"model_type": "distilbert", "hidden_size": 768}
    ;
    try parseConfigJson(&manifest, allocator, config_json);

    try std.testing.expectEqual(bert.ModelType.distilbert, manifest.bert_model_type);
    try std.testing.expectEqualStrings("distilbert", manifest.config_model_arch);
}

test "manifest late interaction generation preference detects qwen2" {
    const allocator = std.testing.allocator;
    var manifest_inst = ModelManifest{ .allocator = allocator };
    defer manifest_inst.deinit();

    const config_json =
        \\{"model_type": "qwen2", "hidden_size": 896}
    ;
    try parseConfigJson(&manifest_inst, allocator, config_json);

    try std.testing.expect(manifest_inst.prefersGenerationEncodingForLateInteraction());
}

test "gemma4 gguf tool chat template uses built-in fallback" {
    const gguf_tool_template =
        "{%- macro format_parameters(properties, required, filter_keys=false) -%}" ++
        "{%- set ns = namespace(found_first=false) -%}" ++
        "{%- set captured_content -%}{{ message.get('content') }}{%- endset -%}";

    try std.testing.expect(shouldUseBuiltInGemma4GgufChatTemplate("gemma4", gguf_tool_template));
    try std.testing.expect(!shouldUseBuiltInGemma4GgufChatTemplate("llama", gguf_tool_template));
    try std.testing.expect(!shouldUseBuiltInGemma4GgufChatTemplate("gemma4", "{{ bos_token }}{{ messages[0]['content'] }}"));
}

test "manifest detects layoutlmv3 as classifier-native bundle" {
    const allocator = std.testing.allocator;
    var manifest_inst = ModelManifest{ .allocator = allocator };
    defer manifest_inst.deinit();

    const config_json =
        \\{"model_type":"layoutlmv3","hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12}
    ;
    try parseConfigJson(&manifest_inst, allocator, config_json);

    try std.testing.expectEqual(NativeArchHint.layoutlmv3, manifest_inst.native_arch_hint);
    try std.testing.expectEqual(ModelType.classifier, manifest_inst.model_type);
    try std.testing.expectEqualStrings("layoutlmv3", manifest_inst.config_model_arch);
}

test "manifest detects layoutlmv3 token classification architecture as recognizer" {
    const allocator = std.testing.allocator;
    const model_dir = try testScratchDir(allocator, "manifest-layoutlmv3-token-recognizer");
    defer {
        compat.cwd().deleteTree(compat.io(), model_dir) catch {};
        allocator.free(model_dir);
    }
    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "config.json" });
    defer allocator.free(config_path);
    const tokenizer_path = try std.fs.path.join(allocator, &.{ model_dir, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = config_path,
        .data =
        \\{"model_type":"layoutlmv3","architectures":["LayoutLMv3ForTokenClassification"],"hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"num_labels":2}
        ,
    });
    try compat.cwd().writeFile(compat.io(), .{
        .sub_path = tokenizer_path,
        .data = "{}",
    });

    var manifest_inst = try loadFromDir(allocator, model_dir);
    defer manifest_inst.deinit();
    try std.testing.expectEqual(ModelType.recognizer, manifest_inst.model_type);
    try std.testing.expectEqual(NativeArchHint.layoutlmv3, manifest_inst.native_arch_hint);
}

fn testScratchDir(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const root = try std.fmt.allocPrint(allocator, "antfly-inference-model-tests-{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    const dir_path = try std.fs.path.join(allocator, &.{ "/tmp", root, name });
    errdefer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    return dir_path;
}

fn expectCanonicalPath(
    allocator: std.mem.Allocator,
    expected_path: []const u8,
    actual_path: []const u8,
) !void {
    const expected_canonical = try Dir.cwd().realPathFileAlloc(std.testing.io, expected_path, allocator);
    defer allocator.free(expected_canonical);
    try std.testing.expectEqualStrings(expected_canonical, actual_path);
}

test "manifest gguf discovery separates decoder and projector files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "mmproj-gemma-4-e2b-it-f16.gguf", .data = "projector" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-e2b-it-Q8_0.gguf", .data = "decoder" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    const decoder = try findFirstGgufInDir(allocator, model_dir, false) orelse return error.TestExpectedDecoderGguf;
    defer allocator.free(decoder);
    const projector = try findFirstGgufInDir(allocator, model_dir, true) orelse return error.TestExpectedProjectorGguf;
    defer allocator.free(projector);

    try std.testing.expect(std.mem.endsWith(u8, decoder, "gemma-4-e2b-it-Q8_0.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, projector, "mmproj-gemma-4-e2b-it-f16.gguf"));
}

test "manifest gguf discovery prefers q8 projector over stale dense sidecars" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Write dense projectors first to ensure filesystem iteration order cannot
    // override the bounded-residency preference used by managed downloads.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "mmproj-gemma-4-e2b-it-BF16.gguf", .data = "bf16" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "mmproj-gemma-4-e2b-it-F16.gguf", .data = "f16" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "mmproj-gemma-4-e2b-it-Q8_0.gguf", .data = "q8" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-e2b-it-Q4_0.gguf", .data = "decoder" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    const projector = try findFirstGgufInDir(allocator, model_dir, true) orelse return error.TestExpectedProjectorGguf;
    defer allocator.free(projector);
    try std.testing.expect(std.mem.endsWith(u8, projector, "mmproj-gemma-4-e2b-it-Q8_0.gguf"));
}

test "manifest gguf discovery loads nested managed projector layouts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "artifacts/projectors");
    try tmp.dir.writeFile(io, .{ .sub_path = "gemma-4-e2b-it-Q4_0.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{ .sub_path = "artifacts/projectors/mmproj-gemma-4-e2b-it-Q8_0.gguf", .data = "projector" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    const decoder = try findFirstGgufInDir(allocator, model_dir, false) orelse return error.TestExpectedDecoderGguf;
    defer allocator.free(decoder);
    const projector = try findFirstGgufInDir(allocator, model_dir, true) orelse return error.TestExpectedProjectorGguf;
    defer allocator.free(projector);

    try std.testing.expect(std.mem.endsWith(u8, decoder, "gemma-4-e2b-it-Q4_0.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, projector, "artifacts/projectors/mmproj-gemma-4-e2b-it-Q8_0.gguf"));
}

test "bundle artifact paths stay within the canonical model root" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model/artifacts");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/artifacts/model.gguf", .data = "inside" });
    try tmp.dir.writeFile(io, .{ .sub_path = "outside.gguf", .data = "outside" });
    try tmp.dir.symLink(io, "../outside.gguf", "model/escape.gguf", .{});
    try tmp.dir.symLink(io, "artifacts/model.gguf", "model/alias.gguf", .{});

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);
    try std.testing.expectError(
        error.InvalidModelArtifactPath,
        resolveBundlePath(allocator, null, model_dir, "../outside.gguf"),
    );
    const outside_relative = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "outside.gguf" });
    defer allocator.free(outside_relative);
    const outside_path = try Dir.cwd().realPathFileAlloc(io, outside_relative, allocator);
    defer allocator.free(outside_path);
    try std.testing.expectError(
        error.InvalidModelArtifactPath,
        resolveBundlePath(allocator, null, model_dir, outside_path),
    );
    try std.testing.expectError(
        error.ModelArtifactOutsideRoot,
        resolveBundlePath(allocator, null, model_dir, "escape.gguf"),
    );
    const resolved = try resolveBundlePath(allocator, null, model_dir, "alias.gguf");
    defer allocator.free(resolved);
    try std.testing.expect(std.mem.endsWith(u8, resolved, "model/artifacts/model.gguf"));
}

test "managed receipt is authoritative for gguf discovery" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "artifacts/current");
    try tmp.dir.writeFile(io, .{ .sub_path = "artifacts/current/z-decoder.gguf", .data = "managed-decoder" });
    try tmp.dir.writeFile(io, .{ .sub_path = "artifacts/current/mmproj-z-Q8_0.gguf", .data = "managed-projector" });
    // These shallower, lexically earlier files would win an unrestricted
    // directory scan but are not part of the committed publication.
    try tmp.dir.writeFile(io, .{ .sub_path = "a-decoder.gguf", .data = "stale" });
    try tmp.dir.writeFile(io, .{ .sub_path = "mmproj-a-Q8_0.gguf", .data = "stale" });
    try tmp.dir.writeFile(io, .{
        .sub_path = managed_receipt.complete_filename,
        .data =
        \\{"version":1,"artifacts":[{"path":"artifacts/current/z-decoder.gguf","size":15},{"path":"artifacts/current/mmproj-z-Q8_0.gguf","size":17}]}
        ,
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var discovered = try discoverGgufPaths(allocator, model_dir);
    defer discovered.deinit(allocator);
    try std.testing.expect(discovered.decoder != null);
    try std.testing.expect(discovered.projector != null);
    try std.testing.expect(std.mem.endsWith(u8, discovered.decoder.?, "artifacts/current/z-decoder.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, discovered.projector.?, "artifacts/current/mmproj-z-Q8_0.gguf"));
}

test "managed manifest loading ignores unreceipted metadata and payloads" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "artifacts/current");
    try tmp.dir.createDirPath(io, "onnx");
    try tmp.dir.writeFile(io, .{ .sub_path = "artifacts/current/z-decoder.gguf", .data = "managed-decoder" });
    try tmp.dir.writeFile(io, .{ .sub_path = "onnx/model.onnx", .data = "managed-onnx" });
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "stale-onnx" });
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"hidden_size\":1234}" });
    try tmp.dir.writeFile(io, .{ .sub_path = "antfly_inference_bundle.json", .data = "{\"family\":\"clipclap_gguf_bundle/v1\",\"clip\":\"stale.gguf\",\"clap\":\"stale-clap.gguf\"}" });
    try tmp.dir.writeFile(io, .{
        .sub_path = managed_receipt.complete_filename,
        .data =
        \\{"version":1,"artifacts":[{"path":"artifacts/current/z-decoder.gguf","size":15},{"path":"onnx/model.onnx","size":12}]}
        ,
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(@as(u32, 768), manifest.hidden_size);
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_path.?, "artifacts/current/z-decoder.gguf"));
    try std.testing.expect(manifest.onnx_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.onnx_path.?, "onnx/model.onnx"));
    try std.testing.expectEqualStrings("", manifest.inference_bundle_family);
}

test "managed bundle metadata cannot reference unreceipted artifacts" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const bundle_json = "{\"family\":\"clipclap_gguf_bundle/v1\",\"clip\":\"stale.gguf\",\"clap\":\"stale-clap.gguf\"}";

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{ .sub_path = "antfly_inference_bundle.json", .data = bundle_json });
    const receipt_json = try std.fmt.allocPrint(
        allocator,
        "{{\"version\":1,\"artifacts\":[{{\"path\":\"model.gguf\",\"size\":7}},{{\"path\":\"antfly_inference_bundle.json\",\"size\":{d}}}]}}",
        .{bundle_json.len},
    );
    defer allocator.free(receipt_json);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt_json });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expectEqualStrings("", manifest.inference_bundle_family);
    try std.testing.expectEqual(@as(?[]const u8, null), manifest.audio_model_path);
}

test "managed explicit bundles retain their receipted artifact route" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const bundle_json = "{\"family\":\"clipclap_gguf_bundle/v1\",\"clip\":\"clip.gguf\",\"clap\":\"clap.gguf\"}";

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "clip.gguf", .data = "clip" });
    try tmp.dir.writeFile(io, .{ .sub_path = "clap.gguf", .data = "clap" });
    try tmp.dir.writeFile(io, .{ .sub_path = "antfly_inference_bundle.json", .data = bundle_json });
    const receipt_json = try std.fmt.allocPrint(
        allocator,
        "{{\"version\":1,\"artifacts\":[{{\"path\":\"clip.gguf\",\"size\":4}},{{\"path\":\"clap.gguf\",\"size\":4}},{{\"path\":\"antfly_inference_bundle.json\",\"size\":{d}}}]}}",
        .{bundle_json.len},
    );
    defer allocator.free(receipt_json);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt_json });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expect(manifest.isClipclapGgufBundle());
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_path.?, "clip.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.audio_model_path.?, "clap.gguf"));
}

test "private staging manifests load only through the validated plan API" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "decoder" });
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"hidden_size\":42}" });
    try tmp.dir.writeFile(io, .{
        .sub_path = managed_receipt.in_progress_filename,
        .data = "{\"version\":1,\"state\":\"in_progress\"}",
    });
    try tmp.dir.writeFile(io, .{
        .sub_path = managed_receipt.plan_filename,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7},{\"path\":\"config.json\",\"size\":18}]}",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    try std.testing.expectError(error.IncompleteManagedDownload, loadFromDir(allocator, model_dir));
    var manifest = try loadFromManagedPlanDir(allocator, model_dir);
    defer manifest.deinit();
    try std.testing.expectEqual(@as(u32, 42), manifest.hidden_size);
    try std.testing.expect(manifest.gguf_path != null);
}

test "direct gguf paths honor the nearest managed publication receipt" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model/aliases");
    try tmp.dir.createDirPath(io, "model/artifacts");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/artifacts/published.gguf", .data = "published" });
    try tmp.dir.writeFile(io, .{ .sub_path = "model/artifacts/stale.gguf", .data = "stale" });
    try tmp.dir.symLink(io, "../artifacts/published.gguf", "model/aliases/published.gguf", .{});
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"artifacts/published.gguf\",\"size\":9}]}",
    });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "model" });
    defer allocator.free(model_dir);
    const published_path = try std.fs.path.join(allocator, &.{ model_dir, "artifacts", "published.gguf" });
    defer allocator.free(published_path);
    const stale_path = try std.fs.path.join(allocator, &.{ model_dir, "artifacts", "stale.gguf" });
    defer allocator.free(stale_path);
    const alias_path = try std.fs.path.join(allocator, &.{ model_dir, "aliases", "published.gguf" });
    defer allocator.free(alias_path);

    var manifest = try loadFromDir(allocator, published_path);
    defer manifest.deinit();
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expectError(error.ModelArtifactNotPublished, loadFromDir(allocator, stale_path));
    try std.testing.expectError(error.ModelArtifactNotPublished, loadListingFromDir(allocator, stale_path));
    try std.testing.expectError(error.ModelArtifactNotPublished, loadFromDir(allocator, alias_path));
    try std.testing.expectError(error.ModelArtifactNotPublished, loadListingFromDir(allocator, alias_path));

    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"artifacts/published.gguf\",\"size\":9},{\"path\":\"aliases/published.gguf\",\"size\":9}]}",
    });
    var alias_manifest = try loadFromDir(allocator, alias_path);
    defer alias_manifest.deinit();
    try std.testing.expect(alias_manifest.gguf_path != null);

    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-in-progress",
        .data = "{\"version\":1,\"state\":\"in_progress\"}",
    });
    try std.testing.expectError(error.IncompleteManagedDownload, loadFromDir(allocator, published_path));
    try std.testing.expectError(error.IncompleteManagedDownload, loadListingFromDir(allocator, published_path));
}

test "direct unmanaged gguf paths must resolve to regular files" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const missing_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "missing.gguf" });
    defer allocator.free(missing_path);
    try std.testing.expectError(error.FileNotFound, loadFromDir(allocator, missing_path));
    try std.testing.expectError(error.FileNotFound, loadListingFromDir(allocator, missing_path));

    try tmp.dir.createDir(io, "directory.gguf", .default_dir);
    const directory_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "directory.gguf" });
    defer allocator.free(directory_path);
    try std.testing.expectError(error.InvalidModelArtifactKind, loadFromDir(allocator, directory_path));
    try std.testing.expectError(error.InvalidModelArtifactKind, loadListingFromDir(allocator, directory_path));
}

test "direct managed gguf loading cleans up every allocation failure" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "model/artifacts");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/artifacts/model.gguf", .data = "published" });
    try tmp.dir.writeFile(io, .{
        .sub_path = "model/.antfly-download-complete.json",
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"artifacts/model.gguf\",\"size\":9}]}",
    });
    const model_path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
        "model",
        "artifacts",
        "model.gguf",
    });
    defer allocator.free(model_path);

    const Runner = struct {
        fn run(alloc: std.mem.Allocator, path: []const u8) !void {
            var manifest = try loadFromDir(alloc, path);
            defer manifest.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(allocator, Runner.run, .{model_path});
}

test "gguf discovery resolves unknown filesystem entry kinds" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, "nested");
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "model" });

    const file_entry: Dir.Walker.Entry = .{
        .dir = tmp.dir,
        .basename = "model.gguf",
        .path = "model.gguf",
        .kind = .unknown,
    };
    const directory_entry: Dir.Walker.Entry = .{
        .dir = tmp.dir,
        .basename = "nested",
        .path = "nested",
        .kind = .unknown,
    };
    try std.testing.expectEqual(std.Io.File.Kind.file, (try resolvedWalkerEntryKind(io, file_entry)).?);
    try std.testing.expectEqual(std.Io.File.Kind.directory, (try resolvedWalkerEntryKind(io, directory_entry)).?);

    const vanished_entry: Dir.Walker.Entry = .{
        .dir = tmp.dir,
        .basename = "vanished.gguf",
        .path = "vanished.gguf",
        .kind = .unknown,
    };
    try std.testing.expectEqual(@as(?std.Io.File.Kind, null), try resolvedWalkerEntryKind(io, vanished_entry));
}

test "manifest gguf discovery skips hidden artifact trees" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(io, ".stale");
    try tmp.dir.writeFile(io, .{ .sub_path = ".stale/mmproj-gemma-4-e2b-it-Q8_0.gguf", .data = "stale" });
    try tmp.dir.writeFile(io, .{ .sub_path = "mmproj-gemma-4-e2b-it-BF16.gguf", .data = "active" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    const projector = try findFirstGgufInDir(allocator, model_dir, true) orelse return error.TestExpectedProjectorGguf;
    defer allocator.free(projector);
    try std.testing.expect(std.mem.endsWith(u8, projector, "mmproj-gemma-4-e2b-it-BF16.gguf"));
}

test "manifest gguf discovery handles google gemma4 e4b qat layout" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-E4B-it-mmproj.gguf", .data = "projector" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-E4B_q4_0-it.gguf", .data = "decoder" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    const decoder = try findFirstGgufInDir(allocator, model_dir, false) orelse return error.TestExpectedDecoderGguf;
    defer allocator.free(decoder);
    const projector = try findFirstGgufInDir(allocator, model_dir, true) orelse return error.TestExpectedProjectorGguf;
    defer allocator.free(projector);

    try std.testing.expect(std.mem.endsWith(u8, decoder, "gemma-4-E4B_q4_0-it.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, projector, "gemma-4-E4B-it-mmproj.gguf"));
}

test "manifest does not treat projector-only gguf as decoder weights" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "mmproj.gguf", .data = "projector" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expect(manifest.gguf_path == null);
    try std.testing.expect(manifest.gguf_projector_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_projector_path.?, "mmproj.gguf"));
}

test "manifest does not treat trailing mmproj gguf as decoder weights" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma-4-E4B-it-mmproj.gguf", .data = "projector" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expect(manifest.gguf_path == null);
    try std.testing.expect(manifest.gguf_projector_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_projector_path.?, "gemma-4-E4B-it-mmproj.gguf"));
}

test "listing manifest detects gguf assets without gguf metadata parse" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{
        .sub_path = "model_manifest.json",
        .data =
        \\{"type":"generator","tasks":["generate"],"inputs":["text","image"]}
        ,
    });
    try tmp.dir.writeFile(io, .{ .sub_path = "gemma-4-e2b-it-Q8_0.gguf", .data = "" });
    try tmp.dir.writeFile(io, .{ .sub_path = "mmproj-gemma-4-e2b-it-bf16.gguf", .data = "" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadListingFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(ModelType.generator, manifest.model_type);
    try std.testing.expect(manifest.hasTask("generate"));
    try std.testing.expect(manifest.hasInput("image"));
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(manifest.gguf_projector_path != null);
}

test "listing manifest separates google gemma4 e4b qat decoder and projector" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{
        .sub_path = "model_manifest.json",
        .data =
        \\{"type":"generator","tasks":["generate"],"inputs":["text","image","audio"]}
        ,
    });
    try tmp.dir.writeFile(io, .{ .sub_path = "gemma-4-E4B-it-mmproj.gguf", .data = "" });
    try tmp.dir.writeFile(io, .{ .sub_path = "gemma-4-E4B_q4_0-it.gguf", .data = "" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadListingFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(ModelType.generator, manifest.model_type);
    try std.testing.expect(manifest.hasTask("generate"));
    try std.testing.expect(manifest.hasInput("image"));
    try std.testing.expect(manifest.hasInput("audio"));
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(manifest.gguf_projector_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_path.?, "gemma-4-E4B_q4_0-it.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_projector_path.?, "gemma-4-E4B-it-mmproj.gguf"));
}

test "manifest treats gemma4 unified config as generator" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"gemma4_unified","text_config":{"model_type":"gemma4_unified_text"}}
        ,
    });
    try tmp.dir.writeFile(io, .{ .sub_path = "gemma-4-12B-it-Q4_K_M.gguf", .data = "" });
    try tmp.dir.writeFile(io, .{ .sub_path = "mmproj-gemma-4-12B-it-bf16.gguf", .data = "" });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(ModelType.generator, manifest.model_type);
    try std.testing.expectEqualStrings("gemma4_unified", manifest.config_model_arch);
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(manifest.gguf_projector_path != null);
}

test "gemma4 tokenizer config replaces unsupported upstream chat template" {
    const allocator = std.testing.allocator;
    var manifest = ModelManifest{ .allocator = allocator };
    manifest.chat_template = try allocator.dupe(u8, "{%- macro format_parameters(properties, required) -%}{%- endmacro -%}");
    defer manifest.deinit();

    try parseTokenizerConfig(&manifest, allocator,
        \\{"sot_token":"<|turn>","bos_token":"<bos>","eos_token":"<eos>","pad_token":"<pad>","unk_token":"<unk>"}
    );

    try std.testing.expect(manifest.chat_template != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.chat_template.?, "<|turn>model") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.chat_template.?, "format_parameters") == null);
}

test "built-in gemma4 chat template renders explicit thinking modes" {
    var template = try jinja.Template.init(std.testing.allocator, gemma4_chat_template);
    defer template.deinit();

    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const messages = [_]jinja.ChatMessage{.{ .role = "user", .content = "hello" }};

    var default_context = try jinja.chatTemplateContext(arena, &messages, .{ .bos_token = "<bos>" });
    const default_prompt = try template.render(arena, &default_context);
    try std.testing.expect(std.mem.endsWith(u8, default_prompt, "<|channel>thought\n<channel|>"));

    var enabled_context = try jinja.chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .enable_thinking = true,
    });
    const enabled_prompt = try template.render(arena, &enabled_context);
    try std.testing.expect(std.mem.endsWith(u8, enabled_prompt, "<|channel>thought\n<channel|>"));

    var disabled_context = try jinja.chatTemplateContext(arena, &messages, .{
        .bos_token = "<bos>",
        .enable_thinking = false,
    });
    const disabled_prompt = try template.render(arena, &disabled_context);
    try std.testing.expect(std.mem.endsWith(u8, disabled_prompt, "<|channel>final\n<channel|>"));
}

test "manifest infers huggingface tokenizer from gguf gpt2 metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGpt2Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "ggml-model-i2_s.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expectEqual(TokenizerType.huggingface, manifest.tokenizer_type.?);
    try std.testing.expectEqualStrings("<|begin_of_text|>", manifest.bos_token);
    try std.testing.expectEqualStrings("<|end_of_text|>", manifest.eos_token);
}

test "manifest prefers huggingface tokenizer from gemma4 gguf bpe metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGemma4Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma4-q4_0.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expectEqual(TokenizerType.huggingface, manifest.tokenizer_type.?);
    try std.testing.expectEqualStrings("<bos>", manifest.bos_token);
    try std.testing.expectEqualStrings("<eos>", manifest.eos_token);
}

test "manifest applies BERT and T5 tokenizer metadata from GGUF" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithBertT5Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "bge-m3-q4_k_m.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);

    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(TokenizerType.huggingface, manifest.tokenizer_type.?);
    try std.testing.expectEqual(PoolingStrategy.cls, manifest.pooling);
    try std.testing.expectEqual(@as(u32, 1024), manifest.hidden_size);
    try std.testing.expectEqual(@as(u32, 4096), manifest.intermediate_size);
    try std.testing.expectEqual(@as(u32, 8192), manifest.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 24), manifest.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 16), manifest.num_attention_heads);
    try std.testing.expectEqual(bert.ModelType.roberta, manifest.bert_model_type);
}

test "colocated GGUF does not overwrite selected safetensors BERT config" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_json =
        \\{"model_type":"xlm-roberta","hidden_size":777,"intermediate_size":1554,"max_position_embeddings":8194,"num_hidden_layers":7,"num_attention_heads":7,"vocab_size":250002,"type_vocab_size":1,"pad_token_id":1}
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = config_json });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.safetensors", .data = "" });
    const gguf_bytes = try buildTestGgufWithBertT5Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "quantized.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    var manifest = try loadFromDir(allocator, model_dir);
    defer manifest.deinit();

    try std.testing.expectEqual(NativeWeightArtifactKind.safetensors, manifest.nativeWeightArtifactKind().?);
    try std.testing.expectEqual(@as(u32, 777), manifest.hidden_size);
    try std.testing.expectEqual(@as(u32, 1554), manifest.intermediate_size);
    try std.testing.expectEqual(@as(u32, 8194), manifest.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 7), manifest.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 7), manifest.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 250002), manifest.bert_vocab_size);
    try std.testing.expectEqual(@as(u32, 1), manifest.bert_type_vocab_size);
    try std.testing.expectEqual(bert.ModelType.roberta, manifest.bert_model_type);
    try std.testing.expectEqual(PoolingStrategy.mean, manifest.pooling);
}

fn buildTestGgufWithGpt2Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 8);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bitnet-b1.58");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gpt2");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<|begin_of_text|>",
        "hello",
        "<|end_of_text|>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 1, 3 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn buildTestGgufWithGemma4Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 11);

    try appendTestMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gemma4");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<pad>",
        "<eos>",
        "<bos>",
        "<unk>",
        "hello",
        "▁world",
        "<|turn>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataF32Array(allocator, &data, "tokenizer.ggml.scores", &.{ 0, 0, 0, 0, 0, 0, 0 });
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1, 3 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn buildTestGgufWithBertT5Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 16);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bert");
    try appendTestMetadataU32(allocator, &data, "bert.block_count", 24);
    try appendTestMetadataU32(allocator, &data, "bert.context_length", 8192);
    try appendTestMetadataU32(allocator, &data, "bert.embedding_length", 1024);
    try appendTestMetadataU32(allocator, &data, "bert.feed_forward_length", 4096);
    try appendTestMetadataU32(allocator, &data, "bert.attention.head_count", 16);
    try appendTestMetadataF32(allocator, &data, "bert.attention.layer_norm_epsilon", 1e-5);
    try appendTestMetadataU32(allocator, &data, "bert.pooling_type", 2);
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "t5");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{ "<s>", "<pad>", "</s>", "<unk>", "\u{2581}hello" });
    try appendTestMetadataF32Array(allocator, &data, "tokenizer.ggml.scores", &.{ 0, 0, 0, 0, -1 });
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);

    return data.toOwnedSlice(allocator);
}

fn appendTestLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    const bytes = std.mem.asBytes(&std.mem.nativeToLittle(T, value));
    try data.appendSlice(allocator, bytes);
}

fn appendTestString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendTestLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendTestMetadataString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.string));
    try appendTestString(allocator, data, value);
}

fn appendTestMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.u32));
    try appendTestLe(u32, allocator, data, value);
}

fn appendTestMetadataBool(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.bool_));
    try appendTestLe(u8, allocator, data, @intFromBool(value));
}

fn appendTestMetadataF32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: f32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.f32));
    try appendTestLe(u32, allocator, data, @bitCast(value));
}

fn appendTestMetadataStringArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.string));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestString(allocator, data, value);
}

fn appendTestMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.i32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(i32, allocator, data, value);
}

fn appendTestMetadataF32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const f32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.f32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(u32, allocator, data, @bitCast(value));
}
