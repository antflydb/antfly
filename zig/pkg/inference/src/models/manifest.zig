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
const build_options = @import("build_options");

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
    "{{ '<|turn>model\\n' }}" ++
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

/// SafeTensors file candidates in priority order.
pub const safetensors_candidates = [_][]const u8{
    "model.safetensors",
    "pytorch_model.safetensors",
};

pub const safetensors_index_candidates = [_][]const u8{
    "model.safetensors.index.json",
    "pytorch_model.safetensors.index.json",
};

/// Resolved model configuration loaded from a model directory.
pub const ModelManifest = struct {
    allocator: std.mem.Allocator,

    // Identity
    model_type: ModelType = .embedder,

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
    bert_model_type: bert.ModelType = .bert,
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

    pub fn hasInput(self: *const ModelManifest, input: []const u8) bool {
        for (self.inputs) |candidate| {
            if (std.mem.eql(u8, candidate, input)) return true;
        }
        return false;
    }
};

/// ONNX file candidates in priority order.
const onnx_candidates = [_][]const u8{
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
    "text_model.onnx",
    "text_model_f16.onnx",
    "text_model_i8.onnx",
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

/// Load a model manifest by inspecting the directory contents and parsing configs.
pub fn loadFromDir(allocator: std.mem.Allocator, model_dir_path: []const u8) !ModelManifest {
    var manifest = ModelManifest{ .allocator = allocator };

    if (std.mem.endsWith(u8, model_dir_path, ".gguf")) {
        manifest.gguf_path = try allocator.dupe(u8, model_dir_path);
        applyGgufTokenizerMetadata(&manifest, allocator, std.fs.path.dirname(model_dir_path) orelse ".", model_dir_path) catch {};
        return manifest;
    }

    if (inferModelTypeFromPath(model_dir_path)) |model_type| {
        manifest.model_type = model_type;
    }

    // Try to parse config.json, then clip_config.json for CLIPCLAP-style repos.
    if (c_file.readFileFromDir(allocator, model_dir_path, "config.json")) |config_bytes| {
        defer allocator.free(config_bytes);
        parseConfigJson(&manifest, allocator, config_bytes) catch {};
    } else |_| {}
    if (manifest.native_arch_hint == .none and manifest.max_position_embeddings == 512 and manifest.hidden_size == 768) {
        if (c_file.readFileFromDir(allocator, model_dir_path, "clip_config.json")) |config_bytes| {
            defer allocator.free(config_bytes);
            parseConfigJson(&manifest, allocator, config_bytes) catch {};
        } else |_| {}
    }

    // Try to parse model_manifest.json
    if (c_file.readFileFromDir(allocator, model_dir_path, "model_manifest.json")) |manifest_bytes| {
        defer allocator.free(manifest_bytes);
        parseModelManifestJson(&manifest, allocator, manifest_bytes) catch {};
    } else |_| {}

    if (c_file.readFileFromDir(allocator, model_dir_path, "antfly_inference_bundle.json")) |bundle_bytes| {
        defer allocator.free(bundle_bytes);
        parseInferenceBundleJson(&manifest, allocator, model_dir_path, bundle_bytes) catch {};
    } else |_| {}
    if (shouldParseClipclapGgufVariant(allocator, model_dir_path)) {
        if (c_file.readFileFromDir(allocator, model_dir_path, "antfly_inference_variants.json")) |variants_bytes| {
            defer allocator.free(variants_bytes);
            parseInferenceVariantsJson(&manifest, allocator, model_dir_path, variants_bytes) catch {};
        } else |_| {}
    }

    // Try to parse gliner_config.json (for GLiNER NER models)
    if (c_file.readFileFromDir(allocator, model_dir_path, "gliner_config.json")) |gliner_bytes| {
        defer allocator.free(gliner_bytes);
        parseGlinerConfig(&manifest, allocator, gliner_bytes) catch {};
    } else |_| {}

    // Try to parse added_tokens.json (for GLiNER special token IDs)
    if (c_file.readFileFromDir(allocator, model_dir_path, "added_tokens.json")) |at_bytes| {
        defer allocator.free(at_bytes);
        parseAddedTokens(&manifest, at_bytes) catch {};
    } else |_| {}

    // Auto-detect ONNX files unless a mixed single-repo ClipClap checkout was
    // explicitly resolved to its GGUF pair. The GGUF pair embeds projection
    // weights, so falling through to the default ONNX files would mix variants.
    if (!manifest.isClipclapGgufBundle()) {
        if (manifest.onnx_path == null) manifest.onnx_path = try findFileInSubdirs(allocator, model_dir_path, &onnx_candidates, &onnx_subdirs);
        if (manifest.visual_model_path == null) manifest.visual_model_path = try findFileInSubdirs(allocator, model_dir_path, &visual_model_candidates, &onnx_subdirs);
        if (manifest.audio_model_path == null) manifest.audio_model_path = try findFileInSubdirs(allocator, model_dir_path, &audio_model_candidates, &onnx_subdirs);
        if (manifest.text_projection_path == null) manifest.text_projection_path = try findFileInSubdirs(allocator, model_dir_path, &text_projection_candidates, &onnx_subdirs);
        if (manifest.visual_projection_path == null) manifest.visual_projection_path = try findFileInSubdirs(allocator, model_dir_path, &visual_projection_candidates, &onnx_subdirs);
        if (manifest.audio_projection_path == null) manifest.audio_projection_path = try findFileInSubdirs(allocator, model_dir_path, &audio_projection_candidates, &onnx_subdirs);
    }

    // Auto-detect SafeTensors file
    if (manifest.safetensors_path == null) manifest.safetensors_path = try findFileInSubdirs(allocator, model_dir_path, &safetensors_candidates, &.{""});
    if (manifest.safetensors_index_path == null) manifest.safetensors_index_path = try findFileInSubdirs(allocator, model_dir_path, &safetensors_index_candidates, &.{""});
    if (manifest.gliner_head_gguf_path == null) manifest.gliner_head_gguf_path = try findFileInSubdirs(allocator, model_dir_path, &.{"gliner_head.gguf"}, &.{""});
    if (manifest.gliner_head_safetensors_path == null) manifest.gliner_head_safetensors_path = try findFileInSubdirs(allocator, model_dir_path, &.{"gliner_head.safetensors"}, &.{""});
    if (manifest.config_path == null) manifest.config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"config.json"}, &.{""});
    if (manifest.model_manifest_path == null) manifest.model_manifest_path = try findFileInSubdirs(allocator, model_dir_path, &.{"model_manifest.json"}, &.{""});
    if (manifest.tokenizer_json_path == null) manifest.tokenizer_json_path = try findFileInSubdirs(allocator, model_dir_path, &.{"tokenizer.json"}, &.{""});
    if (manifest.tokenizer_config_path == null) manifest.tokenizer_config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"tokenizer_config.json"}, &.{""});
    if (manifest.special_tokens_map_path == null) manifest.special_tokens_map_path = try findFileInSubdirs(allocator, model_dir_path, &.{"special_tokens_map.json"}, &.{""});
    if (manifest.preprocessor_config_path == null) manifest.preprocessor_config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"preprocessor_config.json"}, &.{""});
    if (manifest.processor_config_path == null) manifest.processor_config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"processor_config.json"}, &.{""});

    // Auto-detect GGUF files. External multimodal projectors are GGUFs too,
    // but they are not decoder weights and must not be opened as the main model.
    if (manifest.gguf_path == null) manifest.gguf_path = try findFirstGgufInDir(allocator, model_dir_path, false);
    if (manifest.gguf_projector_path == null) manifest.gguf_projector_path = try findFirstGgufInDir(allocator, model_dir_path, true);

    // Auto-detect tokenizer
    if (c_file.fileExistsInDir(allocator, model_dir_path, "tokenizer.json") or
        c_file.fileExistsInDir(allocator, model_dir_path, "vocab.txt") or
        c_file.fileExistsInDir(allocator, model_dir_path, "vocab.json"))
    {
        manifest.tokenizer_type = .huggingface;
    } else if (c_file.fileExistsInDir(allocator, model_dir_path, "tokenizer.model")) {
        manifest.tokenizer_type = .sentencepiece;
    }

    // Load chat template (from chat_template.jinja file)
    if (c_file.readFileFromDir(allocator, model_dir_path, "chat_template.jinja")) |ct| {
        if (std.mem.trim(u8, ct, &.{ ' ', '\t', '\n', '\r' }).len > 0) {
            manifest.chat_template = ct;
        } else {
            allocator.free(ct);
        }
    } else |_| {}

    // Load special tokens from tokenizer_config.json
    if (c_file.readFileFromDir(allocator, model_dir_path, "tokenizer.json")) |tok_bytes| {
        defer allocator.free(tok_bytes);
        parseTokenizerJsonSpecialTokens(&manifest, allocator, tok_bytes) catch {};
    } else |_| {}
    if (c_file.readFileFromDir(allocator, model_dir_path, "tokenizer_config.json")) |tc_bytes| {
        defer allocator.free(tc_bytes);
        parseTokenizerConfig(&manifest, allocator, tc_bytes) catch {};
    } else |_| {}

    if (manifest.gguf_path) |gguf_path| {
        applyGgufTokenizerMetadata(&manifest, allocator, model_dir_path, gguf_path) catch {};
    }

    applyImplicitSparseOutputLayout(&manifest, model_dir_path);
    applyImplicitModelTypeHints(&manifest, model_dir_path);

    return manifest;
}

/// Load only the metadata needed to list a model in server discovery results.
///
/// This intentionally avoids tokenizer parsing and GGUF metadata inspection. It
/// still records enough artifact paths to hide obviously unloadable bundles and
/// to expose text/image/audio listing metadata.
pub fn loadListingFromDir(allocator: std.mem.Allocator, model_dir_path: []const u8) !ModelManifest {
    var manifest = ModelManifest{ .allocator = allocator };

    if (std.mem.endsWith(u8, model_dir_path, ".gguf")) {
        manifest.gguf_path = try allocator.dupe(u8, model_dir_path);
        return manifest;
    }

    if (inferModelTypeFromPath(model_dir_path)) |model_type| {
        manifest.model_type = model_type;
    }

    if (c_file.readFileFromDir(allocator, model_dir_path, "config.json")) |config_bytes| {
        defer allocator.free(config_bytes);
        parseListingConfigJson(&manifest, allocator, config_bytes) catch {};
    } else |_| {}
    if (manifest.native_arch_hint == .none and manifest.config_model_arch.len == 0) {
        if (c_file.readFileFromDir(allocator, model_dir_path, "clip_config.json")) |config_bytes| {
            defer allocator.free(config_bytes);
            parseListingConfigJson(&manifest, allocator, config_bytes) catch {};
        } else |_| {}
    }

    if (c_file.readFileFromDir(allocator, model_dir_path, "model_manifest.json")) |manifest_bytes| {
        defer allocator.free(manifest_bytes);
        parseModelManifestJson(&manifest, allocator, manifest_bytes) catch {};
    } else |_| {}

    if (c_file.readFileFromDir(allocator, model_dir_path, "antfly_inference_bundle.json")) |bundle_bytes| {
        defer allocator.free(bundle_bytes);
        parseInferenceBundleJson(&manifest, allocator, model_dir_path, bundle_bytes) catch {};
    } else |_| {}
    if (c_file.readFileFromDir(allocator, model_dir_path, "antfly_inference_variants.json")) |variants_bytes| {
        defer allocator.free(variants_bytes);
        parseInferenceVariantsJson(&manifest, allocator, model_dir_path, variants_bytes) catch {};
    } else |_| {}

    if (c_file.readFileFromDir(allocator, model_dir_path, "gliner_config.json")) |gliner_bytes| {
        defer allocator.free(gliner_bytes);
        parseGlinerConfig(&manifest, allocator, gliner_bytes) catch {};
    } else |_| {}
    if (c_file.readFileFromDir(allocator, model_dir_path, "added_tokens.json")) |at_bytes| {
        defer allocator.free(at_bytes);
        parseAddedTokens(&manifest, at_bytes) catch {};
    } else |_| {}
    applyListingGlinerHint(&manifest, allocator, model_dir_path);

    if (!manifest.isClipclapGgufBundle()) {
        if (manifest.onnx_path == null) manifest.onnx_path = try findFileInSubdirs(allocator, model_dir_path, &onnx_candidates, &onnx_subdirs);
        if (manifest.visual_model_path == null) manifest.visual_model_path = try findFileInSubdirs(allocator, model_dir_path, &visual_model_candidates, &onnx_subdirs);
        if (manifest.audio_model_path == null) manifest.audio_model_path = try findFileInSubdirs(allocator, model_dir_path, &audio_model_candidates, &onnx_subdirs);
        if (manifest.text_projection_path == null) manifest.text_projection_path = try findFileInSubdirs(allocator, model_dir_path, &text_projection_candidates, &onnx_subdirs);
        if (manifest.visual_projection_path == null) manifest.visual_projection_path = try findFileInSubdirs(allocator, model_dir_path, &visual_projection_candidates, &onnx_subdirs);
        if (manifest.audio_projection_path == null) manifest.audio_projection_path = try findFileInSubdirs(allocator, model_dir_path, &audio_projection_candidates, &onnx_subdirs);
    }

    if (manifest.safetensors_path == null) manifest.safetensors_path = try findFileInSubdirs(allocator, model_dir_path, &safetensors_candidates, &.{""});
    if (manifest.safetensors_index_path == null) manifest.safetensors_index_path = try findFileInSubdirs(allocator, model_dir_path, &safetensors_index_candidates, &.{""});
    if (manifest.gliner_head_gguf_path == null) manifest.gliner_head_gguf_path = try findFileInSubdirs(allocator, model_dir_path, &.{"gliner_head.gguf"}, &.{""});
    if (manifest.gliner_head_safetensors_path == null) manifest.gliner_head_safetensors_path = try findFileInSubdirs(allocator, model_dir_path, &.{"gliner_head.safetensors"}, &.{""});
    if (manifest.config_path == null) manifest.config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"config.json"}, &.{""});
    if (manifest.model_manifest_path == null) manifest.model_manifest_path = try findFileInSubdirs(allocator, model_dir_path, &.{"model_manifest.json"}, &.{""});
    if (manifest.tokenizer_json_path == null) manifest.tokenizer_json_path = try findFileInSubdirs(allocator, model_dir_path, &.{"tokenizer.json"}, &.{""});
    if (manifest.tokenizer_config_path == null) manifest.tokenizer_config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"tokenizer_config.json"}, &.{""});
    if (manifest.preprocessor_config_path == null) manifest.preprocessor_config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"preprocessor_config.json"}, &.{""});
    if (manifest.processor_config_path == null) manifest.processor_config_path = try findFileInSubdirs(allocator, model_dir_path, &.{"processor_config.json"}, &.{""});
    if (manifest.gguf_path == null) manifest.gguf_path = try findFirstGgufInDir(allocator, model_dir_path, false);
    if (manifest.gguf_projector_path == null) manifest.gguf_projector_path = try findFirstGgufInDir(allocator, model_dir_path, true);

    applyImplicitSparseOutputLayout(&manifest, model_dir_path);
    applyImplicitModelTypeHints(&manifest, model_dir_path);

    return manifest;
}

fn applyListingGlinerHint(manifest: *ModelManifest, allocator: std.mem.Allocator, model_dir_path: []const u8) void {
    if (manifest.gliner_model_type.len > 0) return;
    if (!std.mem.eql(u8, manifest.config_model_arch, "extractor") and !hasGlinerPathHint(model_dir_path)) return;

    if (c_file.readFileFromDir(allocator, model_dir_path, "special_tokens_map.json")) |tokens_bytes| {
        defer allocator.free(tokens_bytes);
        if (!listingSpecialTokensMapHasGlinerMarkers(tokens_bytes)) return;
    } else |_| {
        return;
    }

    manifest.gliner_model_type = allocator.dupe(u8, "gliner2") catch "";
}

fn listingSpecialTokensMapHasGlinerMarkers(json_bytes: []const u8) bool {
    return std.mem.indexOf(u8, json_bytes, "\"[P]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[C]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[E]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[R]\"") != null and
        std.mem.indexOf(u8, json_bytes, "\"[SEP_TEXT]\"") != null;
}

fn applyImplicitSparseOutputLayout(manifest: *ModelManifest, model_dir_path: []const u8) void {
    if (manifest.sparse_3d_output_layout != null) return;
    if (c_file.fileExistsInDir(manifest.allocator, model_dir_path, "1_SpladePooling/config.json")) {
        manifest.sparse_3d_output_layout = .batch_seq;
    }
}

fn applyImplicitModelTypeHints(manifest: *ModelManifest, model_dir_path: []const u8) void {
    if (inferGlinerModelType(manifest, model_dir_path)) |gliner_type| {
        if (manifest.gliner_model_type.len > 0 and !std.mem.eql(u8, manifest.gliner_model_type, gliner_type)) {
            manifest.allocator.free(manifest.gliner_model_type);
            manifest.gliner_model_type = "";
        }
        if (manifest.gliner_model_type.len == 0) {
            manifest.gliner_model_type = manifest.allocator.dupe(u8, gliner_type) catch "";
        }
    }

    if (inferModelTypeFromTasks(manifest.tasks)) |task_model_type| {
        manifest.model_type = task_model_type;
        return;
    }

    if (manifest.gliner_model_type.len > 0) {
        manifest.model_type = .recognizer;
        return;
    }

    if (manifest.model_type != .embedder) return;

    if (manifest.native_arch_hint == .whisper) {
        manifest.model_type = .transcriber;
        return;
    }
    if (manifest.native_arch_hint == .florence or
        std.mem.eql(u8, manifest.config_model_arch, "vision-encoder-decoder"))
    {
        manifest.model_type = .reader;
        return;
    }
    if (manifest.config_model_arch.len > 0 and gpt.isGenerativeModel(manifest.config_model_arch)) {
        manifest.model_type = .generator;
    }
}

fn inferModelTypeFromTasks(tasks: []const []const u8) ?ModelType {
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "recognize") or std.mem.eql(u8, task, "extract")) return .recognizer;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "classify")) return .classifier;
    }
    for (tasks) |task| {
        if (std.mem.eql(u8, task, "rerank")) return .reranker;
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
        if (std.mem.eql(u8, component, "recognizers")) return .recognizer;
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
    model_dir_path: []const u8,
    gguf_path: []const u8,
) !void {
    var region = try c_file.MmapRegion.init(allocator, gguf_path);
    defer region.deinit();

    var parsed = try gguf_format.parse(allocator, region.data);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);

    const gguf_model_name = view.getString("tokenizer.ggml.model");
    if (gguf_model_name) |model_name| {
        if (c_file.fileExistsInDir(allocator, model_dir_path, "tokenizer.model")) {
            manifest.tokenizer_type = .sentencepiece;
        } else if (c_file.fileExistsInDir(allocator, model_dir_path, "tokenizer.json")) {
            manifest.tokenizer_type = .huggingface;
        } else if (supportsGgufSentencePieceFallback(model_name) and hasGgufSentencePieceMetadata(&parsed)) {
            manifest.tokenizer_type = .sentencepiece;
        } else if (supportsGgufHuggingFaceFallback(model_name) and hasGgufHuggingFaceMetadata(&parsed)) {
            manifest.tokenizer_type = .huggingface;
        } else {
            manifest.tokenizer_type = null;
        }
    } else if (c_file.fileExistsInDir(allocator, model_dir_path, "tokenizer.model")) {
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

fn shouldUseBuiltInGemma4GgufChatTemplate(model_name: []const u8, chat_template: []const u8) bool {
    if (!std.mem.eql(u8, model_name, "gemma4")) return false;

    return std.mem.indexOf(u8, chat_template, "macro format_parameters") != null or
        std.mem.indexOf(u8, chat_template, "namespace(") != null or
        std.mem.indexOf(u8, chat_template, "{% set captured_content") != null or
        std.mem.indexOf(u8, chat_template, "{%- set captured_content") != null;
}

fn supportsGgufSentencePieceFallback(model_name: []const u8) bool {
    return std.mem.eql(u8, model_name, "llama") or std.mem.startsWith(u8, model_name, "gemma");
}

fn supportsGgufHuggingFaceFallback(model_name: []const u8) bool {
    return std.mem.eql(u8, model_name, "gpt2");
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
    const merges = findMetadataEntry(parsed, "tokenizer.ggml.merges") orelse return false;

    return tokens.value == .array and
        merges.value == .array and
        tokens.value.array.element_type == .string and
        merges.value.array.element_type == .string;
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
    base_dir: []const u8,
    candidates: []const []const u8,
    subdirs: []const []const u8,
) !?[]const u8 {
    for (subdirs) |subdir| {
        for (candidates) |candidate| {
            const path = if (subdir.len > 0)
                try std.fmt.allocPrint(allocator, "{s}/{s}/{s}", .{ base_dir, subdir, candidate })
            else
                try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base_dir, candidate });

            if (c_file.fileExists(allocator, path)) {
                return path;
            }
            allocator.free(path);
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

    const base_dir_z = try allocator.dupeZ(u8, base_dir);
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
    return std.mem.eql(u8, name, "mmproj.gguf") or
        std.mem.startsWith(u8, name, "mmproj-") or
        std.mem.startsWith(u8, name, "mmproj_");
}

fn isGlinerHeadGgufFileName(name: []const u8) bool {
    return std.mem.eql(u8, name, "gliner_head.gguf");
}

fn findFirstGgufInDir(allocator: std.mem.Allocator, base_dir: []const u8, want_projector: bool) !?[]const u8 {
    if (!c_file.link_libc) {
        var dir = Dir.cwd().openDir(std.Options.debug_io, base_dir, .{ .iterate = true }) catch return null;
        defer dir.close(std.Options.debug_io);
        var iter = dir.iterate();
        while (iter.next(std.Options.debug_io) catch null) |entry| {
            const name = entry.name;
            if (name.len == 0 or name[0] == '.') continue;
            if (!std.mem.endsWith(u8, name, ".gguf")) continue;
            if (isGlinerHeadGgufFileName(name)) continue;
            if (isGgufProjectorFileName(name) != want_projector) continue;
            return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base_dir, name });
        }
        return null;
    }

    const base_dir_z = try allocator.dupeZ(u8, base_dir);
    defer allocator.free(base_dir_z);

    const dir = c_file.c.opendir(base_dir_z.ptr);
    if (dir == null) return null;
    defer _ = c_file.c.closedir(dir);

    while (c_file.c.readdir(dir)) |entry| {
        const name_z: [*:0]const u8 = @ptrCast(&entry.*.d_name);
        const name = std.mem.span(name_z);
        if (name.len == 0 or name[0] == '.') continue;
        if (!std.mem.endsWith(u8, name, ".gguf")) continue;
        if (isGlinerHeadGgufFileName(name)) continue;
        if (isGgufProjectorFileName(name) != want_projector) continue;
        return try std.fmt.allocPrint(allocator, "{s}/{s}", .{ base_dir, name });
    }
    return null;
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
    if (obj.get("num_hidden_layers")) |v| {
        if (jsonU32(v)) |val| manifest.num_hidden_layers = val;
    }
    if (obj.get("num_attention_heads")) |v| {
        if (jsonU32(v)) |val| manifest.num_attention_heads = val;
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
                    break;
                }
            }
        }
    }

    if (obj.get("model_type")) |v| {
        if (v == .string) {
            const s = v.string;
            if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
            manifest.config_model_arch = allocator.dupe(u8, s) catch "";
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
                    break;
                }
            }
        }
    }

    if (obj.get("model_type")) |v| {
        if (v == .string) {
            const s = v.string;
            if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
            manifest.config_model_arch = allocator.dupe(u8, s) catch "";
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
                }
            }
        }
    }

    if (obj.get("tasks")) |v| {
        if (v == .array) {
            var tasks = std.ArrayListUnmanaged([]const u8).empty;
            for (v.array.items) |item| {
                if (item == .string) {
                    const task = allocator.dupe(u8, item.string) catch continue;
                    tasks.append(allocator, task) catch {
                        allocator.free(task);
                        continue;
                    };
                }
            }
            if (tasks.items.len > 0) {
                manifest.tasks = tasks.toOwnedSlice(allocator) catch &.{};
            }
        }
    }

    // Parse capabilities array
    if (obj.get("capabilities")) |v| {
        if (v == .array) {
            var caps = std.ArrayListUnmanaged([]const u8).empty;
            for (v.array.items) |item| {
                if (item == .string) {
                    const cap = allocator.dupe(u8, item.string) catch continue;
                    caps.append(allocator, cap) catch {
                        allocator.free(cap);
                        continue;
                    };
                }
            }
            if (caps.items.len > 0) {
                manifest.capabilities = caps.toOwnedSlice(allocator) catch &.{};
            }
        }
    }

    if (obj.get("inputs")) |v| {
        if (v == .array) {
            var inputs = std.ArrayListUnmanaged([]const u8).empty;
            for (v.array.items) |item| {
                if (item == .string) {
                    const input = allocator.dupe(u8, item.string) catch continue;
                    inputs.append(allocator, input) catch {
                        allocator.free(input);
                        continue;
                    };
                }
            }
            if (inputs.items.len > 0) {
                manifest.inputs = inputs.toOwnedSlice(allocator) catch &.{};
            }
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
            if (manifest.gliner_model_type.len > 0) allocator.free(manifest.gliner_model_type);
            manifest.gliner_model_type = allocator.dupe(u8, v.string) catch "";
        }
    }
    if (obj.get("default_labels")) |v| {
        if (v == .array) {
            var labels = std.ArrayListUnmanaged([]const u8).empty;
            for (v.array.items) |item| {
                if (item == .string) {
                    const lbl = allocator.dupe(u8, item.string) catch continue;
                    labels.append(allocator, lbl) catch {
                        allocator.free(lbl);
                        continue;
                    };
                }
            }
            if (labels.items.len > 0) {
                manifest.gliner_default_labels = labels.toOwnedSlice(allocator) catch &.{};
            }
        }
    }
    if (obj.get("relation_labels")) |v| {
        if (v == .array) {
            var labels = std.ArrayListUnmanaged([]const u8).empty;
            for (v.array.items) |item| {
                if (item == .string) {
                    const lbl = allocator.dupe(u8, item.string) catch continue;
                    labels.append(allocator, lbl) catch {
                        allocator.free(lbl);
                        continue;
                    };
                }
            }
            if (labels.items.len > 0) {
                manifest.gliner_relation_labels = labels.toOwnedSlice(allocator) catch &.{};
            }
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
                            if (labels_v == .array) {
                                var labels = std.ArrayListUnmanaged([]const u8).empty;
                                for (labels_v.array.items) |item| {
                                    if (item == .string) {
                                        const lbl = allocator.dupe(u8, item.string) catch continue;
                                        labels.append(allocator, lbl) catch {
                                            allocator.free(lbl);
                                            continue;
                                        };
                                    }
                                }
                                if (labels.items.len > 0) {
                                    manifest.gliner_relation_labels = labels.toOwnedSlice(allocator) catch &.{};
                                }
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
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    var family: ?[]const u8 = null;
    if (obj.get("family")) |v| {
        if (v == .string and v.string.len > 0) {
            if (manifest.inference_bundle_family.len > 0) allocator.free(manifest.inference_bundle_family);
            manifest.inference_bundle_family = allocator.dupe(u8, v.string) catch "";
            family = v.string;
        }
    }
    if (obj.get("wrapper")) |v| {
        if (v == .string and v.string.len > 0 and family != null and std.mem.eql(u8, family.?, "gliner2_split_bundle/v1")) {
            if (manifest.gliner_model_type.len > 0) allocator.free(manifest.gliner_model_type);
            manifest.gliner_model_type = allocator.dupe(u8, v.string) catch "";
        }
    }
    if (family) |bundle_family| {
        if (std.mem.eql(u8, bundle_family, "clipclap_gguf_bundle/v1")) {
            if (obj.get("clip")) |clip| {
                if (clip == .string and clip.string.len > 0) {
                    setOptionalPath(allocator, &manifest.gguf_path, try resolveBundlePath(allocator, model_dir_path, clip.string));
                }
            }
            if (obj.get("clap")) |clap| {
                if (clap == .string and clap.string.len > 0) {
                    setOptionalPath(allocator, &manifest.audio_model_path, try resolveBundlePath(allocator, model_dir_path, clap.string));
                }
            }
            manifest.native_arch_hint = .clip;
            if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
            manifest.config_model_arch = allocator.dupe(u8, "clipclap") catch "";
        }
    }
}

fn completeClipclapDefaultOnnxPresent(allocator: std.mem.Allocator, model_dir_path: []const u8) bool {
    const required = [_][]const u8{
        "text_model.onnx",
        "visual_model.onnx",
        "audio_model.onnx",
        "text_projection.onnx",
        "visual_projection.onnx",
        "audio_projection.onnx",
    };
    for (&required) |name| {
        if (!c_file.fileExistsInDir(allocator, model_dir_path, name)) return false;
    }
    return true;
}

fn shouldUseClipclapGgufVariant(allocator: std.mem.Allocator, model_dir_path: []const u8) bool {
    return !completeClipclapDefaultOnnxPresent(allocator, model_dir_path);
}

fn shouldParseClipclapGgufVariant(allocator: std.mem.Allocator, model_dir_path: []const u8) bool {
    if (shouldUseClipclapGgufVariant(allocator, model_dir_path)) return true;
    if (build_options.enable_cuda and !build_options.enable_onnx) {
        return c_file.fileExistsInDir(allocator, model_dir_path, "antfly_inference_variants.json");
    }
    return false;
}

fn parseInferenceVariantsJson(manifest: *ModelManifest, allocator: std.mem.Allocator, model_dir_path: []const u8, json_bytes: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    const variants_family = obj.get("family") orelse return;
    if (variants_family != .string or !std.mem.eql(u8, variants_family.string, "clipclap_variants/v1")) return;
    const variants = obj.get("variants") orelse return;
    if (variants != .array) return;

    var selected: ?ResolvedClipclapGgufPair = null;
    errdefer if (selected) |*pair| pair.deinit(allocator);
    for (variants.array.items) |variant| {
        if (!isClipclapGgufVariant(variant)) continue;
        var pair = (try resolveExistingClipclapGgufVariant(allocator, model_dir_path, variant)) orelse continue;
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
    if (manifest.config_model_arch.len > 0) allocator.free(manifest.config_model_arch);
    manifest.config_model_arch = arch;
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

fn resolveExistingClipclapGgufVariant(
    allocator: std.mem.Allocator,
    model_dir_path: []const u8,
    variant: std.json.Value,
) !?ResolvedClipclapGgufPair {
    const clip = variant.object.get("clip") orelse return null;
    const clap = variant.object.get("clap") orelse return null;
    if (clip != .string or clip.string.len == 0) return null;
    if (clap != .string or clap.string.len == 0) return null;

    const clip_path = try resolveBundlePath(allocator, model_dir_path, clip.string);
    errdefer allocator.free(clip_path);
    const clap_path = try resolveBundlePath(allocator, model_dir_path, clap.string);
    errdefer allocator.free(clap_path);
    if (!c_file.fileExists(allocator, clip_path) or !c_file.fileExists(allocator, clap_path)) {
        allocator.free(clip_path);
        allocator.free(clap_path);
        return null;
    }
    return .{ .clip_path = clip_path, .clap_path = clap_path };
}

fn isClipclapGgufVariant(variant: std.json.Value) bool {
    if (variant != .object) return false;
    const target = variant.object.get("target") orelse return false;
    if (target != .string or !std.mem.eql(u8, target.string, "gguf")) return false;
    const clip = variant.object.get("clip") orelse return false;
    const clap = variant.object.get("clap") orelse return false;
    return clip == .string and clip.string.len > 0 and clap == .string and clap.string.len > 0;
}

fn resolveBundlePath(allocator: std.mem.Allocator, model_dir_path: []const u8, path: []const u8) ![]const u8 {
    if (std.fs.path.isAbsolute(path)) return allocator.dupe(u8, path);
    return std.fs.path.join(allocator, &.{ model_dir_path, path });
}

fn setOptionalPath(allocator: std.mem.Allocator, slot: *?[]const u8, value: []const u8) void {
    if (slot.*) |old| allocator.free(old);
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

test "inferModelTypeFromPath detects classifier directory" {
    try std.testing.expectEqual(@as(?ModelType, .classifier), inferModelTypeFromPath("/tmp/models/classifiers/cross-encoder/nli-distilroberta-base"));
}

test "inferModelTypeFromPath detects recognizer directory" {
    try std.testing.expectEqual(@as(?ModelType, .recognizer), inferModelTypeFromPath("C:\\models\\recognizers\\fastino\\gliner2-base-v1"));
}

test "parseModelManifestJson parses inputs array" {
    var manifest = ModelManifest{ .allocator = std.testing.allocator };
    defer manifest.deinit();

    try parseModelManifestJson(&manifest, std.testing.allocator,
        \\{"type":"recognizer","tasks":["recognize","extract"],"capabilities":["extraction"],"inputs":["text","image"],"sparse_3d_output_layout":"seq_batch"}
    );

    try std.testing.expect(manifest.hasTask("recognize"));
    try std.testing.expect(manifest.hasTask("extract"));
    try std.testing.expect(manifest.hasCapability("extraction"));
    try std.testing.expect(manifest.hasInput("text"));
    try std.testing.expect(manifest.hasInput("image"));
    try std.testing.expectEqual(Sparse3DOutputLayout.seq_batch, manifest.sparse_3d_output_layout.?);
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

    // Gemma 4 models may lack a chat_template field — detect by sot_token and apply built-in.
    if (manifest.chat_template == null) {
        if (obj.get("sot_token")) |v| {
            if (v == .string and std.mem.eql(u8, v.string, "<|turn>")) {
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
        \\{"model_type": "bert", "hidden_size": 384, "max_position_embeddings": 256, "num_hidden_layers": 6}
    ;
    try parseConfigJson(&manifest, allocator, config_json);

    try std.testing.expectEqual(@as(u32, 384), manifest.hidden_size);
    try std.testing.expectEqual(@as(u32, 256), manifest.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 6), manifest.num_hidden_layers);
    try std.testing.expectEqual(bert.ModelType.bert, manifest.bert_model_type);
    try std.testing.expectEqualStrings("bert", manifest.config_model_arch);
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
    var manifest = ModelManifest{ .allocator = allocator };
    defer manifest.deinit();

    try parseInferenceBundleJson(&manifest, allocator, "/tmp/clipclap-q4_k",
        \\{"family":"clipclap_gguf_bundle/v1","clip":"clip.gguf","clap":"clap.gguf","inputs":["text","image","audio"],"projections_embedded":true}
    );

    try std.testing.expect(manifest.isClipclapGgufBundle());
    try std.testing.expectEqual(NativeArchHint.clip, manifest.native_arch_hint);
    try std.testing.expectEqualStrings("clipclap", manifest.config_model_arch);
    try std.testing.expect(manifest.gguf_path != null);
    try std.testing.expect(manifest.audio_model_path != null);
    try std.testing.expect(std.mem.endsWith(u8, manifest.gguf_path.?, "/tmp/clipclap-q4_k/clip.gguf"));
    try std.testing.expect(std.mem.endsWith(u8, manifest.audio_model_path.?, "/tmp/clipclap-q4_k/clap.gguf"));
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
    try std.testing.expectEqualStrings(clip_path, manifest.gguf_path.?);
    try std.testing.expectEqualStrings(clap_path, manifest.audio_model_path.?);
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
    try std.testing.expectEqualStrings(clip_path, manifest.gguf_path.?);
    try std.testing.expectEqualStrings(clap_path, manifest.audio_model_path.?);
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

    try std.testing.expect(shouldUseClipclapGgufVariant(allocator, dir_path));
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

    try std.testing.expect(!shouldUseClipclapGgufVariant(allocator, dir_path));
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
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.string));
    try appendTestString(allocator, data, value);
}

fn appendTestMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.u32));
    try appendTestLe(u32, allocator, data, value);
}

fn appendTestMetadataBool(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.bool_));
    try appendTestLe(u8, allocator, data, @intFromBool(value));
}

fn appendTestMetadataStringArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.string));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestString(allocator, data, value);
}

fn appendTestMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @intFromEnum(gguf_format.MetadataValueType.i32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(i32, allocator, data, value);
}
