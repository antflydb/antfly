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

//! Native Moondream2 question answering over one image: preprocessing,
//! tokenizer, prompt assembly, vision encoding, and greedy or sampled decoding
//! with the paged KV cache. Mirrors `Moondream.answer_question` from the
//! upstream 2024-03-04 checkpoint:
//!
//!   [BOS] tok("<image>") [image embeddings] tok("</image>\n\nQuestion: {q}\n\nAnswer: ")
//!
//! and stops on any token of `tok("<END>")` (upstream passes them all as EOS)
//! or `<|endoftext|>`.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const arch = @import("../architectures/moondream.zig");
const generation = @import("generation.zig");
const image_mod = @import("image.zig");
const c_file = @import("../util/c_file.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const ops = @import("../ops/ops.zig");
const native_mod = if (build_options.enable_native) @import("../ops/native_compute.zig") else struct {};
const execution_control_mod = @import("../execution_control.zig");
const InferenceExecutionControl = execution_control_mod.InferenceExecutionControl;

pub const Config = arch.Config;

pub const weights_file = "model.safetensors";
pub const default_question = "Describe this image.";
pub const stop_text = "<END>";

/// Cheap capability probe: a `moondream1` config.json, a tokenizer, and the
/// single-file safetensors checkpoint. Loads no weights.
pub fn probeModelDir(allocator: std.mem.Allocator, model_dir: []const u8) !bool {
    if (!build_options.enable_native) return false;
    if (!try c_file.fileExistsInDirChecked(allocator, model_dir, "config.json")) return false;
    if (!try c_file.fileExistsInDirChecked(allocator, model_dir, "tokenizer.json")) return false;
    if (!try c_file.fileExistsInDirChecked(allocator, model_dir, weights_file)) return false;
    const cfg_bytes = c_file.readFileFromDir(allocator, model_dir, "config.json") catch return false;
    defer allocator.free(cfg_bytes);
    return arch.isMoondreamConfig(allocator, cfg_bytes);
}

pub fn isSupportedModelDir(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    return probeModelDir(allocator, model_dir) catch false;
}

/// Format the upstream `answer_question` text halves around the image.
pub const PromptText = struct {
    before_image: []const u8 = "<image>",
    after_image: []u8,

    pub fn init(allocator: std.mem.Allocator, question: ?[]const u8) !PromptText {
        const q = blk: {
            const raw = question orelse break :blk default_question;
            const trimmed = std.mem.trim(u8, raw, " \t\r\n");
            break :blk if (trimmed.len == 0) default_question else trimmed;
        };
        return .{ .after_image = try std.fmt.allocPrint(allocator, "</image>\n\nQuestion: {s}\n\nAnswer: ", .{q}) };
    }

    pub fn deinit(self: *PromptText, allocator: std.mem.Allocator) void {
        allocator.free(self.after_image);
    }
};

/// Token ids for the text around the image, tokenized as upstream does
/// (two separate calls, no special tokens).
pub const PromptTokens = struct {
    before: []i32,
    after: []i32,

    pub fn deinit(self: *PromptTokens, allocator: std.mem.Allocator) void {
        allocator.free(self.before);
        allocator.free(self.after);
    }

    pub fn textCount(self: PromptTokens) usize {
        return 1 + self.before.len + self.after.len;
    }
};

pub const Pipeline = struct {
    allocator: std.mem.Allocator,
    config: Config,
    tokenizer: *hf_tokenizer.HfTokenizer,
    compute: if (build_options.enable_native) *native_mod.NativeCompute else void,
    weights: if (build_options.enable_native) *native_mod.WeightStore else void,
    /// Borrowed f16/bf16/f32 embedding table inside the mapped checkpoint.
    embedding: tensor_view,
    stop_token_ids: []i32,
    /// Request control for `generate`; `answer` takes it explicitly.
    execution_control: ?InferenceExecutionControl = null,

    const tensor_view = struct {
        data: []const u8,
        dtype: @import("../backends/tensor.zig").DType,
        rows: usize,
        cols: usize,
    };

    pub fn load(allocator: std.mem.Allocator, model_dir: []const u8) !Pipeline {
        return loadWithControl(allocator, model_dir, null, null);
    }

    /// `io` schedules the GEMM worker pool; without it macOS runs serially.
    pub fn loadWithControl(
        allocator: std.mem.Allocator,
        model_dir: []const u8,
        control: ?InferenceExecutionControl,
        io: ?std.Io,
    ) !Pipeline {
        if (!build_options.enable_native) return error.NativeNotEnabled;
        if (control) |active| try active.check();

        const cfg_bytes = try c_file.readFileFromDir(allocator, model_dir, "config.json");
        defer allocator.free(cfg_bytes);
        const config = try arch.parseConfig(allocator, cfg_bytes);

        const tok_bytes = try c_file.readFileFromDir(allocator, model_dir, "tokenizer.json");
        defer allocator.free(tok_bytes);
        const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
        errdefer tok.deinitSelf();

        const stop_token_ids = try loadStopTokens(allocator, tok, config);
        errdefer allocator.free(stop_token_ids);

        const weights_path = try std.fs.path.join(allocator, &.{ model_dir, weights_file });
        defer allocator.free(weights_path);
        const st = try tensor_store_mod.SafetensorsStore.initAbsolute(allocator, weights_path);
        const store = st.tensorStore();
        var store_owned = true;
        errdefer if (store_owned) store.deinit();
        if (control) |active| try active.check();

        try validateCheckpoint(allocator, store, config);

        const weights = try allocator.create(native_mod.WeightStore);
        errdefer allocator.destroy(weights);
        weights.* = .{
            .allocator = allocator,
            .resident_weights = .empty,
            .lazy_weights = .empty,
        };
        var weights_owned = false;
        errdefer if (weights_owned) weights.deinitOwned() else {
            var it = weights.resident_weights.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.deinit();
                allocator.free(entry.key_ptr.*);
            }
            weights.resident_weights.deinit(allocator);
        };

        const source = (try store.weightSource()) orelse return error.NoDenseWeightSource;
        const all_names = try source.listNames(allocator);
        defer allocator.free(all_names);
        for (all_names) |name| {
            var ref = try store.describeTensor(allocator, name);
            defer ref.deinit(allocator);
            var loaded = try store.loadTensorRef(&ref);
            errdefer loaded.deinit();
            const key = try allocator.dupe(u8, name);
            errdefer allocator.free(key);
            try weights.resident_weights.put(allocator, key, loaded);
        }
        weights.tensor_store = store;
        store_owned = false;
        weights_owned = true;

        const embedding = blk: {
            const w = weights.resident_weights.getPtr(arch.names.token_embedding) orelse return error.MissingWeight;
            const t = &w.tensor;
            if (t.shape.len != 2) return error.InvalidEmbeddingShape;
            break :blk tensor_view{
                .data = t.data,
                .dtype = t.dtype,
                .rows = @intCast(t.shape[0]),
                .cols = @intCast(t.shape[1]),
            };
        };
        switch (embedding.dtype) {
            .f16, .bf16, .f32 => {},
            else => return error.UnsupportedTensorType,
        }

        const compute = try allocator.create(native_mod.NativeCompute);
        errdefer allocator.destroy(compute);
        compute.* = if (io) |runtime|
            native_mod.NativeCompute.initWithIo(allocator, weights, null, runtime)
        else
            native_mod.NativeCompute.init(allocator, weights, null);
        // Consume the f16 checkpoint matrices directly instead of widening
        // every projection to f32 on each use.
        compute.borrow_typed_matrix_weights = true;

        return .{
            .allocator = allocator,
            .config = config,
            .tokenizer = tok,
            .compute = compute,
            .weights = weights,
            .embedding = embedding,
            .stop_token_ids = stop_token_ids,
        };
    }

    pub fn deinit(self: *Pipeline) void {
        if (build_options.enable_native) {
            var cb = self.compute.computeBackend();
            cb.deinit();
            self.weights.deinitOwned();
            self.allocator.destroy(self.weights);
        }
        self.allocator.free(self.stop_token_ids);
        self.tokenizer.deinitSelf();
        self.* = undefined;
    }

    fn backend(self: *Pipeline, control: ?InferenceExecutionControl) ops.ComputeBackend {
        var cb = self.compute.computeBackend();
        cb.execution_control = control;
        return cb;
    }

    pub fn tokenizePrompt(self: *Pipeline, question: ?[]const u8) !PromptTokens {
        var text = try PromptText.init(self.allocator, question);
        defer text.deinit(self.allocator);
        const tok = self.tokenizer.tokenizer();
        const before = try tok.encode(self.allocator, text.before_image);
        errdefer self.allocator.free(before);
        const after = try tok.encode(self.allocator, text.after_image);
        return .{ .before = before, .after = after };
    }

    /// Decoder input length for one image and question: BOS, text, image.
    pub fn inputTokenCount(self: *Pipeline, question: ?[]const u8) !usize {
        var tokens = try self.tokenizePrompt(question);
        defer tokens.deinit(self.allocator);
        return tokens.textCount() + self.config.imageTokens();
    }

    /// Preprocess (Pillow bicubic to 378x378, [-1, 1]) and encode one image
    /// into projected decoder embeddings [imageTokens * hidden_size].
    pub fn encodeImage(self: *Pipeline, image_bytes: []const u8, control: ?InferenceExecutionControl) ![]f32 {
        const pixels = try preprocessImage(self.allocator, self.config, image_bytes);
        defer self.allocator.free(pixels);
        var cb = self.backend(control);
        return arch.encodeImage(&cb, self.allocator, self.config, pixels, true);
    }

    pub fn embedTokens(self: *Pipeline, ids: []const i32, out: []f32) !void {
        const d = self.config.hidden_size;
        if (out.len != ids.len * d or self.embedding.cols != d) return error.InvalidEmbeddingShape;
        for (ids, 0..) |id, row| {
            if (id < 0 or @as(usize, @intCast(id)) >= self.embedding.rows) return error.TokenIdOutOfRange;
            const dst = out[row * d ..][0..d];
            const index: usize = @intCast(id);
            switch (self.embedding.dtype) {
                .f32 => {
                    const bytes = self.embedding.data[index * d * 4 ..][0 .. d * 4];
                    for (dst, 0..) |*v, i| v.* = @bitCast(std.mem.readInt(u32, bytes[i * 4 ..][0..4], .little));
                },
                .f16 => {
                    const bytes = self.embedding.data[index * d * 2 ..][0 .. d * 2];
                    for (dst, 0..) |*v, i| {
                        const half: f16 = @bitCast(std.mem.readInt(u16, bytes[i * 2 ..][0..2], .little));
                        v.* = @floatCast(half);
                    }
                },
                .bf16 => {
                    const bytes = self.embedding.data[index * d * 2 ..][0 .. d * 2];
                    for (dst, 0..) |*v, i| {
                        const bits: u32 = @as(u32, std.mem.readInt(u16, bytes[i * 2 ..][0..2], .little)) << 16;
                        v.* = @bitCast(bits);
                    }
                },
                else => return error.UnsupportedTensorType,
            }
        }
    }

    /// Assemble decoder input embeddings for one image and question.
    pub fn promptEmbeddings(self: *Pipeline, tokens: PromptTokens, image_embeddings: []const f32) ![]f32 {
        const d = self.config.hidden_size;
        const image_rows = self.config.imageTokens();
        if (image_embeddings.len != image_rows * d) return error.ImageFeatureCountMismatch;
        const rows = tokens.textCount() + image_rows;
        const out = try self.allocator.alloc(f32, rows * d);
        errdefer self.allocator.free(out);
        var row: usize = 0;
        const bos = [_]i32{self.config.bos_token_id};
        try self.embedTokens(&bos, out[0..d]);
        row += 1;
        try self.embedTokens(tokens.before, out[row * d ..][0 .. tokens.before.len * d]);
        row += tokens.before.len;
        @memcpy(out[row * d ..][0..image_embeddings.len], image_embeddings);
        row += image_rows;
        try self.embedTokens(tokens.after, out[row * d ..][0 .. tokens.after.len * d]);
        row += tokens.after.len;
        std.debug.assert(row == rows);
        return out;
    }

    /// Answer `question` about one encoded image. `config.temperature == 0`
    /// is greedy, matching upstream's default `generate` settings.
    pub fn answer(
        self: *Pipeline,
        image_bytes: []const u8,
        question: ?[]const u8,
        config: generation.GenerationConfig,
        control: ?InferenceExecutionControl,
    ) !generation.GenerationResult {
        if (control) |active| try active.check();
        var timer = Timer.start();
        var tokens = try self.tokenizePrompt(question);
        defer tokens.deinit(self.allocator);

        const image_embeddings = try self.encodeImage(image_bytes, control);
        defer self.allocator.free(image_embeddings);
        const vision_ms = timer.lapMs();

        const embeddings = try self.promptEmbeddings(tokens, image_embeddings);
        defer self.allocator.free(embeddings);
        const prompt_rows = embeddings.len / self.config.hidden_size;
        const max_new: usize = @intCast(@max(config.max_tokens, 1));
        if (prompt_rows + max_new > self.config.max_position_embeddings) return error.PromptTooLong;

        var result = try self.decode(embeddings, prompt_rows, tokens, config, control);
        if (result.timing_ms) |*timing| {
            timing.multimodal_prepare = vision_ms;
            timing.total = vision_ms + timing.prefill + timing.decode;
            if (platform.env.getenv("ANTFLY_MOONDREAM_TIMING") != null) {
                std.debug.print("moondream timing: vision_ms={d} prefill_ms={d} prompt_rows={d} decode_ms={d} tokens={d}\n", .{
                    vision_ms, timing.prefill, prompt_rows, timing.decode, result.tokens_used,
                });
            }
        }
        return result;
    }

    fn decode(
        self: *Pipeline,
        embeddings: []const f32,
        prompt_rows: usize,
        tokens: PromptTokens,
        config: generation.GenerationConfig,
        control: ?InferenceExecutionControl,
    ) !generation.GenerationResult {
        var cb = self.backend(control);
        var kv = try arch.KvState.init(self.allocator, self.config);
        defer kv.deinit();
        kv.host_prefill_attention = true;

        var history = std.ArrayListUnmanaged(i64).empty;
        defer history.deinit(self.allocator);
        try history.append(self.allocator, self.config.bos_token_id);
        for (tokens.before) |id| try history.append(self.allocator, id);
        for (tokens.after) |id| try history.append(self.allocator, id);

        var generated = std.ArrayListUnmanaged(i32).empty;
        defer generated.deinit(self.allocator);

        var timer = Timer.start();
        var logits = try arch.forwardLastLogits(&cb, self.allocator, self.config, embeddings, prompt_rows, &kv);
        const prefill_ms = timer.lapMs();
        defer self.allocator.free(logits);

        var finish_reason: []const u8 = "length";
        var stopped_on: ?i32 = null;
        const max_new: usize = @intCast(@max(config.max_tokens, 1));
        const row = try self.allocator.alloc(f32, self.config.hidden_size);
        defer self.allocator.free(row);
        for (0..max_new) |step| {
            if (control) |active| try active.update(.executing, @intCast(step), @intCast(max_new));
            const next: i32 = @intCast(generation.sampleTokenFromLogits(self.allocator, logits, config, history.items));
            if (!config.ignore_eos and self.isStopToken(next)) {
                finish_reason = "stop";
                stopped_on = next;
                break;
            }
            try generated.append(self.allocator, next);
            try history.append(self.allocator, next);
            if (step + 1 == max_new) break;
            try self.embedTokens(&.{next}, row);
            const next_logits = try arch.forwardLastLogits(&cb, self.allocator, self.config, row, 1, &kv);
            self.allocator.free(logits);
            logits = next_logits;
        }
        const decode_ms = timer.lapMs();

        const raw = try self.tokenizer.tokenizer().decode(self.allocator, generated.items);
        defer self.allocator.free(raw);
        const text = try cleanAnswer(self.allocator, raw);
        errdefer self.allocator.free(text);
        return .{
            .text = text,
            .token_ids = try self.allocator.dupe(i32, generated.items),
            .prompt_tokens = prompt_rows,
            .tokens_used = generated.items.len,
            .finish_reason = finish_reason,
            .timing_ms = .{ .prefill = prefill_ms, .decode = decode_ms },
            .allocator = self.allocator,
        };
    }

    fn isStopToken(self: *const Pipeline, id: i32) bool {
        for (self.stop_token_ids) |stop| if (stop == id) return true;
        return false;
    }

    /// Answer the last user turn's text about its single image; used by
    /// `/generate` multimodal requests. Uses `execution_control`.
    pub fn generate(
        self: *Pipeline,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
    ) !generation.GenerationResult {
        var image: ?[]const u8 = null;
        var question: ?[]const u8 = null;
        for (messages) |message| {
            if (message.image_bytes) |images| for (images) |img| {
                if (image != null) return error.MultipleImagesUnsupported;
                image = img;
            };
            if (std.mem.eql(u8, message.role, "user")) question = message.content;
        }
        const bytes = image orelse return error.NoImages;
        return self.answer(bytes, question, config, self.execution_control);
    }

    /// Single-chunk streaming: the answer is delivered once complete.
    pub fn generateStreaming(
        self: *Pipeline,
        messages: []const generation.Message,
        config: generation.GenerationConfig,
        on_token_ctx: *anyopaque,
        on_token: generation.TokenCallback,
    ) !generation.GenerationResult {
        var result = try self.generate(messages, config);
        errdefer result.deinit();
        if (result.text.len > 0 and !on_token(on_token_ctx, result.text)) {
            result.finish_reason = "stop";
        }
        return result;
    }
};

const Timer = struct {
    last: u64,

    fn start() Timer {
        return .{ .last = monotonicNowNs() };
    }

    fn lapMs(self: *Timer) u64 {
        const now = monotonicNowNs();
        const elapsed = now -| self.last;
        self.last = now;
        return elapsed / std.time.ns_per_ms;
    }
};

fn monotonicNowNs() u64 {
    if (comptime @import("builtin").os.tag == .freestanding) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

/// Preprocess like upstream: RGB, Pillow bicubic resize to image_size,
/// rescale to [0, 1], normalize with mean/std 0.5. Returns CHW f32.
pub fn preprocessImage(allocator: std.mem.Allocator, config: Config, image_bytes: []const u8) ![]f32 {
    return image_mod.preprocessWithResample(
        allocator,
        image_bytes,
        @intCast(config.image_size),
        config.image_mean,
        config.image_std,
        .pillow_bicubic,
    );
}

/// Upstream passes every token of `tokenizer("<END>")` as an EOS id; we add
/// `<|endoftext|>` so a model that emits it stops instead of continuing.
fn loadStopTokens(allocator: std.mem.Allocator, tok: *hf_tokenizer.HfTokenizer, config: Config) ![]i32 {
    const ids = try tok.tokenizer().encode(allocator, stop_text);
    defer allocator.free(ids);
    var out = std.ArrayListUnmanaged(i32).empty;
    errdefer out.deinit(allocator);
    for (ids) |id| try out.append(allocator, id);
    var has_eos = false;
    for (out.items) |id| has_eos = has_eos or id == config.eos_token_id;
    if (!has_eos) try out.append(allocator, config.eos_token_id);
    return out.toOwnedSlice(allocator);
}

/// `re.sub("<$|<END$", "", answer).strip()`. The stop token itself is not
/// part of the generated ids here, so this only strips a partial marker.
pub fn cleanAnswer(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    var text = raw;
    if (std.mem.endsWith(u8, text, "<END")) {
        text = text[0 .. text.len - 4];
    } else if (std.mem.endsWith(u8, text, "<")) {
        text = text[0 .. text.len - 1];
    }
    return allocator.dupe(u8, std.mem.trim(u8, text, " \t\r\n"));
}

fn validateCheckpoint(allocator: std.mem.Allocator, store: tensor_store_mod.TensorStore, config: Config) !void {
    const expected = try arch.expectedTensors(allocator, config);
    defer arch.freeExpectedTensors(allocator, expected);
    for (expected) |want| {
        const range = (store.describeTensorRange(allocator, want.name) catch |err| switch (err) {
            error.TensorNotFound => {
                std.log.err("moondream checkpoint is missing tensor {s}", .{want.name});
                return error.MissingWeight;
            },
            else => return err,
        }) orelse return error.MissingWeight;
        if (range.shape.len != want.rank) return error.WeightShapeMismatch;
        for (range.shape, 0..) |dim, i| {
            if (dim < 0 or @as(usize, @intCast(dim)) != want.shape[i]) {
                std.log.err("moondream tensor {s} has unexpected shape", .{want.name});
                return error.WeightShapeMismatch;
            }
        }
    }
}

test "prompt text matches upstream answer_question" {
    var text = try PromptText.init(std.testing.allocator, "What is this?");
    defer text.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("<image>", text.before_image);
    try std.testing.expectEqualStrings("</image>\n\nQuestion: What is this?\n\nAnswer: ", text.after_image);

    var empty = try PromptText.init(std.testing.allocator, "  \n");
    defer empty.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("</image>\n\nQuestion: Describe this image.\n\nAnswer: ", empty.after_image);
}

test "cleanAnswer strips partial stop markers and whitespace" {
    const cases = [_][2][]const u8{
        .{ " A cat on a mat.<", "A cat on a mat." },
        .{ "A dog<END", "A dog" },
        .{ "  plain  ", "plain" },
    };
    for (cases) |case| {
        const out = try cleanAnswer(std.testing.allocator, case[0]);
        defer std.testing.allocator.free(out);
        try std.testing.expectEqualStrings(case[1], out);
    }
}
