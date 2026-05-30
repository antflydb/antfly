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

// ONNX Runtime GenAI backend for autoregressive text generation.
//
// Links against libonnxruntime-genai via the OGA C API.
// Only compiled when -Donnx=true is passed to the build.

const std = @import("std");
const c_file = @import("../util/c_file.zig");

const c = @cImport({
    @cInclude("ort_genai_c.h");
});

var overlay_package_nonce = std.atomic.Value(u64).init(0);

const decoder_candidates = [_][]const u8{
    "decoder_model_merged.onnx",
    "onnx/decoder_model_merged.onnx",
    "decoder_model_merged_fp16.onnx",
    "onnx/decoder_model_merged_fp16.onnx",
    "decoder_model_merged_q4f16.onnx",
    "onnx/decoder_model_merged_q4f16.onnx",
    "decoder_model_merged_q4.onnx",
    "onnx/decoder_model_merged_q4.onnx",
    "decoder_model_merged_quantized.onnx",
    "onnx/decoder_model_merged_quantized.onnx",
    "model.onnx",
    "onnx/model.onnx",
};

fn fileExists(path: []const u8) bool {
    return c_file.fileExists(std.heap.page_allocator, path);
}

fn pathExistsNoFollow(path: []const u8) bool {
    const path_z = std.heap.page_allocator.dupeSentinel(u8, path, 0) catch return false;
    defer std.heap.page_allocator.free(path_z);
    var stat_buf: c_file.c.struct_stat = undefined;
    return c_file.c.lstat(path_z.ptr, &stat_buf) == 0;
}

fn modelJoinExists(allocator: std.mem.Allocator, model_dir: []const u8, relative: []const u8) bool {
    const joined = std.fs.path.join(allocator, &.{ model_dir, relative }) catch return false;
    defer allocator.free(joined);
    return fileExists(joined);
}

fn mkdirIfMissing(path: []const u8) !void {
    const path_z = try std.heap.page_allocator.dupeSentinel(u8, path, 0);
    defer std.heap.page_allocator.free(path_z);
    if (c_file.c.mkdir(path_z.ptr, @as(c_file.c.mode_t, 0o755)) != 0 and !fileExists(path)) {
        return error.CreateDirectoryFailed;
    }
}

fn symlinkIfMissing(target: []const u8, link_path: []const u8) !void {
    if (pathExistsNoFollow(link_path)) return;
    const target_z = try std.heap.page_allocator.dupeSentinel(u8, target, 0);
    defer std.heap.page_allocator.free(target_z);
    const link_z = try std.heap.page_allocator.dupeSentinel(u8, link_path, 0);
    defer std.heap.page_allocator.free(link_z);
    if (c_file.c.symlink(target_z.ptr, link_z.ptr) != 0 and !pathExistsNoFollow(link_path)) {
        return error.CreateSymlinkFailed;
    }
}

fn removePathIfExists(path: []const u8) !void {
    if (!pathExistsNoFollow(path)) return;
    const path_z = try std.heap.page_allocator.dupeSentinel(u8, path, 0);
    defer std.heap.page_allocator.free(path_z);
    if (c_file.c.unlink(path_z.ptr) != 0 and pathExistsNoFollow(path)) {
        return error.RemovePathFailed;
    }
}

fn hardLinkOrSymlink(target: []const u8, link_path: []const u8) !void {
    try removePathIfExists(link_path);
    const target_z = try std.heap.page_allocator.dupeSentinel(u8, target, 0);
    defer std.heap.page_allocator.free(target_z);
    const link_z = try std.heap.page_allocator.dupeSentinel(u8, link_path, 0);
    defer std.heap.page_allocator.free(link_z);
    if (c_file.c.link(target_z.ptr, link_z.ptr) == 0) return;
    if (c_file.c.symlink(target_z.ptr, link_z.ptr) == 0) return;
    return error.CreateSymlinkFailed;
}

fn findFirstExistingRelativePath(allocator: std.mem.Allocator, model_dir: []const u8, candidates: []const []const u8) ?[]u8 {
    for (candidates) |candidate| {
        if (modelJoinExists(allocator, model_dir, candidate)) {
            return allocator.dupe(u8, candidate) catch null;
        }
    }
    return null;
}

fn findGenerativeOnnxRelativePath(allocator: std.mem.Allocator, model_dir: []const u8) ?[]u8 {
    return findFirstExistingRelativePath(allocator, model_dir, &decoder_candidates);
}

fn absolutePath(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    if (std.fs.path.isAbsolute(path)) return allocator.dupe(u8, path);
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd_z = c_file.c.getcwd(&buf, buf.len) orelse return error.GetCwdFailed;
    const cwd = std.mem.span(cwd_z);
    return std.fs.path.join(allocator, &.{ cwd, path });
}

fn findContextConfigObject(root: std.json.Value) ?std.json.ObjectMap {
    if (root != .object) return null;
    if (root.object.get("text_config")) |text_cfg| {
        if (text_cfg == .object) return text_cfg.object;
    }
    if (root.object.get("phi_config")) |phi_cfg| {
        if (phi_cfg == .object) return phi_cfg.object;
    }
    return root.object;
}

fn jsonLookupInt(obj: std.json.ObjectMap, key: []const u8) ?i64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => value.integer,
        .float => @intFromFloat(value.float),
        else => null,
    };
}

fn jsonLookupString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return if (value == .string) value.string else null;
}

fn jsonLookupIntArray(allocator: std.mem.Allocator, obj: std.json.ObjectMap, key: []const u8) ?[]i64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => blk: {
            const ids = allocator.alloc(i64, 1) catch return null;
            ids[0] = value.integer;
            break :blk ids;
        },
        .float => blk: {
            const ids = allocator.alloc(i64, 1) catch return null;
            ids[0] = @intFromFloat(value.float);
            break :blk ids;
        },
        .array => blk: {
            const ids = allocator.alloc(i64, value.array.items.len) catch return null;
            var count: usize = 0;
            for (value.array.items) |item| {
                switch (item) {
                    .integer => {
                        ids[count] = item.integer;
                        count += 1;
                    },
                    .float => {
                        ids[count] = @intFromFloat(item.float);
                        count += 1;
                    },
                    else => {},
                }
            }
            if (count == 0) {
                allocator.free(ids);
                return null;
            }
            if (count < ids.len) {
                break :blk allocator.realloc(ids, count) catch ids[0..count];
            }
            break :blk ids;
        },
        else => null,
    };
}

fn inferGenAiModelType(root_obj: std.json.ObjectMap, text_obj: std.json.ObjectMap, has_vision: bool) []const u8 {
    if (has_vision) {
        if (jsonLookupString(root_obj, "model_type")) |ty| {
            if (std.mem.eql(u8, ty, "moondream1")) {
                return "phi3v";
            }
            return ty;
        }
    }
    if (jsonLookupString(text_obj, "model_type")) |ty| {
        if (std.mem.eql(u8, ty, "gemma3")) return "gemma3_text";
        return ty;
    }
    if (jsonLookupString(root_obj, "model_type")) |ty| {
        if (std.mem.eql(u8, ty, "gemma3")) return if (has_vision) "gemma3" else "gemma3_text";
        if (std.mem.eql(u8, ty, "gemma")) return "gemma";
        if (std.mem.eql(u8, ty, "gemma2")) return "gemma";
        if (std.mem.eql(u8, ty, "phi3")) return "phi";
        return ty;
    }
    if (root_obj.get("architectures")) |archs| {
        if (archs == .array and archs.array.items.len > 0 and archs.array.items[0] == .string) {
            const arch = std.ascii.allocLowerString(std.heap.page_allocator, archs.array.items[0].string) catch return "gpt2";
            defer std.heap.page_allocator.free(arch);
            if (std.mem.indexOf(u8, arch, "gemma") != null) return if (has_vision) "gemma3" else "gemma3_text";
            if (std.mem.indexOf(u8, arch, "llama") != null) return "llama";
            if (std.mem.indexOf(u8, arch, "mistral") != null) return "mistral";
            if (std.mem.indexOf(u8, arch, "phi") != null) return "phi";
            if (std.mem.indexOf(u8, arch, "qwen") != null) return "qwen2";
        }
    }
    return "gpt2";
}

fn allocIntArrayJson(allocator: std.mem.Allocator, ints: []const i64) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, '[');
    for (ints, 0..) |value, idx| {
        if (idx != 0) try out.appendSlice(allocator, ", ");
        const piece = try std.fmt.allocPrint(allocator, "{d}", .{value});
        defer allocator.free(piece);
        try out.appendSlice(allocator, piece);
    }
    try out.append(allocator, ']');
    return out.toOwnedSlice(allocator);
}

fn shouldForceOverlayGenAiPackage(allocator: std.mem.Allocator, model_dir: []const u8) bool {
    if (!modelJoinExists(allocator, model_dir, "config.json")) return false;

    const config_path = std.fs.path.join(allocator, &.{ model_dir, "config.json" }) catch return false;
    defer allocator.free(config_path);
    const config_bytes = c_file.readFileMax(allocator, config_path, 16 * 1024 * 1024) catch return false;
    defer allocator.free(config_bytes);

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;

    return if (jsonLookupString(parsed.value.object, "model_type")) |model_type|
        std.mem.eql(u8, model_type, "moondream1")
    else
        false;
}

fn writeGeneratedGenAiConfig(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    decoder_filename: []const u8,
    model_type: []const u8,
    bos_token_id: i64,
    eos_token_ids: []const i64,
    pad_token_id: i64,
    context_length: i64,
    hidden_size: i64,
    num_hidden_layers: i64,
    num_attention_heads: i64,
    num_key_value_heads: i64,
    head_dim: i64,
    vocab_size: i64,
    do_sample: bool,
    top_k: i64,
    top_p: f64,
) !void {
    const eos_json = try allocIntArrayJson(allocator, eos_token_ids);
    defer allocator.free(eos_json);
    const out = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\  "model": {{
        \\    "bos_token_id": {d},
        \\    "context_length": {d},
        \\    "decoder": {{
        \\      "session_options": {{
        \\        "log_id": "onnxruntime-genai",
        \\        "provider_options": []
        \\      }},
        \\      "filename": "{s}",
        \\      "head_size": {d},
        \\      "hidden_size": {d},
        \\      "inputs": {{
        \\        "input_ids": "input_ids",
        \\        "attention_mask": "attention_mask",
        \\        "past_key_names": "past_key_values.%d.key",
        \\        "past_value_names": "past_key_values.%d.value"
        \\      }},
        \\      "outputs": {{
        \\        "logits": "logits",
        \\        "present_key_names": "present.%d.key",
        \\        "present_value_names": "present.%d.value"
        \\      }},
        \\      "num_attention_heads": {d},
        \\      "num_hidden_layers": {d},
        \\      "num_key_value_heads": {d}
        \\    }},
        \\    "eos_token_id": {s},
        \\    "pad_token_id": {d},
        \\    "type": "{s}",
        \\    "vocab_size": {d}
        \\  }},
        \\  "search": {{
        \\    "diversity_penalty": 0.0,
        \\    "do_sample": {s},
        \\    "early_stopping": true,
        \\    "length_penalty": 1.0,
        \\    "max_length": {d},
        \\    "min_length": 0,
        \\    "no_repeat_ngram_size": 0,
        \\    "num_beams": 1,
        \\    "num_return_sequences": 1,
        \\    "past_present_share_buffer": true,
        \\    "repetition_penalty": 1.0,
        \\    "temperature": 1.0,
        \\    "top_k": {d},
        \\    "top_p": {d}
        \\  }}
        \\}}
    ,
        .{
            bos_token_id,
            context_length,
            decoder_filename,
            head_dim,
            hidden_size,
            num_attention_heads,
            num_hidden_layers,
            num_key_value_heads,
            eos_json,
            pad_token_id,
            model_type,
            vocab_size,
            if (do_sample) "true" else "false",
            context_length,
            top_k,
            top_p,
        },
    );
    defer allocator.free(out);

    const genai_path = try std.fs.path.join(allocator, &.{ model_dir, "genai_config.json" });
    defer allocator.free(genai_path);
    const genai_path_z = try allocator.dupeSentinel(u8, genai_path, 0);
    defer allocator.free(genai_path_z);
    const fd = c_file.c.open(genai_path_z.ptr, c_file.c.O_WRONLY | c_file.c.O_CREAT | c_file.c.O_TRUNC, @as(c_file.c.mode_t, 0o644));
    if (fd < 0) return error.GenAiConfigWriteFailed;
    defer _ = c_file.c.close(fd);
    const written = c_file.c.write(fd, out.ptr, out.len);
    if (written != @as(isize, @intCast(out.len))) return error.GenAiConfigWriteFailed;
}

fn defaultArchitecturesForModelType(model_type: []const u8) []const u8 {
    if (std.mem.eql(u8, model_type, "gemma") or
        std.mem.eql(u8, model_type, "gemma2") or
        std.mem.eql(u8, model_type, "gemma3") or
        std.mem.eql(u8, model_type, "gemma3_text"))
    {
        return "[\"GemmaForCausalLM\"]";
    }
    if (std.mem.eql(u8, model_type, "llama")) return "[\"LlamaForCausalLM\"]";
    if (std.mem.eql(u8, model_type, "mistral")) return "[\"MistralForCausalLM\"]";
    if (std.mem.eql(u8, model_type, "phi") or std.mem.eql(u8, model_type, "phi3")) return "[\"PhiForCausalLM\"]";
    if (std.mem.eql(u8, model_type, "phi3v")) return "[\"Phi3VForCausalLM\"]";
    if (std.mem.eql(u8, model_type, "qwen2")) return "[\"Qwen2ForCausalLM\"]";
    if (std.mem.eql(u8, model_type, "gpt2")) return "[\"GPT2LMHeadModel\"]";
    return "[\"AutoModelForCausalLM\"]";
}

fn writeGeneratedHuggingFaceConfigFromGenAi(allocator: std.mem.Allocator, model_dir: []const u8) !void {
    const genai_path = try std.fs.path.join(allocator, &.{ model_dir, "genai_config.json" });
    defer allocator.free(genai_path);
    const genai_bytes = try c_file.readFileMax(allocator, genai_path, 16 * 1024 * 1024);
    defer allocator.free(genai_bytes);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, genai_bytes, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGenAiConfig;
    const root_obj = parsed.value.object;
    const model_value = root_obj.get("model") orelse return error.InvalidGenAiConfig;
    if (model_value != .object) return error.InvalidGenAiConfig;
    const model_obj = model_value.object;

    const model_type = jsonLookupString(model_obj, "type") orelse "gpt2";
    const architectures = defaultArchitecturesForModelType(model_type);
    const decoder_value = model_obj.get("decoder") orelse return error.InvalidGenAiConfig;
    if (decoder_value != .object) return error.InvalidGenAiConfig;
    const decoder_obj = decoder_value.object;

    const bos_token_id = jsonLookupInt(model_obj, "bos_token_id") orelse 2;
    const eos_token_ids = model_obj.get("eos_token_id");
    const eos_token_id = if (eos_token_ids) |value|
        switch (value) {
            .integer => value.integer,
            .array => blk: {
                for (value.array.items) |item| {
                    if (item == .integer) break :blk item.integer;
                }
                break :blk 1;
            },
            else => 1,
        }
    else
        1;
    const pad_token_id = jsonLookupInt(model_obj, "pad_token_id") orelse 0;
    const context_length = jsonLookupInt(model_obj, "context_length") orelse 8192;
    const hidden_size = jsonLookupInt(decoder_obj, "hidden_size") orelse 2048;
    const num_hidden_layers = jsonLookupInt(decoder_obj, "num_hidden_layers") orelse 16;
    const num_attention_heads = jsonLookupInt(decoder_obj, "num_attention_heads") orelse 8;
    const num_key_value_heads = jsonLookupInt(decoder_obj, "num_key_value_heads") orelse num_attention_heads;
    const head_dim = jsonLookupInt(decoder_obj, "head_size") orelse @divTrunc(hidden_size, num_attention_heads);
    const vocab_size = jsonLookupInt(model_obj, "vocab_size") orelse jsonLookupInt(decoder_obj, "vocab_size") orelse 32000;

    const out = try std.fmt.allocPrint(
        allocator,
        \\{{
        \\  "architectures": {s},
        \\  "model_type": "{s}",
        \\  "vocab_size": {d},
        \\  "hidden_size": {d},
        \\  "num_hidden_layers": {d},
        \\  "num_attention_heads": {d},
        \\  "num_key_value_heads": {d},
        \\  "head_dim": {d},
        \\  "max_position_embeddings": {d},
        \\  "bos_token_id": {d},
        \\  "eos_token_id": {d},
        \\  "pad_token_id": {d}
        \\}}
    ,
        .{
            architectures,
            model_type,
            vocab_size,
            hidden_size,
            num_hidden_layers,
            num_attention_heads,
            num_key_value_heads,
            head_dim,
            context_length,
            bos_token_id,
            eos_token_id,
            pad_token_id,
        },
    );
    defer allocator.free(out);

    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "config.json" });
    defer allocator.free(config_path);
    const config_path_z = try allocator.dupeSentinel(u8, config_path, 0);
    defer allocator.free(config_path_z);
    if (pathExistsNoFollow(config_path)) {
        _ = c_file.c.unlink(config_path_z.ptr);
    }
    const fd = c_file.c.open(config_path_z.ptr, c_file.c.O_WRONLY | c_file.c.O_CREAT | c_file.c.O_TRUNC, @as(c_file.c.mode_t, 0o644));
    if (fd < 0) return error.ConfigWriteFailed;
    defer _ = c_file.c.close(fd);
    const written = c_file.c.write(fd, out.ptr, out.len);
    if (written != @as(isize, @intCast(out.len))) return error.ConfigWriteFailed;
}

pub fn ensureGenerativeModelPackage(allocator: std.mem.Allocator, model_dir: []const u8) !bool {
    if (modelJoinExists(allocator, model_dir, "genai_config.json")) return true;

    const decoder_filename = findGenerativeOnnxRelativePath(allocator, model_dir) orelse return false;
    defer allocator.free(decoder_filename);
    if (!modelJoinExists(allocator, model_dir, "config.json")) return false;

    const config_path = try std.fs.path.join(allocator, &.{ model_dir, "config.json" });
    defer allocator.free(config_path);
    const config_bytes = try c_file.readFileMax(allocator, config_path, 16 * 1024 * 1024);
    defer allocator.free(config_bytes);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{});
    defer parsed.deinit();

    const root_obj = parsed.value.object;
    const text_obj = findContextConfigObject(parsed.value) orelse return false;
    const has_vision = modelJoinExists(allocator, model_dir, "onnx/vision_encoder.onnx") or modelJoinExists(allocator, model_dir, "vision_encoder.onnx");
    const model_type = inferGenAiModelType(root_obj, text_obj, has_vision);

    const generation_config_path = try std.fs.path.join(allocator, &.{ model_dir, "generation_config.json" });
    defer allocator.free(generation_config_path);
    var do_sample = true;
    var top_k: i64 = 1;
    var top_p: f64 = 1.0;
    var eos_token_ids: []i64 = &.{};
    if (fileExists(generation_config_path)) {
        const generation_config_bytes = try c_file.readFileMax(allocator, generation_config_path, 4 * 1024 * 1024);
        defer allocator.free(generation_config_bytes);
        var gen_parsed = try std.json.parseFromSlice(std.json.Value, allocator, generation_config_bytes, .{});
        defer gen_parsed.deinit();
        if (gen_parsed.value == .object) {
            const gen_obj = gen_parsed.value.object;
            if (gen_obj.get("do_sample")) |value| {
                if (value == .bool) do_sample = value.bool;
            }
            if (jsonLookupInt(gen_obj, "top_k")) |value| top_k = value;
            if (gen_obj.get("top_p")) |value| switch (value) {
                .float => top_p = value.float,
                .integer => top_p = @floatFromInt(value.integer),
                else => {},
            };
            if (jsonLookupIntArray(allocator, gen_obj, "eos_token_id")) |ids| eos_token_ids = ids;
        }
    }
    defer if (eos_token_ids.len > 0 and @intFromPtr(eos_token_ids.ptr) != @intFromPtr((&[_]i64{}).ptr)) allocator.free(eos_token_ids);

    if (eos_token_ids.len == 0) {
        eos_token_ids = jsonLookupIntArray(allocator, text_obj, "eos_token_id") orelse jsonLookupIntArray(allocator, root_obj, "eos_token_id") orelse blk: {
            const ids = try allocator.alloc(i64, 1);
            ids[0] = 1;
            break :blk ids;
        };
    }

    const bos_token_id = jsonLookupInt(text_obj, "bos_token_id") orelse jsonLookupInt(root_obj, "bos_token_id") orelse 2;
    const pad_token_id = jsonLookupInt(text_obj, "pad_token_id") orelse jsonLookupInt(root_obj, "pad_token_id") orelse 0;
    const context_length = jsonLookupInt(text_obj, "max_position_embeddings") orelse jsonLookupInt(root_obj, "max_position_embeddings") orelse 8192;
    const hidden_size = jsonLookupInt(text_obj, "hidden_size") orelse jsonLookupInt(root_obj, "hidden_size") orelse 2048;
    const num_hidden_layers = jsonLookupInt(text_obj, "num_hidden_layers") orelse jsonLookupInt(root_obj, "num_hidden_layers") orelse 16;
    const num_attention_heads = jsonLookupInt(text_obj, "num_attention_heads") orelse jsonLookupInt(root_obj, "num_attention_heads") orelse 8;
    const num_key_value_heads = jsonLookupInt(text_obj, "num_key_value_heads") orelse jsonLookupInt(root_obj, "num_key_value_heads") orelse num_attention_heads;
    const head_dim = jsonLookupInt(text_obj, "head_dim") orelse jsonLookupInt(root_obj, "head_dim") orelse @divTrunc(hidden_size, num_attention_heads);
    const vocab_size = jsonLookupInt(text_obj, "vocab_size") orelse jsonLookupInt(root_obj, "vocab_size") orelse 32000;

    try writeGeneratedGenAiConfig(
        allocator,
        model_dir,
        decoder_filename,
        model_type,
        bos_token_id,
        eos_token_ids,
        pad_token_id,
        context_length,
        hidden_size,
        num_hidden_layers,
        num_attention_heads,
        num_key_value_heads,
        head_dim,
        vocab_size,
        do_sample,
        top_k,
        top_p,
    );
    return true;
}

fn createOverlayPackage(allocator: std.mem.Allocator, model_dir: []const u8) ![]u8 {
    const absolute_model_dir = try absolutePath(allocator, model_dir);
    defer allocator.free(absolute_model_dir);
    const hash = std.hash.Wyhash.hash(0, absolute_model_dir);
    try mkdirIfMissing("/tmp/termite-ortgenai");
    const nonce = overlay_package_nonce.fetchAdd(1, .monotonic);
    const pid: u32 = @intCast(c_file.c.getpid());
    const base_dir = try std.fmt.allocPrint(allocator, "/tmp/termite-ortgenai/{x}-{x}-{x}", .{ hash, pid, nonce });
    errdefer allocator.free(base_dir);
    try mkdirIfMissing(base_dir);

    const optional_files = [_][]const u8{
        "config.json",
        "generation_config.json",
        "merges.txt",
        "vocab.json",
        "tokenizer.json",
        "tokenizer_config.json",
        "chat_template.jinja",
        "processor_config.json",
        "preprocessor_config.json",
        "special_tokens_map.json",
    };
    for (optional_files) |name| {
        if (!modelJoinExists(allocator, absolute_model_dir, name)) continue;
        const target = try std.fs.path.join(allocator, &.{ absolute_model_dir, name });
        defer allocator.free(target);
        const link_path = try std.fs.path.join(allocator, &.{ base_dir, name });
        defer allocator.free(link_path);
        try symlinkIfMissing(target, link_path);
    }

    if (modelJoinExists(allocator, absolute_model_dir, "onnx")) {
        const onnx_dir = try std.fs.path.join(allocator, &.{ absolute_model_dir, "onnx" });
        defer allocator.free(onnx_dir);
        const overlay_onnx_dir = try std.fs.path.join(allocator, &.{ base_dir, "onnx" });
        defer allocator.free(overlay_onnx_dir);
        try mkdirIfMissing(overlay_onnx_dir);
        const onnx_dir_z = try allocator.dupeSentinel(u8, onnx_dir, 0);
        defer allocator.free(onnx_dir_z);
        const dir = c_file.c.opendir(onnx_dir_z.ptr) orelse return error.OpenDirectoryFailed;
        defer _ = c_file.c.closedir(dir);

        while (c_file.c.readdir(dir)) |entry| {
            const name_z: [*:0]const u8 = @ptrCast(&entry.*.d_name);
            const name = std.mem.span(name_z);
            if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
            if (!(std.mem.startsWith(u8, name, "decoder_model_merged") or
                std.mem.startsWith(u8, name, "embed_tokens") or
                std.mem.startsWith(u8, name, "vision_encoder")))
            {
                continue;
            }

            const target = try std.fs.path.join(allocator, &.{ onnx_dir, name });
            defer allocator.free(target);
            const root_link_path = try std.fs.path.join(allocator, &.{ base_dir, name });
            defer allocator.free(root_link_path);
            try removePathIfExists(root_link_path);
            const link_path = try std.fs.path.join(allocator, &.{ overlay_onnx_dir, name });
            defer allocator.free(link_path);
            try hardLinkOrSymlink(target, link_path);
        }
    }

    const genai_path = try std.fs.path.join(allocator, &.{ base_dir, "genai_config.json" });
    defer allocator.free(genai_path);
    if (pathExistsNoFollow(genai_path)) {
        const genai_path_z = try allocator.dupeSentinel(u8, genai_path, 0);
        defer allocator.free(genai_path_z);
        _ = c_file.c.unlink(genai_path_z.ptr);
    }
    _ = try ensureGenerativeModelPackage(allocator, base_dir);
    try writeGeneratedHuggingFaceConfigFromGenAi(allocator, base_dir);
    return base_dir;
}

pub fn prepareGenerativeModelPackage(allocator: std.mem.Allocator, model_dir: []const u8) !?[]u8 {
    if (modelJoinExists(allocator, model_dir, "genai_config.json")) {
        if (shouldForceOverlayGenAiPackage(allocator, model_dir)) {
            return try createOverlayPackage(allocator, model_dir);
        }
        return try allocator.dupe(u8, model_dir);
    }
    if (!isGenerativeModel(model_dir)) return null;
    if (ensureGenerativeModelPackage(allocator, model_dir)) |_| {
        return try allocator.dupe(u8, model_dir);
    } else |_| {
        return try createOverlayPackage(allocator, model_dir);
    }
}

/// Check an OGA result and return an error if non-null.
fn check(result: ?*c.OgaResult) !void {
    if (result) |r| {
        if (c.OgaResultGetError(r)) |msg| {
            std.log.err("ortgenai: {s}", .{std.mem.span(msg)});
        }
        defer c.OgaDestroyResult(r);
        return error.OrtGenAiFailed;
    }
}

/// A loaded generative model (wraps OgaModel + OgaTokenizer).
pub const GenAiModel = struct {
    model: *c.OgaModel,
    tokenizer: *c.OgaTokenizer,
    context_length: i32,

    pub fn load(allocator: std.mem.Allocator, model_dir: []const u8) !GenAiModel {
        const path_z = try allocator.dupeSentinel(u8, model_dir, 0);
        defer allocator.free(path_z);

        var model: ?*c.OgaModel = null;
        try check(c.OgaCreateModel(path_z.ptr, &model));
        errdefer c.OgaDestroyModel(model.?);

        var tokenizer: ?*c.OgaTokenizer = null;
        try check(c.OgaCreateTokenizer(model.?, &tokenizer));

        return .{
            .model = model.?,
            .tokenizer = tokenizer.?,
            .context_length = 8192,
        };
    }

    pub fn deinit(self: *GenAiModel) void {
        c.OgaDestroyTokenizer(self.tokenizer);
        c.OgaDestroyModel(self.model);
    }
};

/// Options for text generation.
pub const GenerateOptions = struct {
    max_tokens: i32 = 256,
    temperature: f32 = 0,
    top_p: f32 = 0,
    top_k: i32 = 0,
};

/// Result of a generation call.
pub const GenerateResult = struct {
    text: []const u8,
    tokens_used: usize,
    finish_reason: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GenerateResult) void {
        self.allocator.free(self.text);
    }
};

pub const FirstTokenDebug = struct {
    token_id: i32,
    text: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *FirstTokenDebug) void {
        self.allocator.free(self.text);
    }
};

/// Generate text from a prompt using ortgenai.
pub fn generate(allocator: std.mem.Allocator, model: *const GenAiModel, prompt: []const u8, opts: GenerateOptions) !GenerateResult {
    // Encode prompt to token sequences
    const prompt_z = try allocator.dupeSentinel(u8, prompt, 0);
    defer allocator.free(prompt_z);

    var sequences: ?*c.OgaSequences = null;
    try check(c.OgaCreateSequences(&sequences));
    defer c.OgaDestroySequences(sequences.?);

    // Tokenize the prompt
    try check(c.OgaTokenizerEncode(model.tokenizer, prompt_z.ptr, sequences.?));

    // Create generator params
    var params: ?*c.OgaGeneratorParams = null;
    try check(c.OgaCreateGeneratorParams(model.model, &params));
    defer c.OgaDestroyGeneratorParams(params.?);

    // Set generation parameters
    const max_length = if (opts.max_tokens > 0) opts.max_tokens + @as(i32, @intCast(c.OgaSequencesGetSequenceCount(sequences.?, 0))) else model.context_length;
    try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "max_length", @floatFromInt(max_length)));

    if (opts.temperature > 0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "temperature", opts.temperature));
        try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", true));
    } else {
        // Explicitly disable sampling for greedy decoding — otherwise ortgenai
        // falls through to the model's genai_config.json defaults which may have
        // do_sample:true, causing nondeterministic output and random early EOS.
        try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", false));
    }
    if (opts.top_p > 0 and opts.top_p < 1.0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "top_p", opts.top_p));
    }
    if (opts.top_k > 0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "top_k", @floatFromInt(opts.top_k)));
    }

    // Create generator and set input sequences
    var generator: ?*c.OgaGenerator = null;
    try check(c.OgaCreateGenerator(model.model, params.?, &generator));
    defer c.OgaDestroyGenerator(generator.?);

    try check(c.OgaGenerator_AppendTokenSequences(generator.?, sequences.?));

    // Create tokenizer stream for decoding
    var stream: ?*c.OgaTokenizerStream = null;
    try check(c.OgaCreateTokenizerStream(model.tokenizer, &stream));
    defer c.OgaDestroyTokenizerStream(stream.?);

    // Generate tokens
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    var token_count: usize = 0;
    var hit_max = false;

    while (!c.OgaGenerator_IsDone(generator.?)) {
        try check(c.OgaGenerator_GenerateNextToken(generator.?));

        // Get the newly generated token via GetNextTokens
        var next_tokens: [*c]const i32 = undefined;
        var next_count: usize = 0;
        try check(c.OgaGenerator_GetNextTokens(generator.?, &next_tokens, &next_count));
        if (next_count == 0) break;
        const new_token = next_tokens[0];

        // Decode token to text
        var decoded: [*c]const u8 = null;
        try check(c.OgaTokenizerStreamDecode(stream.?, new_token, &decoded));
        if (decoded) |d| {
            const text = std.mem.span(d);
            try buf.appendSlice(allocator, text);
        }

        token_count += 1;
        if (opts.max_tokens > 0 and token_count >= @as(usize, @intCast(opts.max_tokens))) {
            hit_max = true;
            break;
        }
    }

    const text = try allocator.dupe(u8, buf.items);
    return .{
        .text = text,
        .tokens_used = token_count,
        .finish_reason = if (hit_max) "length" else "stop",
        .allocator = allocator,
    };
}

pub fn generateFirstTokenDebug(
    allocator: std.mem.Allocator,
    model: *const GenAiModel,
    prompt: []const u8,
    opts: GenerateOptions,
) !FirstTokenDebug {
    const prompt_z = try allocator.dupeSentinel(u8, prompt, 0);
    defer allocator.free(prompt_z);

    var sequences: ?*c.OgaSequences = null;
    try check(c.OgaCreateSequences(&sequences));
    defer c.OgaDestroySequences(sequences.?);
    try check(c.OgaTokenizerEncode(model.tokenizer, prompt_z.ptr, sequences.?));

    var params: ?*c.OgaGeneratorParams = null;
    try check(c.OgaCreateGeneratorParams(model.model, &params));
    defer c.OgaDestroyGeneratorParams(params.?);

    const max_length = if (opts.max_tokens > 0) opts.max_tokens + @as(i32, @intCast(c.OgaSequencesGetSequenceCount(sequences.?, 0))) else model.context_length;
    try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "max_length", @floatFromInt(max_length)));
    try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", false));

    var generator: ?*c.OgaGenerator = null;
    try check(c.OgaCreateGenerator(model.model, params.?, &generator));
    defer c.OgaDestroyGenerator(generator.?);
    try check(c.OgaGenerator_AppendTokenSequences(generator.?, sequences.?));

    var stream: ?*c.OgaTokenizerStream = null;
    try check(c.OgaCreateTokenizerStream(model.tokenizer, &stream));
    defer c.OgaDestroyTokenizerStream(stream.?);

    try check(c.OgaGenerator_GenerateNextToken(generator.?));
    var next_tokens: [*c]const i32 = undefined;
    var next_count: usize = 0;
    try check(c.OgaGenerator_GetNextTokens(generator.?, &next_tokens, &next_count));
    if (next_count == 0) return error.EmptyGeneration;
    const token_id = next_tokens[0];

    var decoded: [*c]const u8 = null;
    try check(c.OgaTokenizerStreamDecode(stream.?, token_id, &decoded));
    const text = try allocator.dupe(u8, if (decoded) |d| std.mem.span(d) else "");
    return .{
        .token_id = token_id,
        .text = text,
        .allocator = allocator,
    };
}

/// Generate text from a prompt with images using ortgenai's multimodal processor.
/// Images are raw decoded bytes (JPEG/PNG). The processor handles image preprocessing
/// (resize, normalize, pixel values) internally based on the model's genai_config.json.
pub fn generateWithImages(
    allocator: std.mem.Allocator,
    model: *const GenAiModel,
    prompt: []const u8,
    image_data: []const []const u8,
    opts: GenerateOptions,
) !GenerateResult {
    if (image_data.len == 0) return generate(allocator, model, prompt, opts);

    // Load images from byte buffers
    const ptrs = try allocator.alloc(*const anyopaque, image_data.len);
    defer allocator.free(ptrs);
    const sizes = try allocator.alloc(usize, image_data.len);
    defer allocator.free(sizes);
    for (image_data, 0..) |img, i| {
        ptrs[i] = @ptrCast(img.ptr);
        sizes[i] = img.len;
    }

    var images: ?*c.OgaImages = null;
    try check(c.OgaLoadImagesFromBuffers(
        @ptrCast(ptrs.ptr),
        sizes.ptr,
        image_data.len,
        &images,
    ));
    defer c.OgaDestroyImages(images.?);

    // Create multimodal processor
    var processor: ?*c.OgaMultiModalProcessor = null;
    try check(c.OgaCreateMultiModalProcessor(model.model, &processor));
    defer c.OgaDestroyMultiModalProcessor(processor.?);

    // Process prompt + images → named tensors
    const prompt_z = try allocator.dupeSentinel(u8, prompt, 0);
    defer allocator.free(prompt_z);

    var named_tensors: ?*c.OgaNamedTensors = null;
    try check(c.OgaProcessorProcessImages(processor.?, prompt_z.ptr, images.?, &named_tensors));
    defer c.OgaDestroyNamedTensors(named_tensors.?);

    // Create generator params
    var params: ?*c.OgaGeneratorParams = null;
    try check(c.OgaCreateGeneratorParams(model.model, &params));
    defer c.OgaDestroyGeneratorParams(params.?);

    // Set generation parameters
    try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "max_length", @floatFromInt(if (opts.max_tokens > 0) opts.max_tokens + 1024 else model.context_length)));

    if (opts.temperature > 0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "temperature", opts.temperature));
        try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", true));
    } else {
        try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", false));
    }
    if (opts.top_p > 0 and opts.top_p < 1.0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "top_p", opts.top_p));
    }
    if (opts.top_k > 0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "top_k", @floatFromInt(opts.top_k)));
    }

    // Create generator and set multimodal inputs (instead of AppendTokenSequences)
    var generator: ?*c.OgaGenerator = null;
    try check(c.OgaCreateGenerator(model.model, params.?, &generator));
    defer c.OgaDestroyGenerator(generator.?);

    try check(c.OgaGenerator_SetInputs(generator.?, named_tensors.?));

    // Create tokenizer stream for decoding
    var stream: ?*c.OgaTokenizerStream = null;
    try check(c.OgaCreateTokenizerStreamFromProcessor(processor.?, &stream));
    defer c.OgaDestroyTokenizerStream(stream.?);

    // Generate tokens (same loop as text-only)
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    var token_count: usize = 0;
    var hit_max = false;

    while (!c.OgaGenerator_IsDone(generator.?)) {
        try check(c.OgaGenerator_GenerateNextToken(generator.?));

        var next_tokens: [*c]const i32 = undefined;
        var next_count: usize = 0;
        try check(c.OgaGenerator_GetNextTokens(generator.?, &next_tokens, &next_count));
        if (next_count == 0) break;
        const new_token = next_tokens[0];

        var decoded: [*c]const u8 = null;
        try check(c.OgaTokenizerStreamDecode(stream.?, new_token, &decoded));
        if (decoded) |d| {
            const text = std.mem.span(d);
            try buf.appendSlice(allocator, text);
        }

        token_count += 1;
        if (opts.max_tokens > 0 and token_count >= @as(usize, @intCast(opts.max_tokens))) {
            hit_max = true;
            break;
        }
    }

    const text = try allocator.dupe(u8, buf.items);
    return .{
        .text = text,
        .tokens_used = token_count,
        .finish_reason = if (hit_max) "length" else "stop",
        .allocator = allocator,
    };
}

/// Streaming token callback. Called with each decoded token fragment.
/// Return `true` to continue generation, `false` to stop early.
pub const TokenCallback = *const fn (ctx: *anyopaque, token_text: []const u8) bool;

/// Generate text with streaming token callback.
/// Calls `on_token` for each decoded token. The callback can signal early stop by returning false.
pub fn generateStreaming(
    allocator: std.mem.Allocator,
    model: *const GenAiModel,
    prompt: []const u8,
    opts: GenerateOptions,
    on_token_ctx: *anyopaque,
    on_token: TokenCallback,
) !GenerateResult {
    const prompt_z = try allocator.dupeSentinel(u8, prompt, 0);
    defer allocator.free(prompt_z);

    var sequences: ?*c.OgaSequences = null;
    try check(c.OgaCreateSequences(&sequences));
    defer c.OgaDestroySequences(sequences.?);

    try check(c.OgaTokenizerEncode(model.tokenizer, prompt_z.ptr, sequences.?));

    var params: ?*c.OgaGeneratorParams = null;
    try check(c.OgaCreateGeneratorParams(model.model, &params));
    defer c.OgaDestroyGeneratorParams(params.?);

    const max_length = if (opts.max_tokens > 0) opts.max_tokens + @as(i32, @intCast(c.OgaSequencesGetSequenceCount(sequences.?, 0))) else model.context_length;
    try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "max_length", @floatFromInt(max_length)));

    if (opts.temperature > 0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "temperature", opts.temperature));
        try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", true));
    } else {
        try check(c.OgaGeneratorParamsSetSearchBool(params.?, "do_sample", false));
    }
    if (opts.top_p > 0 and opts.top_p < 1.0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "top_p", opts.top_p));
    }
    if (opts.top_k > 0) {
        try check(c.OgaGeneratorParamsSetSearchNumber(params.?, "top_k", @floatFromInt(opts.top_k)));
    }

    var generator: ?*c.OgaGenerator = null;
    try check(c.OgaCreateGenerator(model.model, params.?, &generator));
    defer c.OgaDestroyGenerator(generator.?);

    try check(c.OgaGenerator_AppendTokenSequences(generator.?, sequences.?));

    var stream: ?*c.OgaTokenizerStream = null;
    try check(c.OgaCreateTokenizerStream(model.tokenizer, &stream));
    defer c.OgaDestroyTokenizerStream(stream.?);

    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    var token_count: usize = 0;
    var hit_max = false;
    var stopped_by_callback = false;

    while (!c.OgaGenerator_IsDone(generator.?)) {
        try check(c.OgaGenerator_GenerateNextToken(generator.?));

        var next_tokens: [*c]const i32 = undefined;
        var next_count: usize = 0;
        try check(c.OgaGenerator_GetNextTokens(generator.?, &next_tokens, &next_count));
        if (next_count == 0) break;
        const new_token = next_tokens[0];

        var decoded: [*c]const u8 = null;
        try check(c.OgaTokenizerStreamDecode(stream.?, new_token, &decoded));
        if (decoded) |d| {
            const text = std.mem.span(d);
            try buf.appendSlice(allocator, text);
            // Call the streaming callback
            if (!on_token(on_token_ctx, text)) {
                stopped_by_callback = true;
                break;
            }
        }

        token_count += 1;
        if (opts.max_tokens > 0 and token_count >= @as(usize, @intCast(opts.max_tokens))) {
            hit_max = true;
            break;
        }
    }

    const text = try allocator.dupe(u8, buf.items);
    return .{
        .text = text,
        .tokens_used = token_count,
        .finish_reason = if (stopped_by_callback) "stop" else if (hit_max) "length" else "stop",
        .allocator = allocator,
    };
}

/// Check if a model directory contains ortgenai-compatible files.
pub fn isGenerativeModel(model_dir: []const u8) bool {
    const allocator = std.heap.page_allocator;
    if (modelJoinExists(allocator, model_dir, "genai_config.json")) return true;
    return findGenerativeOnnxRelativePath(allocator, model_dir) != null and modelJoinExists(allocator, model_dir, "config.json");
}

fn testingTmpDirPath(allocator: std.mem.Allocator, tmp: *const std.testing.TmpDir) ![]u8 {
    const relative = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(relative);
    return absolutePath(allocator, relative);
}

test "isGenerativeModel accepts hf onnx decoder in onnx subdir" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{}" });
    try tmp.dir.createDirPath(std.testing.io, "onnx");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "onnx/decoder_model_merged.onnx", .data = "" });

    const path = try testingTmpDirPath(allocator, &tmp);
    defer allocator.free(path);
    try std.testing.expect(isGenerativeModel(path));
}

test "ensureGenerativeModelPackage generates config from nested gemma3 text_config" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "onnx");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "onnx/decoder_model_merged.onnx", .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "onnx/vision_encoder.onnx", .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data =
        \\{
        \\  "model_type": "gemma3",
        \\  "text_config": {
        \\    "model_type": "gemma3_text",
        \\    "bos_token_id": 2,
        \\    "eos_token_id": 1,
        \\    "pad_token_id": 0,
        \\    "vocab_size": 262208,
        \\    "hidden_size": 2560,
        \\    "num_hidden_layers": 34,
        \\    "num_attention_heads": 8,
        \\    "num_key_value_heads": 4,
        \\    "head_dim": 256,
        \\    "max_position_embeddings": 131072
        \\  }
        \\}
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "generation_config.json", .data =
        \\{
        \\  "do_sample": true,
        \\  "eos_token_id": [1, 106],
        \\  "top_k": 64,
        \\  "top_p": 0.95
        \\}
    });

    const path = try testingTmpDirPath(allocator, &tmp);
    defer allocator.free(path);
    try std.testing.expect(try ensureGenerativeModelPackage(allocator, path));

    const generated = try tmp.dir.readFileAlloc(std.testing.io, "genai_config.json", allocator, .limited(32 * 1024));
    defer allocator.free(generated);
    try std.testing.expect(std.mem.indexOf(u8, generated, "\"filename\": \"onnx/decoder_model_merged.onnx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "\"type\": \"gemma3\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, generated, "\"vocab_size\": 262208") != null);
}

test "prepareGenerativeModelPackage uses overlay dir when forced" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.createDirPath(std.testing.io, "onnx");
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "onnx/decoder_model_merged.onnx", .data = "" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data = "{}" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data =
        \\{
        \\  "model_type": "moondream1",
        \\  "text_config": {
        \\    "model_type": "gemma3_text",
        \\    "bos_token_id": 2,
        \\    "eos_token_id": [1, 106],
        \\    "pad_token_id": 0,
        \\    "vocab_size": 262208,
        \\    "hidden_size": 2560,
        \\    "num_hidden_layers": 34,
        \\    "num_attention_heads": 8,
        \\    "num_key_value_heads": 4,
        \\    "head_dim": 256,
        \\    "max_position_embeddings": 131072
        \\  }
        \\}
    });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "genai_config.json", .data = "{}" });
    const path = try testingTmpDirPath(allocator, &tmp);
    defer allocator.free(path);

    const overlay = (try prepareGenerativeModelPackage(allocator, path)).?;
    defer allocator.free(overlay);
    try std.testing.expect(!std.mem.eql(u8, overlay, path));
    const generated = try c_file.readFileFromDir(allocator, overlay, "genai_config.json");
    defer allocator.free(generated);
    try std.testing.expect(std.mem.indexOf(u8, generated, "\"filename\": \"onnx/decoder_model_merged.onnx\"") != null);
}
