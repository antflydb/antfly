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
const build_options = @import("build_options");
const backends = @import("backends/backends.zig");
const metal_runtime = if (build_options.enable_metal) @import("backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }
};
const c_file = @import("util/c_file.zig");
const model_manager_mod = @import("server/model_manager.zig");
const native_backend_guard = @import("native_backend_guard.zig");
const session_factory = @import("architectures/session_factory.zig");
const transcription = @import("pipelines/transcription.zig");
const enc_dec_mod = @import("pipelines/encoder_decoder.zig");
const hf_tokenizer = @import("termite_hf_tokenizer");
const tokenizer_mod = @import("termite_tokenizer");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    metal,
};

const Options = struct {
    model_dir: []const u8,
    audio_path: []const u8,
    backend: BackendChoice = .auto,
    language: ?[]const u8 = null,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try ensureRequestedMetalHostedBackendAvailable(opts.backend);
    const audio_data = try c_file.readFile(allocator, opts.audio_path);
    defer allocator.free(audio_data);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    configureBackendPreference(&session_manager, opts.backend);

    if (enc_dec_mod.findEncoderDecoderPaths(allocator, opts.model_dir)) |paths| {
        defer allocator.free(paths.encoder);
        defer allocator.free(paths.decoder);

        var encoder_session = try session_manager.loadModel(paths.encoder);
        defer encoder_session.close();

        var decoder_session = try session_manager.loadModel(paths.decoder);
        defer decoder_session.close();

        const tok_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{opts.model_dir});
        defer allocator.free(tok_path);
        const tok_bytes = try c_file.readFile(allocator, tok_path);
        defer allocator.free(tok_bytes);

        var hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
        defer hf_tok.deinitSelf();

        const dec_config = enc_dec_mod.loadDecoderConfig(allocator, opts.model_dir) catch enc_dec_mod.DecoderConfig{};
        const forced_ids = loadWhisperForcedDecoderIds(allocator, opts.model_dir, hf_tok.tokenizer(), opts.language);
        defer if (forced_ids) |f| allocator.free(f);

        var pipeline = transcription.TranscriptionPipeline.init(
            allocator,
            encoder_session,
            decoder_session,
            hf_tok.tokenizer(),
            .{
                .max_length = dec_config.max_length,
                .decoder_start_token_id = dec_config.decoder_start_token_id,
                .eos_token_id = dec_config.eos_token_id,
                .language = opts.language,
                .forced_decoder_ids = forced_ids,
            },
        );

        var result = try pipeline.transcribe(audio_data);
        defer result.deinit();
        try writeResultJson(allocator, opts.model_dir, result.text, result.language);
        return;
    } else |_| {}

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const whisper_cfg = session_factory.getWhisperConfig(model.session) orelse return error.InvalidModelForTranscription;
    const forced_ids = if (model.hf_tok) |hf_tok|
        loadWhisperForcedDecoderIds(allocator, opts.model_dir, hf_tok.tokenizer(), opts.language)
    else
        loadForcedDecoderIds(allocator, opts.model_dir);
    defer if (forced_ids) |f| allocator.free(f);

    var pipeline = transcription.TranscriptionPipeline.init(
        allocator,
        model.session,
        model.session,
        model.getTokenizer(),
        .{
            .max_length = 448,
            .decoder_start_token_id = whisper_cfg.decoder_start_token_id,
            .eos_token_id = whisper_cfg.eos_token_id,
            .language = opts.language,
            .forced_decoder_ids = forced_ids,
        },
    );

    var result = try pipeline.transcribe(audio_data);
    defer result.deinit();
    try writeResultJson(allocator, opts.model_dir, result.text, result.language);
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) {
        printUsage();
        return error.InvalidArguments;
    }

    var opts = Options{
        .model_dir = args[0],
        .audio_path = args[1],
    };

    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--language")) {
            i += 1;
            if (i >= args.len) return error.MissingLanguageValue;
            opts.language = args[i];
        } else {
            printUsage();
            return error.InvalidArguments;
        }
    }

    return opts;
}

fn writeResultJson(allocator: std.mem.Allocator, model_name: []const u8, text: []const u8, language: ?[]const u8) !void {
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);

    try buf.appendSlice(allocator, "{\"model\":");
    try jsonEncodeString(&buf, allocator, model_name);
    try buf.appendSlice(allocator, ",\"text\":");
    try jsonEncodeString(&buf, allocator, text);
    if (language) |lang| {
        try buf.appendSlice(allocator, ",\"language\":");
        try jsonEncodeString(&buf, allocator, lang);
    }
    try buf.appendSlice(allocator, "}\n");

    print("{s}", .{buf.items});
}

fn loadForcedDecoderIds(allocator: std.mem.Allocator, model_dir: []const u8) ?[]const [2]i32 {
    const path = std.fmt.allocPrint(allocator, "{s}/generation_config.json", .{model_dir}) catch return null;
    defer allocator.free(path);

    const data = c_file.readFile(allocator, path) catch return null;
    defer allocator.free(data);

    const key = "\"forced_decoder_ids\"";
    const key_pos = std.mem.indexOf(u8, data, key) orelse return null;
    const after_key = data[key_pos + key.len ..];

    var pos: usize = 0;
    while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ':' or after_key[pos] == '\n' or after_key[pos] == '\r' or after_key[pos] == '\t')) pos += 1;
    if (pos >= after_key.len or after_key[pos] == 'n') return null;
    if (after_key[pos] != '[') return null;
    pos += 1;

    var result = std.ArrayListUnmanaged([2]i32).empty;
    while (pos < after_key.len) {
        while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ',' or after_key[pos] == '\n' or after_key[pos] == '\r' or after_key[pos] == '\t')) pos += 1;
        if (pos >= after_key.len or after_key[pos] == ']') break;
        if (after_key[pos] != '[') break;
        pos += 1;

        while (pos < after_key.len and after_key[pos] == ' ') pos += 1;
        const first = parseJsonInt(after_key[pos..]) orelse break;
        pos += first.len;

        while (pos < after_key.len and (after_key[pos] == ' ' or after_key[pos] == ',')) pos += 1;
        const second = parseJsonInt(after_key[pos..]) orelse break;
        pos += second.len;

        while (pos < after_key.len and after_key[pos] != ']') pos += 1;
        if (pos < after_key.len) pos += 1;

        result.append(allocator, .{ @intCast(first.value), @intCast(second.value) }) catch return null;
    }

    if (result.items.len == 0) {
        result.deinit(allocator);
        return null;
    }
    return result.toOwnedSlice(allocator) catch null;
}

fn loadWhisperForcedDecoderIds(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    tok: tokenizer_mod.Tokenizer,
    language: ?[]const u8,
) ?[]const [2]i32 {
    if (loadForcedDecoderIds(allocator, model_dir)) |ids| return ids;
    return inferWhisperForcedDecoderIds(allocator, tok, language);
}

fn inferWhisperForcedDecoderIds(
    allocator: std.mem.Allocator,
    tok: tokenizer_mod.Tokenizer,
    language: ?[]const u8,
) ?[]const [2]i32 {
    const transcribe_id = encodeSingleSpecialToken(allocator, tok, "<|transcribe|>") orelse return null;
    const no_timestamps_id = encodeSingleSpecialToken(allocator, tok, "<|notimestamps|>") orelse return null;

    var result = std.ArrayListUnmanaged([2]i32).empty;
    errdefer result.deinit(allocator);

    var next_pos: i32 = 1;
    if (language) |lang| {
        const lang_token = std.fmt.allocPrint(allocator, "<|{s}|>", .{lang}) catch return null;
        defer allocator.free(lang_token);
        const lang_id = encodeSingleSpecialToken(allocator, tok, lang_token) orelse return null;
        result.append(allocator, .{ next_pos, lang_id }) catch return null;
        next_pos += 1;
    }

    result.append(allocator, .{ next_pos, transcribe_id }) catch return null;
    result.append(allocator, .{ next_pos + 1, no_timestamps_id }) catch return null;
    return result.toOwnedSlice(allocator) catch null;
}

fn encodeSingleSpecialToken(
    allocator: std.mem.Allocator,
    tok: tokenizer_mod.Tokenizer,
    text: []const u8,
) ?i32 {
    const ids = tok.encode(allocator, text) catch return null;
    defer allocator.free(ids);
    if (ids.len != 1) return null;
    return ids[0];
}

const ParsedInt = struct { value: i64, len: usize };

fn parseJsonInt(s: []const u8) ?ParsedInt {
    if (s.len == 0) return null;
    var i: usize = 0;
    var neg = false;
    if (s[0] == '-') {
        neg = true;
        i = 1;
    }
    if (i >= s.len or s[i] < '0' or s[i] > '9') return null;
    var val: i64 = 0;
    while (i < s.len and s[i] >= '0' and s[i] <= '9') {
        val = val * 10 + @as(i64, s[i] - '0');
        i += 1;
    }
    return .{ .value = if (neg) -val else val, .len = i };
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
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    session_manager.preferred_backends = switch (choice) {
        .auto => if (build_options.enable_metal)
            &.{ backends.BackendType.metal, backends.BackendType.native }
        else
            &.{backends.BackendType.native},
        .native => &.{backends.BackendType.native},
        .metal => if (build_options.enable_metal) &.{backends.BackendType.metal} else &.{backends.BackendType.native},
    };
}

fn ensureRequestedMetalHostedBackendAvailable(choice: BackendChoice) !void {
    if (choice != .metal) return;
    if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
    }
}

fn printUsage() void {
    print(
        \\usage: termite transcribe <model-dir> <audio.wav> [--backend auto|native|metal] [--language <lang>]
        \\  Runs local audio transcription and prints a JSON response to stdout.
        \\
    , .{});
}

test "parseArgs accepts backend and language" {
    const opts = try parseArgs(&.{
        "/tmp/model",
        "/tmp/audio.wav",
        "--backend",
        "metal",
        "--language",
        "en",
    });

    try std.testing.expectEqualStrings("/tmp/model", opts.model_dir);
    try std.testing.expectEqualStrings("/tmp/audio.wav", opts.audio_path);
    try std.testing.expectEqual(BackendChoice.metal, opts.backend);
    try std.testing.expectEqualStrings("en", opts.language.?);
}
