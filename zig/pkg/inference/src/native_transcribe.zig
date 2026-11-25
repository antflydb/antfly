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
const whisper_prompt = @import("pipelines/whisper_prompt.zig");
const enc_dec_mod = @import("pipelines/encoder_decoder.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");

const print = std.debug.print;

const BackendChoice = enum {
    auto,
    native,
    metal,
    cuda,
    xla,
};

const Options = struct {
    model_dir: []const u8,
    audio_path: []const u8,
    backend: BackendChoice = .auto,
    pjrt: @import("execution/pjrt_options.zig").Options = .{},
    /// Backend for the decoder session. `auto` keeps the decoder on the
    /// primary session; `native` loads a CPU copy for the decoder so the
    /// two placements can be measured against each other.
    decoder_backend: BackendChoice = .auto,
    language: ?[]const u8 = null,
};

pub fn main(allocator: std.mem.Allocator, io: std.Io, args: []const []const u8) !void {
    const opts = try parseArgs(args);
    try opts.pjrt.validateTask(.transcription);
    try ensureRequestedBackendAvailable(opts.backend);
    const audio_data = try c_file.readFile(allocator, opts.audio_path);
    defer allocator.free(audio_data);

    var session_manager = backends.SessionManager.initWithIo(allocator, io);
    // A one-shot CLI retains sessions and inputs until native calls finish,
    // matching the direct embedding command's explicit lifetime policy.
    session_manager.native_call_policy = .in_process;
    session_manager.process_termination_available = false;
    configureBackendPreference(&session_manager, opts.backend);
    try opts.pjrt.applyPolicy(&session_manager);
    if (opts.backend == .xla) try opts.pjrt.configure(&session_manager);
    var decoder_manager_snapshot = session_manager;
    if (opts.decoder_backend != .auto) {
        try ensureRequestedBackendAvailable(opts.decoder_backend);
        const choice = @import("native_backend_choice.zig").parse(@tagName(opts.decoder_backend)).?;
        @import("native_backend_choice.zig").configureDirectSession(&decoder_manager_snapshot, choice);
    }

    // Transcription task controls describe the decoder, independently of
    // encoder placement. A native decoder must not ignore a PJRT cache flag.
    try opts.pjrt.validateTaskPlacement(decoder_manager_snapshot.required_backend == .pjrt);

    if (enc_dec_mod.findEncoderDecoderPaths(allocator, opts.model_dir)) |paths| {
        defer allocator.free(paths.encoder);
        defer allocator.free(paths.decoder);

        var encoder_session = try session_manager.loadModel(paths.encoder);
        defer encoder_session.close();

        var decoder_session = try decoder_manager_snapshot.loadModel(paths.decoder);
        defer decoder_session.close();

        const tok_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{opts.model_dir});
        defer allocator.free(tok_path);
        const tok_bytes = try c_file.readFile(allocator, tok_path);
        defer allocator.free(tok_bytes);

        var hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tok_bytes);
        defer hf_tok.deinitSelf();

        const dec_config = enc_dec_mod.loadDecoderConfig(allocator, opts.model_dir) catch enc_dec_mod.DecoderConfig{};
        var prompt_cache = try whisper_prompt.PromptCache.init(allocator, opts.model_dir, hf_tok.tokenizer());
        defer prompt_cache.deinit();
        var prompt_scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
        const forced_ids = try prompt_cache.resolve(&prompt_scratch, opts.language);

        var pipeline = transcription.TranscriptionPipeline.init(
            allocator,
            encoder_session,
            decoder_session,
            hf_tok.tokenizer(),
            .{
                .pjrt_decoder_cache = opts.pjrt.decoder_cache,
                .max_length = dec_config.max_length,
                .decoder_start_token_id = dec_config.decoder_start_token_id,
                .eos_token_id = dec_config.eos_token_id,
                .language = opts.language,
                .forced_decoder_ids = forced_ids,
                .language_tokens = prompt_cache.language_tokens,
            },
        );

        pipeline.execution_control = .{ .io = io, .native_call_policy = .in_process };
        var result = try pipeline.transcribe(audio_data);
        defer result.deinit();
        try writeResultJson(allocator, opts.model_dir, result.text, result.language, result.timing);
        return;
    } else |_| {}

    var model_manager = model_manager_mod.ModelManager.init(allocator, session_manager);
    defer model_manager.deinit();
    var decoder_manager = model_manager_mod.ModelManager.init(allocator, decoder_manager_snapshot);
    defer decoder_manager.deinit();

    const model = try model_manager.loadFromDir(opts.model_dir);
    const whisper_cfg = session_factory.getWhisperConfig(model.session) orelse return error.InvalidModelForTranscription;
    const decoder_session: backends.Session = blk: {
        if (opts.decoder_backend == .auto) break :blk model.session;
        try ensureRequestedBackendAvailable(opts.decoder_backend);
        const desired: backends.BackendType = switch (opts.decoder_backend) {
            .native => .native,
            .metal => .metal,
            .cuda => .cuda,
            .xla => .pjrt,
            .auto => unreachable,
        };
        if (model.session.backend() == desired) break :blk model.session;
        const decoder_model = try decoder_manager.loadFromDirWithPreferredBackends(opts.model_dir, &.{desired}, false);
        if (decoder_model.session.backend() != desired) return error.RequiredBackendUnavailable;
        break :blk decoder_model.session;
    };
    const prompt_cache = if (model.whisper_prompt_cache) |*cache|
        cache
    else
        return error.InvalidWhisperDecoderConfig;
    var prompt_scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
    const forced_ids = try prompt_cache.resolve(&prompt_scratch, opts.language);

    var pipeline = transcription.TranscriptionPipeline.init(
        allocator,
        model.session,
        decoder_session,
        model.getTokenizer(),
        .{
            .pjrt_decoder_cache = opts.pjrt.decoder_cache,
            .vocab_size = whisper_cfg.vocab_size,
            .max_length = @intCast(whisper_cfg.max_target_positions),
            .decoder_start_token_id = whisper_cfg.decoder_start_token_id,
            .eos_token_id = whisper_cfg.eos_token_id,
            .n_mels = whisper_cfg.num_mel_bins,
            .language = opts.language,
            .forced_decoder_ids = forced_ids,
            .language_tokens = prompt_cache.language_tokens,
            .decode = prompt_cache.decode,
            .no_timestamps_id = prompt_cache.no_timestamps_id,
        },
    );

    pipeline.execution_control = .{ .io = io, .native_call_policy = .in_process };
    var result = try pipeline.transcribe(audio_data);
    defer result.deinit();
    try writeResultJson(allocator, opts.model_dir, result.text, result.language, result.timing);
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
        if (std.mem.startsWith(u8, arg, "--pjrt-")) {
            i += 1;
            if (i >= args.len) return error.MissingArgument;
            if (!try opts.pjrt.parse(arg, args[i])) return error.InvalidArguments;
        } else if (std.mem.eql(u8, arg, "--backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
        } else if (std.mem.eql(u8, arg, "--decoder-backend")) {
            i += 1;
            if (i >= args.len) return error.MissingBackendValue;
            opts.decoder_backend = parseBackendChoice(args[i]) orelse return error.InvalidBackend;
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

fn writeResultJson(allocator: std.mem.Allocator, model_name: []const u8, text: []const u8, language: ?[]const u8, timing: transcription.Timing) !void {
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
    const timing_json = try std.fmt.allocPrint(
        allocator,
        ",\"timing_ms\":{{\"mel\":{d:.1},\"encoder\":{d:.1},\"prefill\":{d:.1},\"decode\":{d:.1},\"decode_steps\":{d},\"kv_cached\":{}}}",
        .{
            @as(f64, @floatFromInt(timing.mel_ns)) / 1e6,
            @as(f64, @floatFromInt(timing.encoder_ns)) / 1e6,
            @as(f64, @floatFromInt(timing.prefill_ns)) / 1e6,
            @as(f64, @floatFromInt(timing.decode_ns)) / 1e6,
            timing.decode_steps,
            timing.kv_cached,
        },
    );
    defer allocator.free(timing_json);
    try buf.appendSlice(allocator, timing_json);
    try buf.appendSlice(allocator, "}\n");

    print("{s}", .{buf.items});
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
    if (std.mem.eql(u8, value, "xla") or std.mem.eql(u8, value, "pjrt")) return .xla;
    if (std.mem.eql(u8, value, "auto")) return .auto;
    if (std.mem.eql(u8, value, "native")) return .native;
    if (std.mem.eql(u8, value, "metal")) return .metal;
    if (std.mem.eql(u8, value, "cuda")) return .cuda;
    return null;
}

fn configureBackendPreference(session_manager: *backends.SessionManager, choice: BackendChoice) void {
    if (choice != .auto) {
        @import("native_backend_choice.zig").configureDirectSession(session_manager, @import("native_backend_choice.zig").parse(@tagName(choice)).?);
        return;
    }
    session_manager.preferred_backends = if (build_options.enable_metal)
        &.{ backends.BackendType.metal, backends.BackendType.native }
    else if (build_options.enable_cuda)
        &.{ backends.BackendType.cuda, backends.BackendType.native }
    else
        &.{backends.BackendType.native};
}

fn ensureRequestedBackendAvailable(choice: BackendChoice) !void {
    if (choice == .xla and !build_options.enable_pjrt) return error.PjrtNotEnabled;
    if (choice == .cuda and !build_options.enable_cuda) return error.CudaNotEnabled;
    if (choice != .metal) return;
    if (native_backend_guard.checkMetal(build_options.enable_metal, metal_runtime.metalDeviceAvailable())) |failure| {
        native_backend_guard.printFailure(failure);
        return native_backend_guard.raise(failure);
    }
}

fn printUsage() void {
    print(
        \\usage: antfly inference transcribe <model-dir> <audio.wav> [--backend auto|native|metal|cuda|xla] [--decoder-backend auto|native|metal|cuda|xla] [--language <lang>]
        \\  Runs local audio transcription and prints a JSON response to stdout.
        \\
    , .{});
    print("{s}\n", .{@import("execution/pjrt_options.zig").usage});
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

test "transcription CLI explicitly selects CUDA without CPU fallback" {
    const opts = try parseArgs(&.{ "model", "audio.wav", "--backend", "cuda" });
    try std.testing.expectEqual(BackendChoice.cuda, opts.backend);
    var manager = backends.SessionManager.init(std.testing.allocator);
    configureBackendPreference(&manager, .cuda);
    try std.testing.expectEqualSlices(backends.BackendType, &.{.cuda}, manager.preferred_backends);
}

test "Whisper Small Metal checkpoint runs assigned packed prefill" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const model_dir = std.c.getenv("ANTFLY_WHISPER_METAL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var sessions = backends.SessionManager.initWithIo(a, std.testing.io);
    sessions.native_call_policy = .in_process;
    sessions.process_termination_available = false;
    configureBackendPreference(&sessions, .metal);
    var manager = model_manager_mod.ModelManager.init(a, sessions);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    const model = try manager.loadFromDir(std.mem.span(model_dir));
    try std.testing.expectEqual(backends.BackendType.metal, model.session.backend());
    const cfg = session_factory.getWhisperConfig(model.session) orelse return error.InvalidModelForTranscription;
    const hidden = try a.alloc(f32, 8 * cfg.d_model);
    defer a.free(hidden);
    for (hidden, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index % 17)) / 64;
    const Adapter = @import("execution/architecture_session.zig").IncrementalDecoderAdapter;
    const control = @import("execution_control.zig").InferenceExecutionControl{ .io = std.testing.io, .native_call_policy = .in_process };
    const tokens = [_]i64{ cfg.decoder_start_token_id, 50259, 50359, 50363 };
    {
        const frames = 16;
        const mel = try a.alloc(f32, cfg.num_mel_bins * frames);
        defer a.free(mel);
        @memset(mel, 0);
        var features = try backends.Tensor.initFloat32(a, "input_features", &.{ 1, @intCast(cfg.num_mel_bins), frames }, mel);
        defer features.deinit();
        var outputs = (try model.session.runResident(&.{features}, a)) orelse return error.MissingWhisperEncoderOutput;
        defer outputs.deinit();
        const receiver = Adapter.qualify(model.session, model.session) orelse return error.MissingWhisperReceiver;
        const shape = try outputs.backend.tensorShape(outputs.outputs[0], a);
        defer a.free(shape);
        try std.testing.expectEqual(@as(usize, 3), shape.len);
        var decoder = try Adapter.fromResidentPrepared(model.session, a, control, .{
            .receiver = receiver,
            .value = outputs.outputs[0],
            .backend = outputs.backend,
            .shape = .{ shape[0], shape[1], shape[2] },
            .resources = try outputs.retainResources(a),
        }, .{ .expected_initial_query_rows = tokens.len });
        defer decoder.deinit();
        const program = decoder.cache.native_program orelse return error.MissingPreparedWhisperSchedule;
        const initial = program.initial orelse return error.MissingPreparedWhisperInitialSchedule;
        try std.testing.expectEqual(cfg.decoder_layers, initial.schedule.selected.pool.regions.len);
        for (initial.schedule.selected.pool.regions) |region| try std.testing.expect(region.owner.requirements.dispatch_workspace.byte_size != 0);
        const logits = try decoder.step(&tokens);
        defer a.free(logits);
        try std.testing.expectEqual(cfg.vocab_size, logits.len);
        try std.testing.expectEqual(initial.schedule.steps.len, initial.schedule.cursor);
    }
    const expected = blk: {
        var ordinary = (try Adapter.fromHostPrepared(model.session, a, control, hidden, 8, .{})) orelse return error.MissingWhisperDecoder;
        defer ordinary.deinit();
        break :blk try ordinary.step(&tokens);
    };
    defer a.free(expected);
    for (0..3) |_| {
        var decoder = (try Adapter.fromHostPrepared(model.session, a, control, hidden, 8, .{ .expected_initial_query_rows = tokens.len })) orelse return error.MissingWhisperDecoder;
        defer decoder.deinit();
        const program = decoder.cache.native_program orelse return error.MissingPreparedWhisperSchedule;
        const initial = program.initial orelse return error.MissingPreparedWhisperInitialSchedule;
        var packed_regions: usize = 0;
        for (initial.schedule.selected.pool.regions) |region| {
            if (region.owner.requirements.dispatch_workspace.byte_size != 0) packed_regions += 1;
        }
        try std.testing.expectEqual(cfg.decoder_layers, packed_regions);
        const actual = try decoder.step(&tokens);
        defer a.free(actual);
        try std.testing.expectEqual(expected.len, actual.len);
        for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.01);
        try std.testing.expectEqual(initial.schedule.steps.len, initial.schedule.cursor);
    }
}

test "Whisper CUDA checkpoint transcribes English with reusable model state" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const model_dir = std.c.getenv("ANTFLY_WHISPER_CUDA_MODEL_DIR") orelse return error.SkipZigTest;
    const audio_path = std.c.getenv("ANTFLY_WHISPER_CUDA_AUDIO") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var sessions = backends.SessionManager.initWithIo(a, std.testing.io);
    sessions.native_call_policy = .in_process;
    sessions.process_termination_available = false;
    configureBackendPreference(&sessions, .cuda);
    var manager = model_manager_mod.ModelManager.init(a, sessions);
    defer manager.deinit();
    const model = try manager.loadFromDir(std.mem.span(model_dir));
    try std.testing.expectEqual(backends.BackendType.cuda, model.session.backend());
    const cfg = session_factory.getWhisperConfig(model.session).?;
    {
        const whisper = @import("architectures/whisper.zig");
        const hidden = try a.alloc(f32, 2 * cfg.d_model);
        defer a.free(hidden);
        @memset(hidden, 0);
        var decoder = (try session_factory.whisperNativeDecoder(model.session, a, .{ .io = std.testing.io, .native_call_policy = .in_process }, hidden, 2)) orelse return error.TestUnexpectedResult;
        defer decoder.deinit();
        var backend = decoder.cb.backend;
        var prepared = try whisper.PreparedDecoderWeights.init(&backend, a, cfg, true);
        defer prepared.deinit();
        const CancelBetweenLayers = struct {
            fn check(raw: ?*anyopaque) !void {
                const count: *usize = @ptrCast(@alignCast(raw.?));
                count.* += 1;
                if (count.* == 3) return error.Cancelled;
            }
        };
        var count: usize = 0;
        backend.execution_control = .{ .ptr = &count, .check_fn = CancelBetweenLayers.check, .native_call_policy = .in_process };
        try std.testing.expectError(error.Cancelled, whisper.DecodeCache.initPrepared(&backend, a, cfg, decoder.encoder_hidden, 2, &prepared, null));
        try std.testing.expectEqual(cfg.decoder_layers, prepared.weights.layers.len);
        backend.execution_control = null;
        var retry = try whisper.DecodeCache.initPrepared(&backend, a, cfg, decoder.encoder_hidden, 2, &prepared, null);
        defer retry.deinit(&backend);
        try std.testing.expectEqual(cfg.decoder_layers, retry.layers.len);
    }
    const prompt = &model.whisper_prompt_cache.?;
    var scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
    const forced = try prompt.resolve(&scratch, "en");
    var pipeline = transcription.TranscriptionPipeline.init(a, model.session, model.session, model.getTokenizer(), .{
        .vocab_size = cfg.vocab_size,
        .max_length = @intCast(cfg.max_target_positions),
        .decoder_start_token_id = cfg.decoder_start_token_id,
        .eos_token_id = cfg.eos_token_id,
        .n_mels = cfg.num_mel_bins,
        .language = "en",
        .forced_decoder_ids = forced,
        .language_tokens = prompt.language_tokens,
        .decode = prompt.decode,
        .no_timestamps_id = prompt.no_timestamps_id,
        .temperature_fallback = false,
    });
    const audio_data = try c_file.readFile(a, std.mem.span(audio_path));
    defer a.free(audio_data);
    pipeline.execution_control = .{ .io = std.testing.io, .native_call_policy = .in_process };
    for (0..5) |_| {
        var result = try pipeline.transcribe(audio_data);
        defer result.deinit();
        for ([_][]const u8{ "quick", "brown", "fox", "lazy", "dog" }) |word|
            try std.testing.expect(std.mem.indexOf(u8, result.text, word) != null);
        try writeResultJson(a, std.mem.span(model_dir), result.text, result.language, result.timing);
    }
}

test "task CLI accepts explicit PJRT device precision and memory policy" {
    const options = try parseArgs(&.{ "model", "audio.wav", "--backend", "xla", "--pjrt-device", "cuda", "--pjrt-capacity-bytes", "1024", "--pjrt-device-bytes", "4096", "--pjrt-precision", "bf16" });
    try std.testing.expectEqual(BackendChoice.xla, options.backend);
    const policy = try options.pjrt.sessionOptions();
    try std.testing.expect(policy.memory_policy.? == .cuda);
    try std.testing.expectEqual(@as(usize, 1024), policy.memory_policy.?.cuda.capacity_bytes);
    try std.testing.expectEqual(BackendChoice.xla, parseBackendChoice("pjrt").?);
}
