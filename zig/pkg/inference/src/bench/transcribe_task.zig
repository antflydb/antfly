// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Benchmark-only public transcription task: encoded audio, existing bounded
//! preprocessing and loaded prompt/tokenizer assets, through real token output.
const std = @import("std");
const inference = @import("inference_internal");
const memory = inference.runtime.tier.memory;
const Control = inference.execution_control.InferenceExecutionControl;

fn transient(amounts: memory.AdmissionAmounts) memory.AdmissionAmounts {
    var result = amounts;
    result.host_weight_bytes = 0;
    result.backend_weight_bytes = 0;
    return result;
}
fn waitRetired(controller: *memory.AdmissionController, initial: memory.AdmissionAmounts, control: Control) !void {
    while (!std.meta.eql(transient(controller.snapshot()), transient(initial))) {
        try control.check();
        inference.platform.time.sleepNs(100 * std.time.ns_per_us);
    }
}

fn actualCudaStatsPolicy(session: anytype) !?bool {
    if (comptime !@import("build_options").enable_cuda) return null;
    if (session.backend() != .cuda) return null;
    const binding = session.invocation_diagnostics orelse return error.TranscriptionPolicyUnavailable;
    const stable = binding.stable_backend orelse return error.TranscriptionPolicyUnavailable;
    const Cuda = inference.native_compute.cuda;
    // Certify the actual canonical owner before casting; all fields read here
    // are immutable construction/publication metadata pinned by model lifetime.
    if (Cuda.foreignTensorCopyTarget(&stable) == null) return error.TranscriptionPolicyUnavailable;
    const actual: *Cuda.CudaCompute = @ptrCast(@alignCast(stable.ptr));
    return actual.row_selection_stats_enabled;
}

const Transcription = inference.pipelines.transcription;

// Detach only public response data for report/output publication, before its
// request arena dies. This bounded publication copy belongs in task time;
// JSON formatting/file/log I/O occurs after terminal cleanup and is excluded.
fn publicationCopy(a: std.mem.Allocator, value: Transcription.TranscribeResult) !Transcription.TranscribeResult {
    var held = value;
    held.allocator = a;
    held.text = try a.dupe(u8, value.text);
    errdefer a.free(held.text);
    held.language = if (value.language) |language| try a.dupe(u8, language) else null;
    errdefer if (held.language) |language| a.free(language);
    held.tokens = try a.dupe(i32, value.tokens);
    errdefer a.free(held.tokens);
    const segments = try a.alloc(Transcription.TimedSegment, value.segments.len);
    errdefer a.free(segments);
    var built: usize = 0;
    errdefer for (segments[0..built]) |segment| a.free(segment.text);
    for (value.segments, segments) |segment, *copy| {
        copy.* = segment;
        copy.text = try a.dupe(u8, segment.text);
        built += 1;
    }
    held.segments = segments;
    return held;
}

fn parityVector(a: std.mem.Allocator, value: *const Transcription.TranscribeResult, timestamps: bool) ![]f32 {
    var values = std.ArrayListUnmanaged(f32).empty;
    errdefer values.deinit(a);
    const Append = struct {
        fn number(allocator: std.mem.Allocator, output: *std.ArrayListUnmanaged(f32), integer: u64) !void {
            if (integer > 16_777_216) return error.TranscriptionParityIntegerOutOfRange;
            try output.append(allocator, @floatFromInt(integer));
        }
        fn bytes(allocator: std.mem.Allocator, output: *std.ArrayListUnmanaged(f32), data: []const u8) !void {
            try number(allocator, output, data.len);
            for (data) |byte| try number(allocator, output, byte);
        }
    };
    try Append.number(a, &values, 1); // stable parity schema
    try Append.number(a, &values, @intFromBool(timestamps));
    try Append.number(a, &values, @backingInt(value.termination));
    try Append.number(a, &values, @backingInt(value.selected_attempt_termination));
    try Append.number(a, &values, @intFromBool(value.silent));
    if (!std.math.isFinite(value.temperature)) return error.InvalidTranscriptionOutput;
    try values.append(a, value.temperature); // exact selected retry policy
    try Append.bytes(a, &values, value.text);
    try Append.bytes(a, &values, value.language orelse "");
    try Append.number(a, &values, value.tokens.len);
    for (value.tokens) |token| {
        if (token < 0) return error.InvalidTranscriptionOutput;
        try Append.number(a, &values, @intCast(token));
    }
    try Append.number(a, &values, value.segments.len);
    for (value.segments) |segment| {
        try Append.number(a, &values, segment.start_ms);
        try Append.number(a, &values, segment.end_ms);
        try Append.bytes(a, &values, segment.text);
    }
    return values.toOwnedSlice(a);
}

fn validatePublication(value: *const Transcription.TranscribeResult, vocab: usize, eos: i32, timestamp_begin: i32, timestamps: bool, window_ms: u64, require_eos: bool) !void {
    // This result field comes from actual EOT branches, never stripped tokens.
    if (require_eos and value.selected_attempt_termination != .eos) return error.TranscriptionDidNotReachEos;
    if (!std.math.isFinite(value.avg_logprob) or !std.math.isFinite(value.no_speech_prob)) return error.InvalidTranscriptionOutput;
    for (value.tokens) |token| {
        if (token < 0 or @as(u64, @intCast(token)) >= vocab or token == eos) return error.InvalidTranscriptionOutput;
        if (timestamps and token >= timestamp_begin) return error.PublicTextContainsTimestampToken;
    }
    if (!timestamps and value.segments.len != 0) return error.UnexpectedTimestampSegments;
    var previous_start: u64 = 0;
    for (value.segments) |segment| {
        if (segment.start_ms > segment.end_ms or segment.end_ms > window_ms or segment.start_ms < previous_start) return error.InvalidTimestampSegmentBounds;
        previous_start = segment.start_ms;
    }
}

fn writeSamples(a: std.mem.Allocator, samples: []const u64) !void {
    if (samples.len == 0) return;
    const sorted = try a.dupe(u64, samples);
    defer a.free(sorted);
    std.mem.sort(u64, sorted, {}, std.sort.asc(u64));
    std.debug.print("transcription_task_metric name=complete_task_lifecycle count={d} p50_ns={d} p95_ns={d} min_ns={d} terminal_quota=baseline request_arena=destroyed\n", .{ sorted.len, sorted[sorted.len / 2], sorted[@min(sorted.len - 1, sorted.len * 95 / 100)], sorted[0] });
}

pub fn run(init: std.process.Init, model: anytype, allocator: std.mem.Allocator, options: anytype) !void {
    const session = model.session;
    const actual_backend = session.backend();
    if (!std.mem.eql(u8, @tagName(actual_backend), @tagName(options.backend))) return error.TranscriptionBackendSelectionMismatch;
    const config = inference.execution.architecture_session.getWhisperConfig(session) orelse return error.WhisperModelRequired;
    const stats_policy = try actualCudaStatsPolicy(session);
    std.debug.print("transcription_task_policy backend={s} cuda_row_selection_stats={s} source=cached_compute_construction\n", .{ @tagName(actual_backend), if (stats_policy) |enabled| if (enabled) "device" else "reference" else "not_applicable" });
    const prompt = if (model.whisper_prompt_cache) |*cache| cache else return error.WhisperPromptCacheRequired;
    const lifetime = session.model_lifetime orelse return error.TranscriptionModelLifetimeRequired;
    const domain = if (lifetime.admission_domain) |pin| pin.retain() else return error.TranscriptionAdmissionRequired;
    defer domain.release();
    const watchdog = try inference.HardCancellationWatchdog.create(allocator);
    defer watchdog.destroy();
    try watchdog.start(init.io);
    _ = session.configureExecutionDiagnostics(options.diagnostics);
    const max_output_tokens: usize = config.max_target_positions;
    if (max_output_tokens == 0) return error.InvalidTranscriptionContext;
    const lifecycle_samples = try allocator.alloc(u64, options.iterations);
    defer allocator.free(lifecycle_samples);
    for (0..options.warmup + options.iterations) |iteration| {
        const lifecycle_start = inference.platform.time.monotonicNs();
        const control = Control{ .io = init.io, .deadline_ns = inference.platform.time.monotonicNs() + 120 * std.time.ns_per_s, .hard_cancellation = watchdog.boundary(), .native_call_policy = .require_supervision, .cancellation_grace_ns = std.time.ns_per_s };
        const initial = domain.controller().snapshot();
        var arena = std.heap.ArenaAllocator.init(allocator);
        var arena_open = true;
        defer if (arena_open) arena.deinit();
        const a = arena.allocator();
        const audio_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, options.transcribe_audio, a, .limited(32 * 1024 * 1024));
        var forced_scratch: [3]@typeInfo(@TypeOf(prompt.automatic_ids)).pointer.child = undefined;
        const forced_ids = try prompt.resolveWithTimestamps(&forced_scratch, null, options.transcribe_timestamps);
        var pipeline = inference.pipelines.transcription.TranscriptionPipeline.init(a, session, session, model.getTokenizer(), .{
            .max_length = config.max_target_positions,
            .max_output_tokens = max_output_tokens,
            .decoder_start_token_id = config.decoder_start_token_id,
            .vocab_size = config.vocab_size,
            .eos_token_id = config.eos_token_id,
            .n_mels = config.num_mel_bins,
            .forced_decoder_ids = forced_ids,
            .language_tokens = prompt.language_tokens,
            .decode = prompt.decode,
            .no_timestamps_id = prompt.no_timestamps_id,
            .timestamps = options.transcribe_timestamps,
            .max_decode_working_bytes = 64 * 1024 * 1024,
            .audio_context = .full,
        });
        var pipeline_open = true;
        defer if (pipeline_open) pipeline.deinit();
        // This offline task owns only model lifetime pins, with no enclosing
        // numerical lane, matching reviewed public server factories.
        pipeline.incremental_cleanup_contract = .no_enclosing_lanes;
        pipeline.execution_control = control;
        try control.check();
        const start = inference.platform.time.monotonicNs();
        var decoded = try inference.pipelines.audio.decodeBounded(a, audio_bytes, .{}, pipeline.config.max_decode_working_bytes);
        var decoded_open = true;
        defer if (decoded_open) decoded.deinit();
        // Use the public single-window task honestly; do not silently discard
        // the tail of a longer recording. Existing long-transcription owns it.
        if (decoded.sample_rate == 0 or decoded.samples.len > try std.math.mul(usize, decoded.sample_rate, pipeline.config.chunk_length_s)) return error.TranscriptionAudioExceedsSingleWindow;
        try control.check();
        const audio_samples = decoded.samples.len;
        const sample_rate = decoded.sample_rate;
        var result = try pipeline.transcribePcm(decoded.samples, decoded.sample_rate);
        var result_open = true;
        defer if (result_open) result.deinit();
        const returned = inference.platform.time.monotonicNs();
        if (!result.timing.kv_cached) return error.ActualIncrementalDecoderRequired;
        if (!std.math.isFinite(result.avg_logprob) or !std.math.isFinite(result.no_speech_prob)) return error.InvalidTranscriptionOutput;
        const duration_ms = @as(u64, @intCast(audio_samples)) * 1000 / sample_rate;
        const window_ms = @as(u64, @intCast(pipeline.config.chunk_length_s)) * 1000;
        try validatePublication(&result, config.vocab_size, config.eos_token_id, prompt.decode.timestamp_begin_id, options.transcribe_timestamps, window_ms, options.transcribe_require_eos);
        var publication = try publicationCopy(allocator, result);
        defer publication.deinit();
        result.deinit();
        result_open = false;
        decoded.deinit();
        decoded_open = false;
        pipeline.deinit();
        pipeline_open = false;
        arena.deinit();
        arena_open = false;
        try waitRetired(domain.controller(), initial, control);
        const retired = inference.platform.time.monotonicNs();
        const record = try std.json.Stringify.valueAlloc(allocator, .{
            .schema_version = 1,
            .task = "public_transcription",
            .requested_backend = @tagName(options.backend),
            .selected_backend = @tagName(actual_backend),
            .encoder_backend = @tagName(actual_backend),
            .decoder_backend = @tagName(actual_backend),
            .fixture = @tagName(options.source_fixture_label),
            .audio_path = options.transcribe_audio,
            .audio_samples = audio_samples,
            .sample_rate = sample_rate,
            .width = config.d_model,
            .encoder_layers = config.encoder_layers,
            .decoder_layers = config.decoder_layers,
            .vocab_size = config.vocab_size,
            .max_length = config.max_target_positions,
            .max_output_tokens = max_output_tokens,
            .configured_eos_token_id = config.eos_token_id,
            .automatic_language = true,
            .configured_warmup_iterations = options.warmup,
            .configured_measured_iterations = options.iterations,
            .persistent_model_load_in_lifecycle = false,
            .cuda_row_selection_stats_policy = if (stats_policy) |enabled| if (enabled) "device" else "reference" else "not_applicable",
            .cuda_row_selection_stats_policy_scope = "cached_actual_compute_construction_not_per_step_dispatch_proof",
            .encoder_input_frames = inference.pipelines.audio.whisperFramesForSeconds(inference.pipelines.audio.WHISPER_CHUNK_LENGTH),
            .mel_bins = config.num_mel_bins,
            .timestamps_requested = options.transcribe_timestamps,
            .require_eos = options.transcribe_require_eos,
            .audio_duration_ms = duration_ms,
            .timestamp_window_bound_ms = window_ms,
            .timestamp_begin_id = prompt.decode.timestamp_begin_id,
            .timestamp_scope = "single_window_public_segments_ms_text_tokens_strip_timestamps",
            .termination = @tagName(publication.termination),
            .selected_attempt_termination = @tagName(publication.selected_attempt_termination),
            .eos_observed = publication.selected_attempt_termination == .eos,
            .segments = publication.segments,
            .compression_ratio = publication.compression_ratio,
            .output_format = if (options.transcribe_output_format == .json) "json" else "parity-f32",
            .parity_scope = "exact_text_language_utf8_tokens_segments_termination_excludes_timings_and_approx_probabilities",
            .complete_task_lifecycle_ns = retired - lifecycle_start,
            .complete_task_lifecycle_scope = "encoded_file_read_audio_preprocess_admission_public_pipeline_actual_selected_termination_publication_copy_result_pcm_pipeline_arena_terminal_quota",
            .report_serialization_and_output_file_io_in_lifecycle = false,
            .encoded_file_read_in_lifecycle = true,
            .retained_report_publication_outlives_task = true,
            .diagnostic_observation = options.diagnostics,
            .language = publication.language,
            .text = publication.text,
            .selected_text_tokens = publication.tokens,
            .token_list_contract = "public result excludes EOS and prompt tokens",
            .silent = publication.silent,
            .temperature = publication.temperature,
            .avg_logprob = publication.avg_logprob,
            .no_speech_prob = publication.no_speech_prob,
            .timing = publication.timing,
            .encoded_audio_and_task_ns = returned - start,
        }, .{});
        defer allocator.free(record);
        if (iteration >= options.warmup) {
            const index = iteration - options.warmup;
            lifecycle_samples[index] = retired - lifecycle_start;
            std.debug.print("transcription_task sample={d} result={s}\n", .{ index, record });
            std.debug.print("transcription_task_metric sample={d} task_ns={d} terminal_ns={d} terminal_quota=baseline request_arena=destroyed\n", .{ index, returned - start, retired - start });
            if (options.output) |path| if (index + 1 == options.iterations) {
                if (options.transcribe_output_format == .json) try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = record }) else {
                    const vector = try parityVector(allocator, &publication, options.transcribe_timestamps);
                    defer allocator.free(vector);
                    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = path, .data = std.mem.sliceAsBytes(vector) });
                }
            };
        }
    }
    if (!options.diagnostics) try writeSamples(allocator, lifecycle_samples);
}

test "transcription qualification publication ownership unwinds every allocation and parity excludes approximate values" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const borrowed = Transcription.TranscribeResult{ .allocator = a, .text = "hola", .language = "es", .tokens = &.{ 12, 34 }, .segments = &.{.{ .text = "hola", .start_ms = 0, .end_ms = 1000 }}, .termination = .eos, .selected_attempt_termination = .eos };
            var first = try publicationCopy(a, borrowed);
            defer first.deinit();
            const vector = try parityVector(a, &first, true);
            defer a.free(vector);
            first.avg_logprob = -0.125;
            first.no_speech_prob = 0.5;
            first.timing.decode_ns = 1234;
            const other = try parityVector(a, &first, true);
            defer a.free(other);
            try std.testing.expectEqualSlices(f32, vector, other);
            try validatePublication(&first, 51865, 50257, 50364, true, 30000, true);
            first.selected_attempt_termination = .context_limit;
            try std.testing.expectError(error.TranscriptionDidNotReachEos, validatePublication(&first, 51865, 50257, 50364, true, 30000, true));
            try validatePublication(&first, 51865, 50257, 50364, true, 30000, false);
            const truncated = try parityVector(a, &first, true);
            defer a.free(truncated);
            try std.testing.expect(!std.mem.eql(f32, vector, truncated));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
