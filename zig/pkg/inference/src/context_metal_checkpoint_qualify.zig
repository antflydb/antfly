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

//! Test-binary-only real HTTP ingress for portable Metal KV qualification.
//! The companion process drives the ordinary generated checkpoint endpoints;
//! this module adds no request field, environment-controlled production gate,
//! or legacy context-policy/profile authority.
const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const backends = @import("backends/backends.zig");
const server_mod = @import("server/server.zig");

const allocator = std.testing.allocator;
const io = std.testing.io;
const default_model_name = "google/gemma-4-E4B-it-qat-q4_0-gguf";

fn validateQualificationModel(name: []const u8) ![]const u8 {
    for ([_][]const u8{
        "google/gemma-4-E2B-it-qat-q4_0-gguf",
        default_model_name,
    }) |allowed| if (std.mem.eql(u8, name, allowed)) return name;
    return error.UnsupportedMetalCheckpointQualificationModel;
}

fn qualificationModel() ![]const u8 {
    return validateQualificationModel(platform.env.getenv("ANTFLY_METAL_CHECKPOINT_MODEL") orelse default_model_name);
}

test "context Metal checkpoint qualification model selection is explicit" {
    try std.testing.expectEqualStrings(default_model_name, try validateQualificationModel(default_model_name));
    const e2b = "google/gemma-4-E2B-it-qat-q4_0-gguf";
    try std.testing.expectEqualStrings(e2b, try validateQualificationModel(e2b));
    try std.testing.expectError(error.UnsupportedMetalCheckpointQualificationModel, validateQualificationModel(""));
    try std.testing.expectError(error.UnsupportedMetalCheckpointQualificationModel, validateQualificationModel("google/gemma-4-26B-A4B-it-qat-q4_0-gguf"));
}
const default_max_lifetime_ns: u64 = 12 * std.time.ns_per_hour;
const maximum_lifetime_seconds: u64 = 48 * 60 * 60 + 5 * 60;
const metal_backends = [_]backends.BackendType{.metal};

/// Admit conservative KV, fitting and mapped-weight reservations together.
/// The collector separately enforces the unchanged 8 GiB process RSS cap;
/// these reservation envelopes do not raise it or the 512 MiB solver bound.
fn qualificationGenerationBudgetOverrides() server_mod.BudgetOverrides {
    return .{
        .host_limit_bytes = 8 * 1024 * 1024 * 1024,
        .backend_limit_bytes = 12 * 1024 * 1024 * 1024,
        .combined_limit_bytes = 16 * 1024 * 1024 * 1024,
        .kv_limit_bytes = 6 * 1024 * 1024 * 1024,
        .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
    };
}

test "context Metal qualification budgets admit both models at 8K and 32K" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const memory = @import("runtime/tier/memory.zig");
    const test_generation = @import("pipelines/generation.zig");
    const GptConfig = @import("models/gpt.zig").Config;
    const Encoding = @import("runtime/kv/attention_match_checkpoint.zig").Encoding;
    const cases = [_]struct { config: GptConfig, weight_bytes: usize }{
        .{ .config = .{
            .family = .gemma,
            .gemma4_channel_protocol = true,
            .hidden_size = 1536,
            .num_hidden_layers = 35,
            .num_attention_heads = 8,
            .num_key_value_heads = 1,
            .attention_head_dim = 256,
            .global_head_dim = 512,
            .sliding_window = 512,
            .sliding_window_pattern = 5,
            .num_kv_shared_layers = 20,
            .ple_hidden_size = 256,
            .vocab_size = 262144,
            .max_position_embeddings = 131072,
        }, .weight_bytes = 3_349_516_256 },
        .{ .config = .{
            .family = .gemma,
            .gemma4_channel_protocol = true,
            .hidden_size = 2560,
            .num_hidden_layers = 42,
            .num_attention_heads = 8,
            .num_key_value_heads = 2,
            .attention_head_dim = 256,
            .global_head_dim = 512,
            .sliding_window = 512,
            .num_kv_shared_layers = 18,
            .ple_hidden_size = 256,
            .vocab_size = 262144,
            .max_position_embeddings = 131072,
        }, .weight_bytes = 5_154_941_280 },
    };
    for (cases) |case| {
        for ([_]usize{ 8192, 32768 }) |context_tokens| {
            const output_tokens: usize = if (context_tokens == 8192) 64 else 256;
            const prompt_tokens = context_tokens - output_tokens;
            for ([_]Encoding{ .f16, .fp8, .rotated4 }) |encoding| {
                const plan = try test_generation.NativeCheckpointPrepare.hostPlan(case.config, prompt_tokens, encoding, true);
                var budget = memory.RunBudget.init(qualificationGenerationBudgetOverrides().apply(.{}));
                try budget.reserveEstimate(.{
                    .prompt_tokens = prompt_tokens,
                    .retained_tokens = prompt_tokens,
                    .kv_bytes = 0,
                    .kv_tier = .backend,
                    .scratch_bytes = plan.reservation_bytes,
                    .scratch_tier = .host,
                });
                const components = [_]memory.GptGenerationBudgetComponent{.{
                    .backend = .metal,
                    .kv_dtype = .f16,
                    .config = case.config,
                }};
                const admitted = try memory.reserveGptGenerationPrefill(&budget, &components, prompt_tokens, output_tokens, 1024);
                // Model admission also accounts for mapped weights. Reserve the
                // larger observed host-weight footprint for either model.
                _ = try budget.tryReserveWeight(.host, 233_979_810);
                _ = try budget.tryReserveWeight(.backend, case.weight_bytes);
                try std.testing.expectEqual(@as(usize, 1024), admitted.max_chunk_rows);
                try std.testing.expect(plan.fit_bytes <= test_generation.NativeCheckpointPrepare.max_transient_host_bytes);
            }
        }
    }
}

fn requiredEnv(comptime name: [:0]const u8) ![]const u8 {
    const value = platform.env.getenv(name) orelse return error.MissingMetalCheckpointQualificationInput;
    if (value.len == 0) return error.InvalidMetalCheckpointQualificationInput;
    return value;
}

fn requireAbsolute(path: []const u8) !void {
    if (!std.fs.path.isAbsolute(path)) return error.InvalidMetalCheckpointQualificationInput;
}

fn qualificationLifetimeNs() !u64 {
    const raw = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_MAX_LIFETIME_SECONDS") orelse
        return default_max_lifetime_ns;
    const seconds = std.fmt.parseInt(u64, raw, 10) catch
        return error.InvalidMetalCheckpointQualificationInput;
    if (seconds == 0 or seconds > maximum_lifetime_seconds)
        return error.InvalidMetalCheckpointQualificationInput;
    return std.math.mul(u64, seconds, std.time.ns_per_s) catch
        error.InvalidMetalCheckpointQualificationInput;
}

fn qualificationMaxConcurrentRequests() !usize {
    const raw = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_MAX_CONCURRENT_REQUESTS") orelse
        return 1;
    const value = std.fmt.parseInt(usize, raw, 10) catch
        return error.InvalidMetalCheckpointQualificationInput;
    if (value == 0 or value > 64) return error.InvalidMetalCheckpointQualificationInput;
    return value;
}

fn requireAbsent(path: []const u8) !void {
    _ = std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    return error.MetalCheckpointQualificationControlExists;
}

fn stopRequested(path: []const u8) !bool {
    const stat = std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    if (stat.kind != .file) return error.InvalidMetalCheckpointQualificationStop;
    return true;
}

fn writeReady(
    path: []const u8,
    port: u16,
    load_generation: u64,
    model_name: []const u8,
    identity: @import("runtime/kv/attention_match_checkpoint.zig").Identity,
) !void {
    if (load_generation == 0) return error.InvalidMetalCheckpointQualificationIdentity;
    try requireAbsent(path);
    const pid = std.posix.system.getpid();
    const temporary = try std.fmt.allocPrint(allocator, "{s}.part-{d}", .{ path, pid });
    defer allocator.free(temporary);
    try requireAbsent(temporary);
    defer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
    const weights = std.fmt.bytesToHex(identity.weights, .lower);
    const tokenizer = std.fmt.bytesToHex(identity.tokenizer, .lower);
    const template = std.fmt.bytesToHex(identity.template, .lower);
    const layout = std.fmt.bytesToHex(identity.layout, .lower);
    const body = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.metal-checkpoint-test-server.v1",
        .backend = "metal",
        .metal_enabled = build_options.enable_metal,
        .cuda_enabled = build_options.enable_cuda,
        .pid = pid,
        .port = port,
        .model = model_name,
        .load_generation = load_generation,
        .loaded_identity = .{
            .weights = weights[0..],
            .tokenizer = tokenizer[0..],
            .template = template[0..],
            .layout = layout[0..],
        },
    }, .{});
    defer allocator.free(body);
    var file = try std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true, .truncate = false });
    var file_open = true;
    defer if (file_open) file.close(io);
    try file.writeStreamingAll(io, body);
    try file.writeStreamingAll(io, "\n");
    try file.sync(io);
    file.close(io);
    file_open = false;
    // The rename is the publication point. Recheck so an existing caller file
    // is never intentionally replaced; the qualification directory is private
    // to its orchestrator and the temporary name is PID-scoped.
    try requireAbsent(path);
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temporary, std.Io.Dir.cwd(), path, io);
}

fn performancePromptCount(model: *server_mod.model_manager.LoadedModel, text: []const u8) !usize {
    const template = model.chat_tmpl orelse return error.MissingChatTemplate;
    const rendered = try template.applyWithOptions(allocator, &.{.{ .role = "user", .content = text }}, .{ .enable_thinking = false });
    defer allocator.free(rendered);
    var encoded = try @import("pipelines/generation.zig").encodeNativeGenerationPrompt(
        model.getTokenizer(),
        allocator,
        rendered,
        131072,
        model.manifest.add_bos_token,
        model.manifest.bos_token,
    );
    defer encoded.deinit();
    var count: usize = 0;
    for (encoded.attention_mask) |mask| {
        if (mask == 0) break;
        count += 1;
    }
    return count;
}

fn sizedPerformancePrompt(model: *server_mod.model_manager.LoadedModel, target: usize) ![]u8 {
    const prefix = "This is a synthetic repeated-token capacity and performance payload. Reference text:";
    const suffix =
        "\nRespond only with the decimal integers from 1 through 10000 in ascending order, separated by single spaces. " ++
        "Continue the sequence without commentary and do not stop early.";
    var low: usize = 0;
    var high = target;
    while (low <= high) {
        const repeats = low + (high - low) / 2;
        const text = try allocator.alloc(u8, prefix.len + repeats * 2 + suffix.len);
        @memcpy(text[0..prefix.len], prefix);
        for (0..repeats) |i| @memcpy(text[prefix.len + i * 2 ..][0..2], " a");
        @memcpy(text[prefix.len + repeats * 2 ..], suffix);
        const count = performancePromptCount(model, text) catch |err| {
            allocator.free(text);
            return err;
        };
        if (count == target) return text;
        allocator.free(text);
        if (count < target) {
            low = repeats + 1;
        } else {
            if (repeats == 0) break;
            high = repeats - 1;
        }
    }
    return error.UnrepresentableMetalCheckpointPerformancePrompt;
}

fn writeExclusive(path: []const u8, data: []const u8) ![32]u8 {
    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true, .truncate = false });
    defer file.close(io);
    try file.writeStreamingAll(io, data);
    try file.writeStreamingAll(io, "\n");
    try file.sync(io);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(data);
    hash.update("\n");
    return hash.finalResult();
}

fn qualificationHttpConfig(base: httpx.ServerConfig) httpx.ServerConfig {
    var config = base;
    // The qualification driver deliberately sends one request at a time and
    // uses the stop file, not an idle pooled socket, as its lifetime control.
    // Closing each response connection makes that ownership explicit: no
    // client connection may outlive the final response and race teardown of
    // the test allocator, router, or Node. This is private harness transport
    // policy; the production Node configuration is left unchanged.
    config.keep_alive = false;
    config.max_requests_per_connection = 1;
    return config;
}

fn logModelAdmissionDiagnostic(node: *server_mod.Node, event: []const u8) u64 {
    const diagnostic = node.model_manager.memoryAdmissionDiagnostic();
    const current = node.model_manager.currentAdmissionAmounts();
    std.debug.print(
        "metal_checkpoint_admission_diagnostic: {{\"schema\":\"antfly.metal-checkpoint-admission-diagnostic.v1\",\"event\":\"{s}\",\"pressure\":\"{s}\",\"denied_count\":{d},\"last_denied_ns\":{d},\"request_count\":{d},\"requested_host_bytes\":{d},\"requested_backend_bytes\":{d},\"requested_kv_bytes\":{d},\"requested_scratch_bytes\":{d},\"admitted_host_bytes\":{d},\"admitted_backend_bytes\":{d},\"host_limit_bytes\":{d},\"backend_limit_bytes\":{d},\"combined_limit_bytes\":{d},\"available_memory_bytes\":{d},\"current\":{{\"host_weight_bytes\":{d},\"backend_weight_bytes\":{d},\"host_kv_bytes\":{d},\"backend_kv_bytes\":{d},\"host_scratch_bytes\":{d},\"backend_scratch_bytes\":{d},\"host_total_bytes\":{d},\"backend_total_bytes\":{d},\"kv_total_bytes\":{d},\"scratch_total_bytes\":{d}}}}}\n",
        .{
            event,
            diagnostic.pressure,
            diagnostic.denied_count,
            diagnostic.last_denied_ns,
            diagnostic.request_count,
            diagnostic.requested_host_bytes,
            diagnostic.requested_backend_bytes,
            diagnostic.requested_kv_bytes,
            diagnostic.requested_scratch_bytes,
            diagnostic.admitted_host_bytes,
            diagnostic.admitted_backend_bytes,
            diagnostic.host_limit_bytes,
            diagnostic.backend_limit_bytes,
            diagnostic.combined_limit_bytes,
            diagnostic.available_memory_bytes,
            current.host_weight_bytes,
            current.backend_weight_bytes,
            current.host_kv_bytes,
            current.backend_kv_bytes,
            current.host_scratch_bytes,
            current.backend_scratch_bytes,
            current.hostTotalBytes(),
            current.backendTotalBytes(),
            current.kvTotalBytes(),
            current.scratchTotalBytes(),
        },
    );
    return diagnostic.denied_count;
}

test "context Metal qualification HTTP transport closes each request" {
    const base: httpx.ServerConfig = .{
        .host = "127.0.0.1",
        .port = 17291,
        .max_connections = 4,
        .max_request_tasks = 2,
        .keep_alive = true,
        .max_requests_per_connection = 17,
    };
    const config = qualificationHttpConfig(base);
    try std.testing.expect(!config.keep_alive);
    try std.testing.expectEqual(@as(u32, 1), config.max_requests_per_connection);
    try std.testing.expectEqual(base.port, config.port);
    try std.testing.expectEqual(base.max_connections, config.max_connections);
    try std.testing.expectEqual(base.max_request_tasks, config.max_request_tasks);
    // The helper receives a value; the caller's production-derived policy is
    // not mutated while making the private listener single-request-owned.
    try std.testing.expect(base.keep_alive);
    try std.testing.expectEqual(@as(u32, 17), base.max_requests_per_connection);
}

test "context Metal native checkpoint qualification server" {
    if (comptime builtin.os.tag != .macos or !build_options.enable_metal) return error.SkipZigTest;
    if (comptime build_options.enable_cuda) return error.InvalidMetalCheckpointQualificationBuild;

    const models_root = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_MODELS_ROOT") orelse return error.SkipZigTest;
    if (models_root.len == 0) return error.InvalidMetalCheckpointQualificationInput;
    const model_name = try qualificationModel();
    const port_text = try requiredEnv("ANTFLY_METAL_CHECKPOINT_PORT");
    const ready_path = try requiredEnv("ANTFLY_METAL_CHECKPOINT_READY_FILE");
    const stop_path = try requiredEnv("ANTFLY_METAL_CHECKPOINT_STOP_FILE");
    const max_lifetime_ns = try qualificationLifetimeNs();
    try requireAbsolute(models_root);
    try requireAbsolute(ready_path);
    try requireAbsolute(stop_path);
    if (std.mem.eql(u8, ready_path, stop_path)) return error.InvalidMetalCheckpointQualificationInput;
    try requireAbsent(ready_path);
    try requireAbsent(stop_path);
    const parsed_port = try std.fmt.parseInt(u16, port_text, 10);
    if (parsed_port == 0) return error.InvalidMetalCheckpointQualificationInput;

    var node = try server_mod.Node.init(allocator, .{
        .models_dir = models_root,
        .max_loaded_models = 1,
        .max_concurrent_requests = try qualificationMaxConcurrentRequests(),
        .http_max_connections = 4,
        .http_max_request_tasks = 2,
        .http_max_body_bytes = 2 * 1024 * 1024,
        .keep_alive_ms = 12 * 60 * 60 * 1000,
        .process_termination_available = true,
        .generation_budget_overrides = qualificationGenerationBudgetOverrides(),
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &metal_backends;
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &metal_backends;
    node.model_manager.session_manager.required_backend = .metal;
    node.test_allow_metal_native_checkpoint = true;
    try node.attachIo(io);

    const model_dir = try node.resolveModelPath(io, model_name, "generators");
    defer allocator.free(model_dir);
    var model = try node.model_manager.acquireFromDirWithPreferredBackends(model_dir, &metal_backends, true);
    defer model.release();
    const loaded = model.get();
    if (loaded.session.backend() != .metal) return error.MetalCheckpointQualificationBackendMismatch;
    const identity = try loaded.nativeKvIdentity(.f16, null);
    if (loaded.load_generation == 0) return error.InvalidMetalCheckpointQualificationIdentity;

    var server_io = std.Io.Threaded.init(allocator, .{ .concurrent_limit = .limited(8) });
    defer server_io.deinit();
    var http_server = httpx.Server.initWithConfig(
        allocator,
        server_io.io(),
        qualificationHttpConfig(node.httpServerConfig("127.0.0.1", parsed_port)),
    );
    defer http_server.deinit();
    try node.registerHttpRoutes(&http_server);
    var listener = httpx.Server.ListenerTask.init(&http_server);
    defer {
        listener.requestStop();
        listener.join() catch {};
    }
    try http_server.bind();
    const address = http_server.boundAddress() orelse return error.MissingMetalCheckpointQualificationAddress;
    if (address != .ip4 or !std.mem.eql(u8, &address.ip4.bytes, &.{ 127, 0, 0, 1 }) or address.ip4.port != parsed_port)
        return error.MetalCheckpointQualificationAddressMismatch;
    try listener.start();
    const startup_deadline = platform.time.monotonicNs() + 30 * std.time.ns_per_s;
    while (!http_server.listen_started.load(.acquire)) {
        if (listener.runtimeFailure()) |err| return err;
        if (platform.time.monotonicNs() >= startup_deadline) return error.MetalCheckpointQualificationStartTimeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try writeReady(ready_path, parsed_port, loaded.load_generation, model_name, identity);

    const lifetime_deadline = platform.time.monotonicNs() + max_lifetime_ns;
    var observed_denied_count = logModelAdmissionDiagnostic(&node, "startup");
    while (true) {
        const denied_count = node.model_manager.memoryAdmissionDiagnostic().denied_count;
        if (denied_count != observed_denied_count)
            observed_denied_count = logModelAdmissionDiagnostic(&node, "denied_count_changed");
        if (try stopRequested(stop_path)) break;
        if (listener.runtimeFailure()) |err| return err;
        if (platform.time.monotonicNs() >= lifetime_deadline) return error.MetalCheckpointQualificationLifetimeExceeded;
        try io.sleep(.fromMilliseconds(25), .awake);
    }
    listener.shutdown(25);
    try listener.join();
    try std.testing.expectEqual(httpx.Server.ListenerTask.RuntimeState.stopped, listener.runtimeState());
    const stats = http_server.runtimeStats();
    try std.testing.expectEqual(@as(usize, 0), stats.active_connections);
    try std.testing.expectEqual(@as(usize, 0), stats.active_requests);
    try std.testing.expectEqual(@as(usize, 0), stats.body_buffer_in_use_bytes);
    const runtime_stats = http_server.httpRuntimeStats();
    try std.testing.expectEqual(@as(usize, 0), runtime_stats.active_listener_leases);
    try std.testing.expectEqual(@as(usize, 0), runtime_stats.reserved_connection_capacity);
    try std.testing.expectEqual(@as(usize, 0), runtime_stats.reserved_request_capacity);
    try std.testing.expectEqual(@as(usize, 0), runtime_stats.reserved_h1_request_capacity);
    try std.testing.expectEqual(@as(usize, 0), runtime_stats.active_h1_cancellation_observers);
}

test "context Metal checkpoint successive installed owner model qualification" {
    if (comptime builtin.os.tag != .macos or !build_options.enable_metal) return error.SkipZigTest;
    if (comptime build_options.enable_cuda) return error.InvalidMetalCheckpointQualificationBuild;
    const input_path = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_CYCLE_INPUT") orelse return error.SkipZigTest;
    var phase: []const u8 = "input";
    errdefer std.log.err("metal_checkpoint_cycle_driver_rejected phase={s}", .{phase});
    const output_path = try requiredEnv("ANTFLY_METAL_CHECKPOINT_CYCLE_OUTPUT");
    const models_root = try requiredEnv("ANTFLY_METAL_CHECKPOINT_MODELS_ROOT");
    const model_name = try qualificationModel();
    try requireAbsolute(input_path);
    try requireAbsolute(output_path);
    try requireAbsolute(models_root);
    try requireAbsent(output_path);
    const generation = @import("pipelines/generation.zig");
    const cycles = @import("runtime/kv/metal_checkpoint_cycles.zig");
    const Input = struct {
        schema: []const u8,
        model: []const u8,
        expected_prompt_tokens: usize,
        messages: []const generation.Message,
        request: cycles.Request,
    };
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, input_path, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(raw);
    var parsed = try std.json.parseFromSlice(Input, allocator, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const input = parsed.value;
    if (!std.mem.eql(u8, input.schema, "antfly.metal-checkpoint-cycles.input.v1") or
        !std.mem.eql(u8, input.model, model_name) or input.messages.len == 0)
        return error.InvalidMetalCheckpointQualificationInput;
    _ = try input.request.capacity(input.expected_prompt_tokens);
    phase = "model_load";
    var node = try server_mod.Node.init(allocator, .{
        .models_dir = models_root,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 8 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
            .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &metal_backends;
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &metal_backends;
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(io);
    const model_dir = try node.resolveModelPath(io, model_name, "generators");
    defer allocator.free(model_dir);
    var handle = try node.model_manager.acquireFromDirWithPreferredBackends(model_dir, &metal_backends, true);
    defer handle.release();
    const model = handle.get();
    if (model.session.backend() != .metal) return error.MetalCheckpointQualificationBackendMismatch;
    phase = "identity";
    const identity = try model.nativeKvIdentity(.f16, null);
    phase = "tokenization";
    const template = model.chat_tmpl orelse return error.MissingChatTemplate;
    const prompt = try template.applyWithOptions(allocator, input.messages, .{ .enable_thinking = false });
    defer allocator.free(prompt);
    var encoded = try generation.encodeNativeGenerationPrompt(model.getTokenizer(), allocator, prompt, @import("runtime/kv/attention_match_checkpoint.zig").max_context_tokens, model.manifest.add_bos_token, model.manifest.bos_token);
    defer encoded.deinit();
    var count: usize = 0;
    while (count < encoded.attention_mask.len and encoded.attention_mask[count] != 0) : (count += 1) {}
    if (count != input.expected_prompt_tokens) return error.InvalidMetalCheckpointQualificationInput;
    const tokens = try allocator.alloc(i64, count);
    defer allocator.free(tokens);
    for (tokens, encoded.ids[0..count]) |*target, token| target.* = token;
    const factory = @import("architectures/session_factory.zig");
    var budget = @import("runtime/tier/memory.zig").RunBudget.init(.{
        .host_limit_bytes = 8 * 1024 * 1024 * 1024,
        .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
        .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
        .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
        .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
    });
    phase = "backend";
    var cb = try factory.getComputeBackendWithBudget(model.session, allocator, &budget);
    defer cb.deinit();
    var pipeline: generation.NativeGenerationPipeline = .{
        .allocator = allocator,
        .io = io,
        .cb = cb,
        .session = model.session,
        .load_generation = model.load_generation,
        .gpt_config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration,
        .kv_dtype = .f16,
        .attention_match_identity = identity,
        .tokenizer = model.getTokenizer(),
        .chat_template = model.chat_tmpl,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .execution_lock = model.nativeGenerationMutex(),
    };
    defer pipeline.deinit();
    phase = "native_cycles";
    const report = try pipeline.qualifyMetalCheckpointCycles(tokens, input.request);
    var input_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &input_digest, .{});
    var prompt_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(prompt, &prompt_digest, .{});
    const input_hex = std.fmt.bytesToHex(input_digest, .lower);
    const prompt_hex = std.fmt.bytesToHex(prompt_digest, .lower);
    const token_hex = std.fmt.bytesToHex(try @import("backends/context_teacher_forced.zig").promptTokenDigest(tokens), .lower);
    const implementation = std.fmt.bytesToHex(@import("compaction_identity").digest, .lower);
    const body = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.metal-checkpoint-cycles.report.v3",
        .model_directory = model.model_dir,
        .model = model_name,
        .encoding = @tagName(input.request.encoding),
        .load_generation = model.load_generation,
        .model_identity = identity,
        .input_sha256 = input_hex[0..],
        .rendered_prompt_sha256 = prompt_hex[0..],
        .prompt_tokens_sha256 = token_hex[0..],
        .implementation_sha256 = implementation[0..],
        .report = report,
    }, .{ .whitespace = .indent_2 });
    defer allocator.free(body);
    phase = "report_write";
    var output = try std.Io.Dir.cwd().createFile(io, output_path, .{ .exclusive = true, .truncate = false });
    defer output.close(io);
    try output.writeStreamingAll(io, body);
    try output.writeStreamingAll(io, "\n");
    try output.sync(io);
}

test "context Metal checkpoint performance requests materializer" {
    if (comptime builtin.os.tag != .macos or !build_options.enable_metal) return error.SkipZigTest;
    if (comptime build_options.enable_cuda) return error.InvalidMetalCheckpointQualificationBuild;
    const models_root = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_MODELS_ROOT") orelse return error.SkipZigTest;
    const output_dir = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_PERF_OUTPUT_DIR") orelse return error.SkipZigTest;
    const model_name = try qualificationModel();
    if (models_root.len == 0 or output_dir.len == 0) return error.InvalidMetalCheckpointQualificationInput;
    try requireAbsolute(models_root);
    try requireAbsolute(output_dir);
    try requireAbsent(output_dir);
    try std.Io.Dir.createDirAbsolute(io, output_dir, std.Io.File.Permissions.fromMode(0o700));

    var node = try server_mod.Node.init(allocator, .{
        .models_dir = models_root,
        .max_loaded_models = 1,
        .max_concurrent_requests = try qualificationMaxConcurrentRequests(),
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 8 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
            .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &metal_backends;
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &metal_backends;
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(io);
    const model_dir = try node.resolveModelPath(io, model_name, "generators");
    defer allocator.free(model_dir);
    var handle = try node.model_manager.acquireFromDirWithPreferredBackends(model_dir, &metal_backends, true);
    defer handle.release();
    const model = handle.get();
    if (model.session.backend() != .metal) return error.MetalCheckpointQualificationBackendMismatch;
    const identity = try model.nativeKvIdentity(.f16, null);
    if (model.load_generation == 0) return error.InvalidMetalCheckpointQualificationIdentity;

    const Spec = struct { name: []const u8, prompt_tokens: usize, max_tokens: usize };
    const specs = [_]Spec{
        .{ .name = "request-8k.json", .prompt_tokens = 8128, .max_tokens = 64 },
        .{ .name = "request-32k.json", .prompt_tokens = 32512, .max_tokens = 256 },
    };
    var request_sha256: [specs.len][32]u8 = undefined;
    for (specs, 0..) |spec, index| {
        const prompt = try sizedPerformancePrompt(model, spec.prompt_tokens);
        defer allocator.free(prompt);
        if (try performancePromptCount(model, prompt) != spec.prompt_tokens)
            return error.MetalCheckpointPerformancePromptLengthMismatch;
        const body = try std.json.Stringify.valueAlloc(allocator, .{
            .model = model_name,
            .messages = &.{.{ .role = "user", .content = prompt }},
            .mode = "eager",
            .cache_dtype = "f16",
            .max_tokens = spec.max_tokens,
            .temperature = 0,
            .prompt_cache = false,
            .stream = true,
            .stream_options = .{ .include_usage = true },
            .chat_template_kwargs = .{ .enable_thinking = false },
        }, .{});
        defer allocator.free(body);
        const path = try std.fs.path.join(allocator, &.{ output_dir, spec.name });
        defer allocator.free(path);
        request_sha256[index] = try writeExclusive(path, body);
    }
    const weights = std.fmt.bytesToHex(identity.weights, .lower);
    const tokenizer = std.fmt.bytesToHex(identity.tokenizer, .lower);
    const template = std.fmt.bytesToHex(identity.template, .lower);
    const layout = std.fmt.bytesToHex(identity.layout, .lower);
    const request_8k = std.fmt.bytesToHex(request_sha256[0], .lower);
    const request_32k = std.fmt.bytesToHex(request_sha256[1], .lower);
    const metadata = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.metal-checkpoint-performance-requests.v1",
        .backend = "metal",
        .model = model_name,
        .load_generation = model.load_generation,
        .synthetic_payload = true,
        .qualification_label = "synthetic_repeated_token_capacity_and_performance_only",
        .loaded_identity = .{
            .weights = weights[0..],
            .tokenizer = tokenizer[0..],
            .template = template[0..],
            .layout = layout[0..],
        },
        .requests = .{
            .request_8k = .{ .file = specs[0].name, .prompt_tokens = specs[0].prompt_tokens, .max_tokens = specs[0].max_tokens, .sha256 = request_8k[0..] },
            .request_32k = .{ .file = specs[1].name, .prompt_tokens = specs[1].prompt_tokens, .max_tokens = specs[1].max_tokens, .sha256 = request_32k[0..] },
        },
    }, .{});
    defer allocator.free(metadata);
    const metadata_path = try std.fs.path.join(allocator, &.{ output_dir, "metadata.json" });
    defer allocator.free(metadata_path);
    _ = try writeExclusive(metadata_path, metadata);
}
test "context Metal native checkpoint paired quality qualification" {
    if (comptime builtin.os.tag != .macos or !build_options.enable_metal) return error.SkipZigTest;
    if (comptime build_options.enable_cuda) return error.InvalidMetalCheckpointQualificationBuild;
    const input_path = platform.env.getenv("ANTFLY_METAL_NATIVE_QUALITY_INPUT") orelse return error.SkipZigTest;
    const input_root = try requiredEnv("ANTFLY_METAL_NATIVE_QUALITY_INPUT_ROOT");
    const output_root = try requiredEnv("ANTFLY_METAL_NATIVE_QUALITY_OUTPUT_ROOT");
    const models_root = try requiredEnv("ANTFLY_METAL_CHECKPOINT_MODELS_ROOT");
    const model_name = try qualificationModel();
    inline for (.{ input_path, input_root, output_root, models_root }) |path| try requireAbsolute(path);
    const driver = @import("context_native_checkpoint_quality.zig");
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, input_path, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(raw);
    var parsed = try std.json.parseFromSlice(driver.Input, allocator, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const input = parsed.value;
    if (!std.mem.eql(u8, input.schema, "antfly.native-checkpoint-quality.input.v1") or
        !std.mem.eql(u8, input.model, model_name) or input.cases.len == 0 or input.cases.len > 100)
        return error.InvalidMetalCheckpointQualificationInput;
    const expected_main = try driver.parseDigest(input.main_gguf_sha256);
    var inputs = try std.Io.Dir.cwd().openDir(io, input_root, .{ .follow_symlinks = false });
    defer inputs.close(io);
    var outputs = try std.Io.Dir.cwd().openDir(io, output_root, .{ .follow_symlinks = false });
    defer outputs.close(io);
    var node = try server_mod.Node.init(allocator, .{
        .models_dir = models_root,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 8 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
            .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &metal_backends;
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &metal_backends;
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(io);
    const model_dir = try node.resolveModelPath(io, model_name, "generators");
    defer allocator.free(model_dir);
    var handle = try node.model_manager.acquireFromDirWithPreferredBackends(model_dir, &metal_backends, true);
    defer handle.release();
    const model = handle.get();
    if (model.session.backend() != .metal) return error.MetalCheckpointQualificationBackendMismatch;
    const native = try model.nativeKvIdentity(.f16, null);
    const factory = @import("architectures/session_factory.zig");
    const actual_main = factory.getLoadedGgufArtifactDigest(model.session) orelse return error.UnsupportedCompactionArtifact;
    if (!native.eql(input.native_identity) or !std.mem.eql(u8, &expected_main, &actual_main))
        return error.ContextQualificationLoadedIdentityMismatch;
    const generation = @import("pipelines/generation.zig");
    var budget = @import("runtime/tier/memory.zig").RunBudget.init(.{
        .host_limit_bytes = 8 * 1024 * 1024 * 1024,
        .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
        .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
        .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
        .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
    });
    var cb = try factory.getComputeBackendWithBudget(model.session, allocator, &budget);
    defer cb.deinit();
    var pipeline: generation.NativeGenerationPipeline = .{
        .allocator = allocator,
        .io = io,
        .cb = cb,
        .session = model.session,
        .load_generation = model.load_generation,
        .gpt_config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration,
        .kv_dtype = .f16,
        .attention_match_identity = native,
        .tokenizer = model.getTokenizer(),
        .chat_template = model.chat_tmpl,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .execution_lock = model.nativeGenerationMutex(),
    };
    defer pipeline.deinit();
    var manifest_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &manifest_digest, .{});
    var session: driver.Session = .{
        .allocator = allocator,
        .io = io,
        .inputs = inputs,
        .outputs = outputs,
        .model = model,
        .pipeline = &pipeline,
        .manifest_sha256 = manifest_digest,
    };
    for (input.cases, 0..) |case, index| {
        for (input.cases[0..index]) |previous|
            if (std.mem.eql(u8, previous.id, case.id) and previous.request.encoding == case.request.encoding)
                return error.DuplicateNativeQualityCase;
        session.execute(case, index) catch |err| {
            const failure = try std.json.Stringify.valueAlloc(allocator, .{
                .schema = "antfly.native-checkpoint-quality.failure.v1",
                .release_eligible = false,
                .case_index = index,
                .case_id = case.id,
                .encoding = @tagName(case.request.encoding),
                .@"error" = @errorName(err),
                .manifest_sha256 = manifest_digest,
            }, .{ .whitespace = .indent_2 });
            defer allocator.free(failure);
            var file = try outputs.createFile(io, "failure.json", .{ .exclusive = true, .truncate = false });
            defer file.close(io);
            try file.writeStreamingAll(io, failure);
            try file.sync(io);
            return err;
        };
        if (!native.eql(try model.nativeKvIdentity(.f16, null)) or model.load_generation != pipeline.load_generation)
            return error.StaleContextView;
    }
}

test "context Metal native quality tokenizer materializer" {
    if (!builtin.is_test or !build_options.enable_metal or
        !platform.env.getenvBool("ANTFLY_METAL_QUALITY_MATERIALIZER")) return error.SkipZigTest;
    const model_name = try qualificationModel();
    const models_root = try requiredEnv("ANTFLY_METAL_CHECKPOINT_MODELS_DIR");
    const campaign_root = try requiredEnv("ANTFLY_METAL_QUALITY_CAMPAIGN_DIR");
    try requireAbsolute(models_root);
    try requireAbsolute(campaign_root);
    const session = try requiredEnv("ANTFLY_METAL_QUALITY_SESSION_ID");
    if (session.len != 64) return error.InvalidMetalCheckpointQualificationInput;
    var session_bytes: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(&session_bytes, session);
    const maximum = try std.fmt.parseInt(u32, try requiredEnv("ANTFLY_METAL_QUALITY_PREPARATION_CONTEXT"), 10);
    if (maximum != 16384 and maximum != 65536) return error.InvalidMetalCheckpointQualificationInput;
    var campaign = try std.Io.Dir.cwd().openDir(io, campaign_root, .{ .follow_symlinks = false });
    defer campaign.close(io);
    var node = try server_mod.Node.init(allocator, .{
        .models_dir = models_root,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 8 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
            .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &metal_backends;
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &metal_backends;
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(io);
    const model_dir = try node.resolveModelPath(io, model_name, "generators");
    defer allocator.free(model_dir);
    var handle = try node.model_manager.acquireFromDirWithPreferredBackends(model_dir, &metal_backends, true);
    defer handle.release();
    const model = handle.get();
    if (model.session.backend() != .metal) return error.MetalCheckpointQualificationBackendMismatch;
    const native = try model.nativeKvIdentity(.f16, null);
    // Hash the actual loaded main GGUF through its retained session, rather
    // than treating a caller's expected file digest as an observation.
    const factory = @import("architectures/session_factory.zig");
    const artifact = factory.getLoadedGgufArtifactDigest(model.session) orelse return error.UnsupportedCompactionArtifact;
    const identity: server_mod.ContextServingLoadedIdentity = .{
        .load_generation = model.load_generation,
        .artifact_sha256 = artifact,
        .tokenizer_sha256 = native.tokenizer,
        .template_sha256 = native.template,
        .layout_sha256 = native.layout,
        .profile_sha256 = @splat(0),
        .implementation_sha256 = @import("compaction_identity").digest,
    };
    const main_hex = std.fmt.bytesToHex(artifact, .lower);
    const implementation_hex = std.fmt.bytesToHex(@import("compaction_identity").digest, .lower);
    const weights_hex = std.fmt.bytesToHex(native.weights, .lower);
    const tokenizer_hex = std.fmt.bytesToHex(native.tokenizer, .lower);
    const template_hex = std.fmt.bytesToHex(native.template, .lower);
    const layout_hex = std.fmt.bytesToHex(native.layout, .lower);
    const native_head_policy = platform.env.getenvBool("ANTFLY_METAL_QUALITY_NATIVE_HEAD_POLICY");
    const fitting = @import("pipelines/generation.zig").NativeCheckpointPrepare.fittingOptions();
    const protected_prefix: u32 = if (native_head_policy) @intCast(fitting.prefix_rows) else 512;
    const protected_recent: u32 = if (native_head_policy) @intCast(fitting.recent_rows) else 512;
    const ready = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.native-quality-materializer.ready.v1",
        .preparation_only = true,
        .model_outputs_observed = false,
        .model_forward_steps = @as(u32, 0),
        .model = model_name,
        .model_directory = model.model_dir,
        .load_generation = model.load_generation,
        .native_identity = .{ .weights = weights_hex[0..], .tokenizer = tokenizer_hex[0..], .template = template_hex[0..], .layout = layout_hex[0..] },
        .main_gguf_sha256 = main_hex[0..],
        .implementation_sha256 = implementation_hex[0..],
        .native_head_policy = native_head_policy,
        .protected_prefix_rows = protected_prefix,
        .protected_recent_rows = protected_recent,
    }, .{});
    defer allocator.free(ready);
    var ready_file = try campaign.createFile(io, "native-materializer-ready.json", .{ .exclusive = true, .truncate = false });
    defer ready_file.close(io);
    try ready_file.writeStreamingAll(io, ready);
    try ready_file.writeStreamingAll(io, "\n");
    try ready_file.sync(io);
    var materializer: @import("context_native_quality_materializer.zig").Session = .{
        .allocator = allocator,
        .io = io,
        .campaign_dir = campaign,
        .model = model_name,
        .loaded_model = model,
        .loaded_identity = identity,
        .expected_session_id = session[0..64].*,
        .max_context_tokens = maximum,
        .max_kv_tokens = maximum / 2,
        .keep_prefix_tokens = protected_prefix,
        .keep_recent_tokens = protected_recent,
    };
    try materializer.run();
    if (model.load_generation != identity.load_generation or
        !std.meta.eql(native, try model.nativeKvIdentity(.f16, null)))
        return error.StaleContextView;
}

test "context Metal checkpoint persistent installed owner soak qualification" {
    if (comptime builtin.os.tag != .macos or !build_options.enable_metal) return error.SkipZigTest;
    if (comptime build_options.enable_cuda) return error.InvalidMetalCheckpointQualificationBuild;
    const input_path = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_SOAK_INPUT") orelse return error.SkipZigTest;
    const output_path = try requiredEnv("ANTFLY_METAL_CHECKPOINT_SOAK_OUTPUT");
    const journal_path = try requiredEnv("ANTFLY_METAL_CHECKPOINT_SOAK_JOURNAL");
    const models_root = try requiredEnv("ANTFLY_METAL_CHECKPOINT_MODELS_ROOT");
    const model_name = try qualificationModel();
    inline for (.{ input_path, output_path, journal_path, models_root }) |path| try requireAbsolute(path);
    try requireAbsent(output_path);
    try requireAbsent(journal_path);
    const checkpoint = @import("runtime/kv/attention_match_checkpoint.zig");
    const soak = @import("runtime/kv/metal_checkpoint_soak.zig");
    const driver = @import("context_native_checkpoint_quality.zig");
    const Input = struct {
        schema: []const u8,
        model: []const u8,
        main_gguf_sha256: []const u8,
        native_identity: checkpoint.Identity,
        checkpoints: [2]driver.File,
        request: soak.Request,
    };
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, input_path, allocator, .limited(16 * 1024 * 1024));
    defer allocator.free(raw);
    var parsed = try std.json.parseFromSlice(Input, allocator, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const input = parsed.value;
    if (!std.mem.eql(u8, input.schema, "antfly.native-checkpoint-soak.input.v2") or
        !std.mem.eql(u8, input.model, model_name)) return error.InvalidNativeSoakRequest;
    try input.request.validate();
    const expected_main = try driver.parseDigest(input.main_gguf_sha256);
    var node = try server_mod.Node.init(allocator, .{
        .models_dir = models_root,
        .max_loaded_models = 1,
        .max_concurrent_requests = 1,
        .process_termination_available = true,
        .generation_budget_overrides = .{
            .host_limit_bytes = 8 * 1024 * 1024 * 1024,
            .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
            .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
            .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
            .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
        },
    });
    defer node.deinit();
    node.session_manager.preferred_backends = &metal_backends;
    node.session_manager.required_backend = .metal;
    node.model_manager.session_manager.preferred_backends = &metal_backends;
    node.model_manager.session_manager.required_backend = .metal;
    try node.attachIo(io);
    const model_dir = try node.resolveModelPath(io, model_name, "generators");
    defer allocator.free(model_dir);
    var handle = try node.model_manager.acquireFromDirWithPreferredBackends(model_dir, &metal_backends, true);
    defer handle.release();
    const model = handle.get();
    if (model.session.backend() != .metal) return error.MetalCheckpointQualificationBackendMismatch;
    const native = try model.nativeKvIdentity(.f16, null);
    const factory = @import("architectures/session_factory.zig");
    const actual_main = factory.getLoadedGgufArtifactDigest(model.session) orelse return error.UnsupportedCompactionArtifact;
    if (!native.eql(input.native_identity) or !std.mem.eql(u8, &expected_main, &actual_main))
        return error.ContextQualificationLoadedIdentityMismatch;
    const generation = @import("pipelines/generation.zig");
    var budget = @import("runtime/tier/memory.zig").RunBudget.init(.{
        .host_limit_bytes = 8 * 1024 * 1024 * 1024,
        .backend_limit_bytes = 8 * 1024 * 1024 * 1024,
        .combined_limit_bytes = 10 * 1024 * 1024 * 1024,
        .kv_limit_bytes = 4 * 1024 * 1024 * 1024,
        .scratch_limit_bytes = 6 * 1024 * 1024 * 1024,
    });
    var cb = try factory.getComputeBackendWithBudget(model.session, allocator, &budget);
    defer cb.deinit();
    var pipeline: generation.NativeGenerationPipeline = .{
        .allocator = allocator,
        .io = io,
        .cb = cb,
        .session = model.session,
        .load_generation = model.load_generation,
        .gpt_config = factory.getGptConfig(model.session) orelse return error.InvalidModelForGeneration,
        .kv_dtype = .f16,
        .attention_match_identity = native,
        .tokenizer = model.getTokenizer(),
        .chat_template = model.chat_tmpl,
        .add_bos_token = model.manifest.add_bos_token,
        .bos_token = model.manifest.bos_token,
        .execution_lock = model.nativeGenerationMutex(),
    };
    defer pipeline.deinit();
    var saved: [2]checkpoint.Owned = undefined;
    var saved_count: usize = 0;
    defer for (saved[0..saved_count]) |*value| value.deinit();
    for (input.checkpoints, &saved) |file_ref, *value| {
        try requireAbsolute(file_ref.path);
        var file = try std.Io.Dir.cwd().openFile(io, file_ref.path, .{ .mode = .read_only, .allow_directory = false, .follow_symlinks = false });
        defer file.close(io);
        const stat = try file.stat(io);
        if (stat.kind != .file or stat.size == 0 or stat.size > checkpoint.serving_limits.max_file_bytes)
            return error.InvalidNativeSoakRequest;
        const wire = try allocator.alloc(u8, @intCast(stat.size));
        defer allocator.free(wire);
        if (try file.readPositionalAll(io, wire, 0) != wire.len) return error.InvalidNativeSoakRequest;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(wire, &digest, .{});
        if (!std.mem.eql(u8, &digest, &try driver.parseDigest(file_ref.sha256))) return error.NativeSoakCheckpointChanged;
        value.* = try checkpoint.decodeWithControl(allocator, wire, native, checkpoint.serving_limits, .{});
        saved_count += 1;
    }
    var journal = try std.Io.Dir.cwd().createFile(io, journal_path, .{ .exclusive = true, .truncate = false });
    defer journal.close(io);
    const report = try pipeline.qualifyMetalCheckpointSoak(.{ saved[0].state, saved[1].state }, input.request, &journal);
    if (!native.eql(try model.nativeKvIdentity(.f16, null)) or model.load_generation != pipeline.load_generation)
        return error.StaleContextView;
    var input_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &input_digest, .{});
    const observed = try @as(*@import("ops/metal_compute.zig").MetalCompute, @ptrCast(@alignCast(cb.ptr))).provider_impl.nativeCheckpointObserved();
    const execution = @as(*@import("ops/metal_compute.zig").MetalCompute, @ptrCast(@alignCast(cb.ptr))).attentionMatchExecution();
    const body = try std.json.Stringify.valueAlloc(allocator, .{
        .schema = "antfly.native-checkpoint-soak.report.v2",
        .model = model_name,
        .model_directory = model.model_dir,
        .model_identity = native,
        .main_gguf_sha256 = actual_main,
        .load_generation = model.load_generation,
        .input_sha256 = input_digest,
        .implementation_sha256 = @import("compaction_identity").digest,
        .observed = observed,
        .execution = execution,
        .configuration_sha256 = @import("runtime/kv/attention_match_release.zig").configurationFingerprint(execution, observed.backend_environment_sha256),
        .encoding = @tagName(input.request.encoding),
        .report = report,
    }, .{ .whitespace = .indent_2 });
    defer allocator.free(body);
    _ = try writeExclusive(output_path, body);
}
