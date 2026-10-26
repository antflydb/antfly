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
const model_name = "google/gemma-4-E4B-it-qat-q4_0-gguf";
const default_max_lifetime_ns: u64 = 12 * std.time.ns_per_hour;
const maximum_lifetime_seconds: u64 = 48 * 60 * 60 + 5 * 60;
const metal_backends = [_]backends.BackendType{.metal};

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

fn writeExclusive(path: []const u8, data: []const u8) !void {
    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .exclusive = true, .truncate = false });
    defer file.close(io);
    try file.writeStreamingAll(io, data);
    try file.writeStreamingAll(io, "\n");
    try file.sync(io);
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
        .max_concurrent_requests = 1,
        .http_max_connections = 4,
        .http_max_request_tasks = 2,
        .http_max_body_bytes = 2 * 1024 * 1024,
        .keep_alive_ms = 12 * 60 * 60 * 1000,
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
    try writeReady(ready_path, parsed_port, loaded.load_generation, identity);

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

test "context Metal checkpoint performance requests materializer" {
    if (comptime builtin.os.tag != .macos or !build_options.enable_metal) return error.SkipZigTest;
    if (comptime build_options.enable_cuda) return error.InvalidMetalCheckpointQualificationBuild;
    const models_root = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_MODELS_ROOT") orelse return error.SkipZigTest;
    const output_dir = platform.env.getenv("ANTFLY_METAL_CHECKPOINT_PERF_OUTPUT_DIR") orelse return error.SkipZigTest;
    if (models_root.len == 0 or output_dir.len == 0) return error.InvalidMetalCheckpointQualificationInput;
    try requireAbsolute(models_root);
    try requireAbsolute(output_dir);
    try requireAbsent(output_dir);
    try std.Io.Dir.createDirAbsolute(io, output_dir, std.Io.File.Permissions.fromMode(0o700));

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
        std.crypto.hash.sha2.Sha256.hash(body, &request_sha256[index], .{});
        const path = try std.fs.path.join(allocator, &.{ output_dir, spec.name });
        defer allocator.free(path);
        try writeExclusive(path, body);
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
    try writeExclusive(metadata_path, metadata);
}
