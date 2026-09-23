// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! `antfly_inference_*`: the embedded inference runtime without a database.
//! Each call takes the same request JSON and returns the same response JSON
//! as the matching `/ai/v1` route of the inference HTTP API, dispatched in
//! memory to the same handlers.
const std = @import("std");
const capi = @import("types.zig");
const db = @import("db.zig");
const inference_provider = db.inference_provider;

const alloc = std.heap.c_allocator;

const known_flags: u32 = 0;

const InferenceHandle = struct {
    io: *std.Io.Threaded,
    lifetime: inference_provider.EmbeddedInferenceProviderLifetime,
    /// The runtime borrows the models directory for its whole life.
    models_dir: ?[]u8,
    call_timeout_ms: u64,

    fn destroy(self: *InferenceHandle) void {
        self.lifetime.quiesce();
        inference_provider.destroyEmbeddedInferenceNode(self.lifetime.handle, self.lifetime.resource_owner);
        self.io.deinit();
        alloc.destroy(self.io);
        if (self.models_dir) |path| alloc.free(path);
        alloc.destroy(self);
    }
};

/// Inference handles get the same id guarantees as database handles (see
/// `HandleRegistryOf`) from a separate registry. On targets that reserve an
/// address range per registry, a database handle passed here, or an
/// inference handle passed to an `antfly_db_*` call, is rejected.
var registry: db.HandleRegistryOf(InferenceHandle) = .{};

pub export fn antfly_inference_options_size() u32 {
    return @intCast(@sizeOf(capi.InferenceOptions));
}

pub export fn antfly_inference_options_init(options: ?*capi.InferenceOptions) capi.ErrorCode {
    const opts = options orelse return .invalid_argument;
    opts.* = .{};
    return .ok;
}

const ResolvedOptions = struct {
    node: inference_provider.EmbeddedInferenceNodeOptions = .{},
    call_timeout_ms: u64 = 0,
};

fn resolveOptions(options: ?*const capi.InferenceOptions) !ResolvedOptions {
    const opts = options orelse return .{};
    const abi_size = opts.abi_size;
    if (abi_size < @offsetOf(capi.InferenceOptions, "flags")) return error.InvalidArgument;
    const Options = capi.InferenceOptions;
    const flags = db.readOptionField(Options, opts, abi_size, "flags") orelse 0;
    if (flags & ~known_flags != 0) return error.InvalidArgument;
    try db.validateOpenOptionsReserved(Options, opts, abi_size);
    const models_dir = (db.readOptionField(Options, opts, abi_size, "models_dir") orelse capi.Slice{}).bytes();
    return .{
        .node = .{
            .host_budget_mb = db.readOptionField(Options, opts, abi_size, "host_budget_mb") orelse 0,
            .backend_budget_mb = db.readOptionField(Options, opts, abi_size, "backend_budget_mb") orelse 0,
            .process_memory_budget_mb = db.readOptionField(Options, opts, abi_size, "process_memory_budget_mb") orelse 0,
            .combined_budget_mb = db.readOptionField(Options, opts, abi_size, "combined_budget_mb") orelse 0,
            .kv_budget_mb = db.readOptionField(Options, opts, abi_size, "kv_budget_mb") orelse 0,
            .scratch_budget_mb = db.readOptionField(Options, opts, abi_size, "scratch_budget_mb") orelse 0,
            .models_dir = if (models_dir.len == 0) null else models_dir,
        },
        .call_timeout_ms = db.readOptionField(Options, opts, abi_size, "call_timeout_ms") orelse 0,
    };
}

fn openInference(options: ?*const capi.InferenceOptions) !*anyopaque {
    if (!db.localInferenceRuntimeAvailable()) return error.UnsupportedOperation;
    var resolved = try resolveOptions(options);

    const handle = try alloc.create(InferenceHandle);
    errdefer alloc.destroy(handle);
    const models_dir = if (resolved.node.models_dir) |path| try alloc.dupe(u8, path) else null;
    errdefer if (models_dir) |path| alloc.free(path);
    resolved.node.models_dir = models_dir;

    const io_impl = try alloc.create(std.Io.Threaded);
    errdefer alloc.destroy(io_impl);
    io_impl.* = std.Io.Threaded.init(std.heap.page_allocator, .{});
    errdefer io_impl.deinit();
    // The data directory is only a fallback anchor for the models directory,
    // which the runtime resolves on its own.
    const created = try inference_provider.createEmbeddedInferenceNode(".", io_impl.io(), resolved.node);
    errdefer inference_provider.destroyEmbeddedInferenceNode(created.handle, created.resource_owner);
    // Unlike a Lite handle, which still serves storage without inference, a
    // handle that cannot run inference is useless: fail the open instead.
    if (created.configure_error != null) return error.InferenceRuntimeStartupFailed;
    handle.* = .{
        .io = io_impl,
        .lifetime = .{ .handle = created.handle, .resource_owner = created.resource_owner },
        .models_dir = models_dir,
        .call_timeout_ms = resolved.call_timeout_ms,
    };
    return registry.register(handle);
}

pub export fn antfly_inference_open(
    options: ?*const capi.InferenceOptions,
    out_handle: ?*?*anyopaque,
) capi.ErrorCode {
    const out = out_handle orelse return .invalid_argument;
    out.* = null;
    out.* = openInference(options) catch |err| return capi.mapError(err);
    return .ok;
}

/// Rejects new calls, waits for in-flight ones, then stops the runtime.
/// Safe for NULL, stale, and concurrent or repeated closes.
pub export fn antfly_inference_close(handle_ptr: ?*anyopaque) void {
    const handle, const id = registry.beginClose(handle_ptr) orelse return;
    handle.destroy();
    registry.finishClose(id);
}

fn errorForStatus(status: u16) capi.ErrorCode {
    return switch (status) {
        200...299 => .ok,
        404 => .not_found,
        408, 429, 503, 504 => .busy,
        400...403, 405...407, 409...428, 430...499 => .invalid_argument,
        // 507: the model does not fit the configured memory budgets.
        501, 507 => .unsupported,
        else => .internal,
    };
}

fn mapCallError(err: anyerror) capi.ErrorCode {
    return switch (err) {
        // A call racing close.
        error.InferenceProviderShuttingDown => .invalid_argument,
        error.Timeout => .busy,
        // The request body is not valid JSON.
        error.SyntaxError, error.UnexpectedEndOfInput => .invalid_argument,
        else => capi.mapError(err),
    };
}

/// Runs one route. The response body goes to `out` whether or not the route
/// succeeded, so a failure carries the runtime's JSON error.
fn invoke(
    handle_ptr: ?*anyopaque,
    method: enum { get, post },
    operation: []const u8,
    request: []const u8,
    out_buf: ?*capi.Buffer,
) capi.ErrorCode {
    const out = db.resetOutBuffer(out_buf) orelse return .invalid_argument;
    const handle, const slot = registry.enter(handle_ptr) orelse return .invalid_argument;
    defer @TypeOf(registry).leave(slot);
    const deadline_ns: u64 = if (handle.call_timeout_ms == 0)
        0
    else
        db.monotonicNowNs() +| handle.call_timeout_ms *| std.time.ns_per_ms;
    const response = inference_provider.invokeEmbeddedInferenceRoute(
        &handle.lifetime,
        alloc,
        switch (method) {
            .get => .get,
            .post => .post,
        },
        operation,
        request,
        deadline_ns,
        .{},
    ) catch |err| return mapCallError(err);
    out.* = .{ .ptr = response.body.ptr, .len = response.body.len };
    return errorForStatus(response.status);
}

pub export fn antfly_inference_embed_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "embed", request_json.bytes(), out);
}

pub export fn antfly_inference_rerank_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "rerank", request_json.bytes(), out);
}

pub export fn antfly_inference_chunk_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "chunk", request_json.bytes(), out);
}

pub export fn antfly_inference_generate_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    // There is no stream sink across the C ABI; without this check the
    // runtime fails a streaming request with an opaque internal error.
    if (requestsStreaming(request_json.bytes())) {
        const buffer = db.resetOutBuffer(out) orelse return .invalid_argument;
        const body = "{\"error\":\"STREAMING_UNSUPPORTED\",\"message\":\"the C API returns complete responses; set stream to false\"}";
        const owned = alloc.dupe(u8, body) catch return .internal;
        buffer.* = .{ .ptr = owned.ptr, .len = owned.len };
        return .invalid_argument;
    }
    return invoke(h, .post, "generate", request_json.bytes(), out);
}

/// Whether a request body sets `"stream": true`. Malformed JSON is left for
/// the runtime to report.
fn requestsStreaming(request: []const u8) bool {
    const parsed = std.json.parseFromSlice(std.json.Value, alloc, request, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const stream = parsed.value.object.get("stream") orelse return false;
    return stream == .bool and stream.bool;
}

/// Runs up to 128 non-streaming generate requests as one batch. Item failures
/// are reported per item in the response; the call itself succeeds.
pub export fn antfly_inference_generate_batch_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "generate/batch", request_json.bytes(), out);
}

pub export fn antfly_inference_rewrite_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "rewrite", request_json.bytes(), out);
}

pub export fn antfly_inference_extract_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "extract", request_json.bytes(), out);
}

pub export fn antfly_inference_read_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "read", request_json.bytes(), out);
}

pub export fn antfly_inference_transcribe_json(h: ?*anyopaque, request_json: capi.Slice, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .post, "transcribe", request_json.bytes(), out);
}

pub export fn antfly_inference_list_models_json(h: ?*anyopaque, out: ?*capi.Buffer) capi.ErrorCode {
    return invoke(h, .get, "models", "", out);
}

const PullCall = struct {
    progress: ?capi.InferencePullProgressFn,
    progress_context: ?*anyopaque,
    result: ?[]u8 = null,

    fn onProgress(raw: ?*anyopaque, progress: *const inference_provider.inference_bridge.PullProgress) callconv(.c) void {
        const self: *PullCall = @ptrCast(@alignCast(raw.?));
        const callback = self.progress orelse return;
        const view = capi.InferencePullProgress{
            .model = sliceOf(progress.model.slice()),
            .file = sliceOf(progress.file.slice()),
            .bytes_downloaded = progress.bytes_downloaded,
            .total_bytes = progress.total_bytes,
            .files_done = progress.files_done,
            .files_total = progress.files_total,
            .cached = progress.cached != 0,
        };
        callback(self.progress_context, &view);
    }

    fn onResult(raw: ?*anyopaque, result: inference_provider.inference_bridge.String) callconv(.c) void {
        const self: *PullCall = @ptrCast(@alignCast(raw.?));
        self.result = alloc.dupe(u8, result.slice()) catch null;
    }
};

fn sliceOf(bytes: []const u8) capi.Slice {
    return .{ .ptr = if (bytes.len == 0) null else bytes.ptr, .len = bytes.len };
}

/// Maps a failed pull, by the error name in its result JSON.
fn pullErrorCode(name: []const u8) capi.ErrorCode {
    const table = [_]struct { []const u8, capi.ErrorCode }{
        .{ "HubModelNotFound", .not_found },
        .{ "NoModelFilesFound", .not_found },
        .{ "HubAccessDenied", .invalid_argument },
        .{ "InvalidModelRef", .invalid_argument },
        .{ "InvalidModelVariant", .invalid_argument },
        .{ "InvalidHubRevision", .invalid_argument },
        .{ "InvalidArgument", .invalid_argument },
        .{ "UnknownField", .invalid_argument },
        .{ "MissingField", .invalid_argument },
        .{ "SyntaxError", .invalid_argument },
        .{ "UnexpectedToken", .invalid_argument },
        .{ "UnexpectedEndOfInput", .invalid_argument },
        .{ "DownloadSizeLimitExceeded", .invalid_argument },
        .{ "ModelSizeLimitExceeded", .invalid_argument },
        // Transient network and hub failures; retrying may succeed.
        .{ "HubApiError", .busy },
        .{ "DownloadFailed", .busy },
        .{ "Timeout", .busy },
        .{ "UnknownHostName", .busy },
        .{ "NameServerFailure", .busy },
        .{ "ConnectionRefused", .busy },
        .{ "ConnectionResetByPeer", .busy },
        .{ "ConnectionTimedOut", .busy },
        .{ "NetworkUnreachable", .busy },
    };
    for (table) |entry| if (std.mem.eql(u8, entry[0], name)) return entry[1];
    return .internal;
}

/// Downloads models into this handle's models directory, calling `progress`
/// (if set) on the calling thread as files download.
pub export fn antfly_inference_pull_json(
    handle_ptr: ?*anyopaque,
    request_json: capi.Slice,
    progress: ?capi.InferencePullProgressFn,
    progress_context: ?*anyopaque,
    out_buf: ?*capi.Buffer,
) capi.ErrorCode {
    const out = db.resetOutBuffer(out_buf) orelse return .invalid_argument;
    const handle, const slot = registry.enter(handle_ptr) orelse return .invalid_argument;
    defer @TypeOf(registry).leave(slot);
    var call = PullCall{ .progress = progress, .progress_context = progress_context };
    const pulled = inference_provider.pullEmbeddedInferenceModels(
        handle.io.io(),
        handle.models_dir,
        request_json.bytes(),
        &call,
        PullCall.onProgress,
        &call,
        PullCall.onResult,
    );
    const result = call.result orelse return if (pulled) |_| .internal else |err| capi.mapError(err);
    out.* = .{ .ptr = result.ptr, .len = result.len };
    _ = pulled catch {
        const parsed = std.json.parseFromSlice(struct { @"error": []const u8 }, alloc, result, .{ .ignore_unknown_fields = true }) catch return .internal;
        defer parsed.deinit();
        return pullErrorCode(parsed.value.@"error");
    };
    return .ok;
}

fn testSlice(bytes: []const u8) capi.Slice {
    return .{ .ptr = bytes.ptr, .len = bytes.len };
}

fn testBuffer(buffer: capi.Buffer) []const u8 {
    return if (buffer.ptr) |ptr| ptr[0..buffer.len] else "";
}

fn modelInstalled(owner: []const u8, prefix: []const u8) bool {
    const home = std.mem.span(std.c.getenv("HOME") orelse return false);
    const owner_dir = std.fs.path.join(std.testing.allocator, &.{ home, ".antfly", "inference", "models", owner }) catch return false;
    defer std.testing.allocator.free(owner_dir);
    var dir = std.Io.Dir.cwd().openDir(std.testing.io, owner_dir, .{ .iterate = true }) catch return false;
    defer dir.close(std.testing.io);
    var it = dir.iterateAssumeFirstIteration();
    while (it.next(std.testing.io) catch return false) |entry| {
        if (std.mem.startsWith(u8, entry.name, prefix)) return true;
    }
    return false;
}

test "capi inference options are prefix compatible and reject unknown flags and reserved bits" {
    try std.testing.expectEqual(@as(u32, @sizeOf(capi.InferenceOptions)), antfly_inference_options_size());
    var options: capi.InferenceOptions = undefined;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_options_init(&options));
    try std.testing.expectEqual(@as(u32, @sizeOf(capi.InferenceOptions)), options.abi_size);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_options_init(null));

    // An older caller's prefix without the timeout is still valid.
    options.abi_size = @offsetOf(capi.InferenceOptions, "call_timeout_ms");
    options.call_timeout_ms = 1234;
    const prefix = try resolveOptions(&options);
    try std.testing.expectEqual(@as(u64, 0), prefix.call_timeout_ms);

    options = .{ .models_dir = testSlice("/models"), .kv_budget_mb = 7, .call_timeout_ms = 9 };
    const full = try resolveOptions(&options);
    try std.testing.expectEqualStrings("/models", full.node.models_dir.?);
    try std.testing.expectEqual(@as(u32, 7), full.node.kv_budget_mb);
    try std.testing.expectEqual(@as(u64, 9), full.call_timeout_ms);

    options = .{ .flags = 1 };
    try std.testing.expectError(error.InvalidArgument, resolveOptions(&options));
    options = .{};
    options.reserved[3] = 1;
    try std.testing.expectError(error.InvalidArgument, resolveOptions(&options));
    options = .{ .abi_size = 2 };
    try std.testing.expectError(error.InvalidArgument, resolveOptions(&options));

    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_open(null, null));
    options = .{ .flags = 1 };
    const expected: capi.ErrorCode = if (db.localInferenceRuntimeAvailable()) .invalid_argument else .unsupported;
    try std.testing.expectEqual(expected, antfly_inference_open(&options, &handle));
    try std.testing.expectEqual(@as(?*anyopaque, null), handle);
}

test "capi inference calls reject null, closed, and database handles" {
    var out: capi.Buffer = .{ .ptr = @constCast("x".ptr), .len = 1 };
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_list_models_json(null, &out));
    try std.testing.expectEqual(capi.Buffer{}, out);
    antfly_inference_close(null);

    var test_tmp = try db.TestDirectoryType.init("capi-inference");
    defer test_tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(std.testing.allocator, "{s}-db.aflite", .{test_tmp.path()}, 0);
    defer std.testing.allocator.free(path);
    var db_handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, db.antfly_lite_create(path.ptr, &db_handle));
    defer db.antfly_db_close(db_handle);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_list_models_json(db_handle, &out));
    // Closing a database handle through the inference API is a no-op.
    antfly_inference_close(db_handle);
    var stats: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, db.antfly_db_stats_json(db_handle, &stats));
    db.antfly_buffer_free(&stats);

    if (!db.localInferenceRuntimeAvailable()) return;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(null, &handle));
    try std.testing.expect(handle != null);
    // An inference handle is not a database handle.
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, db.antfly_db_stats_json(handle, &stats));
    antfly_inference_close(handle);
    antfly_inference_close(handle);
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_list_models_json(handle, &out));
}

test "capi inference lists models and reports route errors with the runtime's JSON" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    var test_tmp = try db.TestDirectoryType.init("capi-inference");
    defer test_tmp.cleanup();
    const models_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}-models", .{test_tmp.path()});
    defer std.testing.allocator.free(models_dir);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, models_dir);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, models_dir) catch {};
    var options: capi.InferenceOptions = .{ .models_dir = testSlice(models_dir) };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(&options, &handle));
    defer antfly_inference_close(handle);

    var models: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_list_models_json(handle, &models));
    defer db.antfly_buffer_free(&models);
    // The models directory is empty.
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, testBuffer(models), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.object.get("data").?.array.items.len);

    var failure: capi.Buffer = .{};
    const request = "{\"model\":\"nobody/no-such-model\",\"input\":[\"hello\"]}";
    const code = antfly_inference_embed_json(handle, testSlice(request), &failure);
    defer db.antfly_buffer_free(&failure);
    try std.testing.expectEqual(capi.ErrorCode.not_found, code);
    try std.testing.expect(std.mem.indexOf(u8, testBuffer(failure), "MODEL_NOT_FOUND") != null);

    var malformed: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_rerank_json(handle, testSlice("{"), &malformed));
    db.antfly_buffer_free(&malformed);
}

// Needs the model pulled locally (`antfly inference pull
// Qwen/Qwen3-Embedding-0.6B-GGUF`); skipped otherwise so it never downloads.
test "capi inference embeds text with a local model" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    if (!modelInstalled("Qwen", "Qwen3-Embedding-0.6B-GGUF")) return error.SkipZigTest;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(null, &handle));
    defer antfly_inference_close(handle);

    var out: capi.Buffer = .{};
    const request = "{\"model\":\"Qwen/Qwen3-Embedding-0.6B-GGUF\",\"input\":[\"antfly\",\"embedded inference\"]}";
    const code = antfly_inference_embed_json(handle, testSlice(request), &out);
    defer db.antfly_buffer_free(&out);
    if (code != .ok) std.debug.print("embed failed: {s}\n", .{testBuffer(out)});
    try std.testing.expectEqual(capi.ErrorCode.ok, code);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, testBuffer(out), .{});
    defer parsed.deinit();
    const data = parsed.value.object.get("data").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), data.len);
    try std.testing.expect(data[0].object.get("embedding").?.array.items.len > 0);
}

// Needs the model pulled locally (`antfly inference pull
// cross-encoder/ms-marco-MiniLM-L6-v2`); skipped otherwise.
test "capi inference reranks prompts with a local model" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    if (!modelInstalled("cross-encoder", "ms-marco-MiniLM-L6-v2")) return error.SkipZigTest;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(null, &handle));
    defer antfly_inference_close(handle);

    var out: capi.Buffer = .{};
    const request =
        \\{"model":"cross-encoder/ms-marco-MiniLM-L6-v2","query":"what do ants eat",
        \\ "prompts":["Ants eat sugar, seeds, and other insects.","The stock market fell today."]}
    ;
    const code = antfly_inference_rerank_json(handle, testSlice(request), &out);
    defer db.antfly_buffer_free(&out);
    if (code != .ok) std.debug.print("rerank failed: {s}\n", .{testBuffer(out)});
    try std.testing.expectEqual(capi.ErrorCode.ok, code);
    try std.testing.expect(std.mem.indexOf(u8, testBuffer(out), "score") != null);
}

test "capi inference chunks text without a model" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(null, &handle));
    defer antfly_inference_close(handle);

    var out: capi.Buffer = .{};
    const request = "{\"input\":\"Ants live in colonies. Each colony has a queen. Workers gather food for the nest.\"}";
    const code = antfly_inference_chunk_json(handle, testSlice(request), &out);
    defer db.antfly_buffer_free(&out);
    if (code != .ok) std.debug.print("chunk failed: {s}\n", .{testBuffer(out)});
    try std.testing.expectEqual(capi.ErrorCode.ok, code);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, testBuffer(out), .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.object.get("data").?.array.items.len > 0);
}

test "capi inference generates text with a local model and rejects streaming" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(null, &handle));
    defer antfly_inference_close(handle);

    var streamed: capi.Buffer = .{};
    const stream_request = "{\"model\":\"any\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"stream\":true}";
    try std.testing.expectEqual(capi.ErrorCode.invalid_argument, antfly_inference_generate_json(handle, testSlice(stream_request), &streamed));
    defer db.antfly_buffer_free(&streamed);
    try std.testing.expect(std.mem.indexOf(u8, testBuffer(streamed), "STREAMING_UNSUPPORTED") != null);

    // Needs the model pulled locally (`antfly inference pull
    // ggml-org/gemma-4-e2b-it-gguf`); skipped otherwise.
    if (!modelInstalled("ggml-org", "gemma-4-e2b-it-gguf")) return error.SkipZigTest;
    var out: capi.Buffer = .{};
    const request = "{\"model\":\"ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0\",\"messages\":[{\"role\":\"user\",\"content\":\"Reply with the single word: ready\"}],\"max_tokens\":8,\"stream\":false}";
    const code = antfly_inference_generate_json(handle, testSlice(request), &out);
    defer db.antfly_buffer_free(&out);
    if (code != .ok) std.debug.print("generate failed: {s}\n", .{testBuffer(out)});
    try std.testing.expectEqual(capi.ErrorCode.ok, code);
    try std.testing.expect(std.mem.indexOf(u8, testBuffer(out), "choices") != null);

    var batch: capi.Buffer = .{};
    const batch_request =
        \\{"requests":[
        \\ {"custom_id":"a","body":{"model":"ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0","messages":[{"role":"user","content":"Say one"}],"max_tokens":4}},
        \\ {"custom_id":"b","body":{"model":"ggml-org/gemma-4-e2b-it-gguf:gguf:Q4_0","messages":[{"role":"user","content":"Say two"}],"max_tokens":4}}]}
    ;
    const batch_code = antfly_inference_generate_batch_json(handle, testSlice(batch_request), &batch);
    defer db.antfly_buffer_free(&batch);
    if (batch_code != .ok) std.debug.print("generate batch failed: {s}\n", .{testBuffer(batch)});
    try std.testing.expectEqual(capi.ErrorCode.ok, batch_code);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, testBuffer(batch), .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("data").?.array.items.len);
}

test "capi inference pull rejects invalid requests with a JSON error" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(null, &handle));
    defer antfly_inference_close(handle);

    for ([_][]const u8{ "{}", "{\"model\":\"\"}", "{\"model\":\"a/b\",\"unknown\":1}", "{\"model\":\"not a ref\"}" }) |request| {
        var out: capi.Buffer = .{};
        const code = antfly_inference_pull_json(handle, testSlice(request), null, null, &out);
        defer db.antfly_buffer_free(&out);
        try std.testing.expectEqual(capi.ErrorCode.invalid_argument, code);
        try std.testing.expect(std.mem.indexOf(u8, testBuffer(out), "\"error\"") != null);
    }
}

// Downloads from the model hub, so it runs only when
// ANTFLY_INFERENCE_PULL_TEST_MODEL names a (small) model to pull, e.g.
// sparse-encoder-testing/splade-bert-tiny-nq-onnx.
test "capi inference pulls a model with progress into the handle's models directory" {
    if (!db.localInferenceRuntimeAvailable()) return error.SkipZigTest;
    const model = std.mem.span(std.c.getenv("ANTFLY_INFERENCE_PULL_TEST_MODEL") orelse return error.SkipZigTest);
    var test_tmp = try db.TestDirectoryType.init("capi-inference-pull");
    defer test_tmp.cleanup();
    const models_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}-models", .{test_tmp.path()});
    defer std.testing.allocator.free(models_dir);
    try std.Io.Dir.cwd().createDirPath(std.testing.io, models_dir);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, models_dir) catch {};

    var options: capi.InferenceOptions = .{ .models_dir = testSlice(models_dir) };
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_open(&options, &handle));
    defer antfly_inference_close(handle);

    const Progress = struct {
        reports: usize = 0,
        saw_model: bool = false,
        fn report(raw: ?*anyopaque, progress: *const capi.InferencePullProgress) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.reports += 1;
            if (progress.model.len > 0 and progress.file.len > 0) self.saw_model = true;
        }
    };
    var progress = Progress{};
    const request = try std.fmt.allocPrint(std.testing.allocator, "{{\"model\":\"{s}\"}}", .{model});
    defer std.testing.allocator.free(request);
    var out: capi.Buffer = .{};
    const code = antfly_inference_pull_json(handle, testSlice(request), Progress.report, &progress, &out);
    defer db.antfly_buffer_free(&out);
    if (code != .ok) std.debug.print("pull failed: {s}\n", .{testBuffer(out)});
    try std.testing.expectEqual(capi.ErrorCode.ok, code);
    try std.testing.expect(progress.reports > 0 and progress.saw_model);

    // The same handle sees the pulled model.
    var models: capi.Buffer = .{};
    try std.testing.expectEqual(capi.ErrorCode.ok, antfly_inference_list_models_json(handle, &models));
    defer db.antfly_buffer_free(&models);
    try std.testing.expect(std.mem.indexOf(u8, testBuffer(models), model) != null);
}
