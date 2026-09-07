// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const generating_runtime = @import("generating/mod.zig");
const managed_embedder = @import("inference/managed_embedder.zig");
const common_secrets = @import("common/secrets.zig");
const CancellationToken = @import("common/cancellation.zig").CancellationToken;
const readers = @import("antfly_readers");
const reader_config = @import("antfly_reader_config");
const transcribing = @import("antfly_transcribing");
const extracting = @import("antfly_extracting");
const extraction_api = @import("antfly_extraction_openapi");
const asset_producer = @import("storage/db/enrichment/asset_producer.zig");
const inference_work = @import("inference/work.zig");
const remote_capabilities = @import("inference/remote_capabilities.zig");
const execution_context = @import("inference/execution_context.zig");
const RequestContext = execution_context.RequestContext;

const provider_limits = @import("common/provider_limits.zig");
const Allocator = std.mem.Allocator;
const local_reader_batch_ceiling: usize = 64;
const test_png_data_uri = "data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD";
const test_png_2x3 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
};
const test_png_3x3 = [_]u8{
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x03,
};
const max_asset_provider_timeout_ms: u64 = 300_000;
const default_asset_provider_response_bytes: usize = 64 << 20;
const max_asset_http_response_bytes: usize = 64 << 20;
const invocation_response_resident_multiplier: usize = 4;
// This is an enforced allocator budget for request construction and parsing,
// not an estimate of what a particular JSON implementation happens to use.
const invocation_nonmedia_allocator_multiplier: usize = 8;
const invocation_control_bytes_per_item: usize = 4096;
const default_provider_response_envelope_bytes: usize = 1 << 20;
// A JSON string may encode one logical byte as a six-byte \u00XX escape. Use
// the task-neutral worst case until a provider publishes a tighter wire codec.
const provider_json_result_wire_multiplier: usize = 6;

pub const ResultLimits = struct {
    reader_bytes_per_item: usize = 256 << 10,
    generator_bytes_per_item: usize = 1 << 20,
    extractor_bytes_per_item: usize = 4 << 20,
    transcriber_bytes_per_item: usize = 4 << 20,
    copy_bytes_per_item: usize = 16 << 20,
    document_extraction_bytes_per_item: usize = 16 << 20,

    fn forProducer(self: ResultLimits, producer_type: asset_producer.ProducerType) usize {
        return switch (producer_type) {
            .reader => self.reader_bytes_per_item,
            .generator => self.generator_bytes_per_item,
            .extractor => self.extractor_bytes_per_item,
            .transcriber => self.transcriber_bytes_per_item,
            .copy => self.copy_bytes_per_item,
            .document_extraction => self.document_extraction_bytes_per_item,
        };
    }
};
fn mergeReaderExecution(report: *inference_work.ExecutionReport, chunk: readers.BatchExecution) !void {
    report.requested_items = std.math.add(usize, report.requested_items, chunk.requested_items) catch
        return error.InvalidReadExecutionReport;
    report.native_batches = std.math.add(usize, report.native_batches, chunk.native_batches) catch
        return error.InvalidReadExecutionReport;
    report.native_items = std.math.add(usize, report.native_items, chunk.native_items) catch
        return error.InvalidReadExecutionReport;
    report.serial_items = std.math.add(usize, report.serial_items, chunk.serial_items) catch
        return error.InvalidReadExecutionReport;
    report.fallback_items = std.math.add(usize, report.fallback_items, chunk.fallback_items) catch
        return error.InvalidReadExecutionReport;
    if (chunk.fallback_items > 0)
        report.fallback_reason = chunk.fallback_reason orelse "reader_fallback";
}

test "reader execution report preserves mixed native and fallback completion" {
    var report = inference_work.ExecutionReport{};
    try mergeReaderExecution(&report, .{
        .requested_items = 2,
        .native_batches = 1,
        .native_items = 1,
        .serial_items = 1,
        .fallback_items = 1,
        .fallback_reason = "native_batch_failed",
    });
    try mergeReaderExecution(&report, .{
        .requested_items = 2,
        .native_batches = 1,
        .native_items = 1,
        .serial_items = 1,
    });
    try report.validate();
    try std.testing.expectEqual(@as(usize, 4), report.requested_items);
    try std.testing.expectEqual(@as(usize, 2), report.native_items);
    try std.testing.expectEqual(@as(usize, 2), report.serial_items);
    try std.testing.expectEqual(@as(usize, 1), report.fallback_items);
    try std.testing.expectEqual(@as(usize, 2), report.native_batches);
    try std.testing.expectEqualStrings("native_batch_failed", report.fallback_reason.?);
}

test "asset producer runtime derives coherent logical and wire result ceilings" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{
        .keep_alive = false,
        .max_response_size = 64,
    });
    defer client.deinit();
    var limits = ResultLimits{};
    limits.generator_bytes_per_item = 10;
    var runtime = Runtime.initWithOptions(alloc, &client, .{
        .max_provider_response_bytes = 64,
        .provider_response_envelope_bytes = 8,
        .result_limits = limits,
    });
    defer runtime.deinit();

    const request = asset_producer.Request{
        .producer_type = .generator,
        .config_json = "{\"provider\":\"openai\",\"model\":\"vision\",\"url\":\"https://example.test/v1\"}",
        .source_text = "prompt",
    };
    const plan = try runtime.producer().invocationMemoryForRequests(alloc, &.{request});
    try std.testing.expectEqual(@as(usize, 9), plan.max_result_bytes);
    try std.testing.expectEqual(@as(usize, 64), runtime.responseLimitForTask(.generator, 1));
    var different_task = request;
    different_task.producer_type = .copy;
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        runtime.producer().invocationMemoryForRequests(alloc, &.{ request, different_task }),
    );
}

test "asset producer runtime applies result ceilings to non-model producers" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();
    var limits = ResultLimits{};
    limits.copy_bytes_per_item = 4;
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .result_limits = limits });
    defer runtime.deinit();

    try std.testing.expectError(
        error.InferenceResultTooLarge,
        runtime.producer().produce(alloc, .{
            .producer_type = .copy,
            .config_json = "",
            .source_text = "12345",
        }),
    );
}

fn localInvocationAllocatorOwner(
    provider: managed_embedder.AntflyProvider,
) !inference_work.InvocationAllocatorOwner {
    if (!provider.owns_invocation_admission)
        return error.InferenceInvocationMemoryUnavailable;
    return .executor;
}

test "asset producer runtime local invocation ownership fails closed without executor admission" {
    var context: u8 = 0;
    var provider = managed_embedder.AntflyProvider{
        .ptr = &context,
        .embed_dense_texts = undefined,
        .embed_sparse_texts = undefined,
    };
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        localInvocationAllocatorOwner(provider),
    );
    provider.owns_invocation_admission = true;
    try std.testing.expectEqual(
        inference_work.InvocationAllocatorOwner.executor,
        try localInvocationAllocatorOwner(provider),
    );
}

pub const Runtime = struct {
    alloc: Allocator,
    http: *httpx.Client,
    capability_cache: remote_capabilities.Cache,
    execution: execution_context.Context = .{},
    request_progress: ?execution_context.ProgressSink = null,
    owned_http: ?*httpx.Client = null,
    owned_default_endpoint: ?[]u8 = null,
    owned_source_table: ?[]u8 = null,
    limits: *provider_limits.Registry = &provider_limits.process_registry,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    inference_api_url: ?[]const u8 = null,
    owns_inference_api_url: bool = false,
    secret_store: ?*common_secrets.FileStore = null,
    max_provider_response_bytes: usize = default_asset_provider_response_bytes,
    provider_response_envelope_bytes: usize = default_provider_response_envelope_bytes,
    result_limits: ResultLimits = .{},

    pub const Options = struct {
        limits: *provider_limits.Registry = &provider_limits.process_registry,
        antfly_provider: ?managed_embedder.AntflyProvider = null,
        /// Process-isolated Antfly inference endpoint. When no local provider
        /// is installed, Antfly asset configs without their own URL inherit
        /// this endpoint instead of falling back to an unrelated localhost
        /// default.
        inference_api_url: ?[]const u8 = null,
        secret_store: ?*common_secrets.FileStore = null,
        max_provider_response_bytes: usize = default_asset_provider_response_bytes,
        provider_response_envelope_bytes: usize = default_provider_response_envelope_bytes,
        result_limits: ResultLimits = .{},
        remote_capability_cache: ?*remote_capabilities.Cache = null,
        source_table: []const u8 = "",
    };

    pub fn init(alloc: Allocator, http: *httpx.Client) Runtime {
        return initWithOptions(alloc, http, .{});
    }

    pub fn initWithOptions(alloc: Allocator, http: *httpx.Client, options: Options) Runtime {
        return .{
            .alloc = alloc,
            .http = http,
            .capability_cache = remote_capabilities.Cache.init(alloc, http.io),
            .execution = .{
                .default_endpoint = options.inference_api_url,
                .capability_cache = options.remote_capability_cache,
                .http_client = http,
                .io = http.io,
                .routing = .{ .source_table = options.source_table },
            },
            .limits = options.limits,
            .antfly_provider = options.antfly_provider,
            .inference_api_url = options.inference_api_url,
            .secret_store = options.secret_store,
            .max_provider_response_bytes = options.max_provider_response_bytes,
            .provider_response_envelope_bytes = options.provider_response_envelope_bytes,
            .result_limits = options.result_limits,
        };
    }

    pub fn createOwned(alloc: Allocator, io: std.Io, options: Options) !*Runtime {
        const runtime = try alloc.create(Runtime);
        errdefer alloc.destroy(runtime);

        const client = try alloc.create(httpx.Client);
        errdefer alloc.destroy(client);
        // This runtime is the ownership boundary for remote inference
        // transport, so its pool lives exactly as long as the producer and is
        // reused across document windows. The httpx pools are internally
        // synchronized; disable ambient cookies because model credentials are
        // supplied explicitly per request.
        var client_config = httpx.ClientConfig{
            .keep_alive = true,
            .cookies_enabled = false,
            .cache_resolved_addresses = true,
        };
        // Operation-specific ceilings are applied on provider requests. This
        // client-wide value is only an outer safety maximum and must not turn a
        // catalog or a configured large extraction into an unrelated 4 MiB
        // failure.
        client_config.max_response_size = max_asset_http_response_bytes;
        client_config.timeouts = httpx.Timeouts.uniform(max_asset_provider_timeout_ms);
        client_config.timeouts.request_ms = max_asset_provider_timeout_ms;
        // Compatibility waves and independent document workers share this
        // transport concurrently. httpx uses its client allocator for
        // request-local headers, URLs, and responses, so keep those allocations
        // off a possibly arena-backed runtime owner.
        client.* = httpx.Client.initWithConfig(std.heap.smp_allocator, io, client_config);
        errdefer client.deinit();

        const owned_default_endpoint = if (options.inference_api_url) |endpoint|
            try normalizeAntflyInferenceBaseUrl(alloc, endpoint)
        else
            null;
        errdefer if (owned_default_endpoint) |endpoint| alloc.free(endpoint);
        const owned_source_table = if (options.source_table.len > 0)
            try alloc.dupe(u8, options.source_table)
        else
            null;
        errdefer if (owned_source_table) |source_table| alloc.free(source_table);

        runtime.* = Runtime.initWithOptions(alloc, client, options);
        runtime.owned_http = client;
        runtime.owned_default_endpoint = owned_default_endpoint;
        runtime.owned_source_table = owned_source_table;
        runtime.execution.default_endpoint = owned_default_endpoint;
        runtime.execution.routing.source_table = owned_source_table orelse "";
        runtime.inference_api_url = owned_default_endpoint;
        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        self.capability_cache.deinit();
        if (self.owned_default_endpoint) |endpoint| self.alloc.free(endpoint);
        if (self.owned_source_table) |source_table| self.alloc.free(source_table);
        if (self.owns_inference_api_url) {
            if (self.inference_api_url) |owned| self.alloc.free(owned);
            self.inference_api_url = null;
            self.owns_inference_api_url = false;
        }
        if (self.owned_http) |client| {
            client.deinit();
            self.alloc.destroy(client);
            self.owned_http = null;
        }
        self.* = undefined;
    }

    fn capabilityCache(self: *Runtime) *remote_capabilities.Cache {
        return self.execution.capability_cache orelse &self.capability_cache;
    }

    fn requestContext(self: *const Runtime) RequestContext {
        return .{
            .io = self.execution.io orelse self.http.io,
            .deadline_ns = self.execution.deadline_ns,
            .cancellation = if (self.execution.cancellation.ptr != null)
                self.execution.cancellation
            else
                null,
            .progress = self.request_progress,
        };
    }

    fn linkedModelCapabilities(
        self: *const Runtime,
        alloc: Allocator,
        provider: managed_embedder.AntflyProvider,
        model: []const u8,
        task: inference_work.Task,
    ) !?inference_work.InferenceCapabilities {
        if (provider.model_capabilities_with_context) |resolve|
            return try managed_embedder.AntflyProviderBoundary.call(
                "model_capabilities_with_context",
                provider.boundary_dispatch,
                resolve,
                .{ provider.ptr, alloc, model, task, self.requestContext() },
            );
        const resolve = provider.model_capabilities orelse return null;
        return try managed_embedder.AntflyProviderBoundary.call(
            "model_capabilities",
            provider.boundary_dispatch,
            resolve,
            .{ provider.ptr, alloc, model, task },
        );
    }

    fn linkedReaderAvailable(self: *const Runtime) bool {
        const provider = self.antfly_provider orelse return false;
        return provider.read_images != null or provider.read_encoded_images != null or
            provider.read_encoded_images_reported != null or provider.read_images_with_context != null or
            provider.read_encoded_images_with_context != null or
            provider.read_encoded_images_reported_with_context != null or
            provider.read_raster_images_reported != null or
            provider.read_raster_images_reported_with_context != null;
    }

    fn linkedGeneratorAvailable(self: *const Runtime) bool {
        const provider = self.antfly_provider orelse return false;
        return provider.generate_messages != null or provider.generate_text != null or
            provider.generate_messages_with_attachments != null or
            provider.generate_messages_with_context != null or
            provider.generate_text_with_context != null or
            provider.generate_messages_with_attachments_with_context != null;
    }

    fn linkedExtractorAvailable(self: *const Runtime) bool {
        const provider = self.antfly_provider orelse return false;
        return provider.extract != null or provider.extract_with_context != null;
    }

    fn linkedTranscriberAvailable(self: *const Runtime) bool {
        const provider = self.antfly_provider orelse return false;
        return provider.transcribe_audio != null or provider.transcribe_audio_with_context != null;
    }

    fn routedReaderConfig(self: *const Runtime, cfg: readers.Config) readers.Config {
        if (cfg.provider != .antfly) return cfg;
        var routed = cfg;
        routed.url = self.execution.resolveAntflyEndpoint(cfg.resolvedUrl(), self.linkedReaderAvailable());
        routed.api_url = null;
        return routed;
    }

    fn routedTranscriberConfig(self: *const Runtime, cfg: transcribing.Config) transcribing.Config {
        if (cfg.provider != .antfly) return cfg;
        var routed = cfg;
        routed.url = self.execution.resolveAntflyEndpoint(cfg.resolvedUrl(), self.linkedTranscriberAvailable());
        routed.api_url = null;
        return routed;
    }

    fn routeGeneratorConfig(self: *const Runtime, alloc: Allocator, cfg: *generating_runtime.GeneratorConfig) !void {
        if (cfg.provider != .antfly or cfg.url.len > 0 or self.linkedGeneratorAvailable()) return;
        const endpoint = self.execution.resolveAntflyEndpoint(null, false) orelse return;
        cfg.url = try alloc.dupe(u8, endpoint);
    }

    fn routeExtractorConfig(self: *const Runtime, alloc: Allocator, cfg: *extracting.Config) !void {
        if (cfg.provider != .antfly or cfg.resolvedUrl() != null or self.linkedExtractorAvailable()) return;
        const endpoint = self.execution.resolveAntflyEndpoint(null, false) orelse return;
        cfg.url = try alloc.dupe(u8, endpoint);
    }

    pub fn producer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            // A caller-owned HTTP client may not have a finite request timeout,
            // so this borrowed interface cannot advertise the foreground
            // liveness contract.
            .vtable = &.{
                .produce = produce,
                .produce_with_context = produceWithContext,
                .produce_batch = produceBatch,
                .produce_batch_with_context = produceBatchWithContext,
                .produce_batch_reported = produceBatchReported,
                .produce_batch_reported_with_context = produceBatchReportedWithContext,
                .batch_mode = batchMode,
                .batch_mode_with_context = batchModeWithContext,
                .can_produce_batch = canProduceBatch,
                .capabilities_for_requests = capabilitiesForRequests,
                .capabilities_for_requests_with_context = capabilitiesForRequestsWithContext,
                .invocation_memory_for_requests = invocationMemoryForRequests,
                .invocation_memory_for_requests_with_context = invocationMemoryForRequestsWithContext,
                .produce_borrowed_raster_batch_reported = produceBorrowedRasterBatchReported,
                .produce_borrowed_raster_batch_reported_with_context = produceBorrowedRasterBatchReportedWithContext,
                .borrowed_raster_batch_available = borrowedRasterBatchAvailable,
                .borrowed_raster_batch_available_with_context = borrowedRasterBatchAvailableWithContext,
            },
        };
    }

    pub fn ownedProducer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            .vtable = &.{
                .produce = produce,
                .produce_with_context = produceWithContext,
                .produce_batch = produceBatch,
                .produce_batch_with_context = produceBatchWithContext,
                .produce_batch_reported = produceBatchReported,
                .produce_batch_reported_with_context = produceBatchReportedWithContext,
                .batch_mode = batchMode,
                .batch_mode_with_context = batchModeWithContext,
                .can_produce_batch = canProduceBatch,
                .capabilities_for_requests = capabilitiesForRequests,
                .capabilities_for_requests_with_context = capabilitiesForRequestsWithContext,
                .invocation_memory_for_requests = invocationMemoryForRequests,
                .invocation_memory_for_requests_with_context = invocationMemoryForRequestsWithContext,
                .produce_borrowed_raster_batch_reported = produceBorrowedRasterBatchReported,
                .produce_borrowed_raster_batch_reported_with_context = produceBorrowedRasterBatchReportedWithContext,
                .borrowed_raster_batch_available = borrowedRasterBatchAvailable,
                .borrowed_raster_batch_available_with_context = borrowedRasterBatchAvailableWithContext,
                .deinit = deinitProducer,
                .foreground_bounded = true,
                .foreground_bounded_for_requests = foregroundBoundedForRequests,
            },
        };
    }

    fn foregroundBoundedForRequests(
        ptr: *anyopaque,
        alloc: Allocator,
        requests: []const asset_producer.Request,
    ) !bool {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        for (requests, 0..) |request, i| {
            // Routing depends only on the producer type and configuration.
            // Avoid reparsing the overwhelmingly common homogeneous batch.
            if (i > 0 and request.producer_type == requests[i - 1].producer_type and
                std.mem.eql(u8, request.config_json, requests[i - 1].config_json)) continue;
            if (!try self.requestForegroundBounded(alloc, request)) return false;
        }
        return true;
    }

    fn invocationMemoryForRequests(
        ptr: *anyopaque,
        alloc: Allocator,
        requests: []const asset_producer.Request,
    ) !inference_work.InvocationMemoryPlan {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0) return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = 0 };
        if (!requestsShareRoute(requests)) return error.InferenceInvocationMemoryUnavailable;

        var nonmedia_bytes: usize = 0;
        for (requests) |request| {
            nonmedia_bytes = std.math.add(usize, nonmedia_bytes, request.config_json.len) catch
                return error.InferenceEncodedBytesExceeded;
            nonmedia_bytes = std.math.add(usize, nonmedia_bytes, request.source_text.len) catch
                return error.InferenceEncodedBytesExceeded;
            if (request.source_parts_json) |parts| nonmedia_bytes = std.math.add(usize, nonmedia_bytes, parts.len) catch
                return error.InferenceEncodedBytesExceeded;
        }
        var remote = false;
        var allocator_owner: inference_work.InvocationAllocatorOwner = .caller;
        const transport: inference_work.AttachmentTransport = switch (requests[0].producer_type) {
            .copy, .document_extraction => .borrowed_binary,
            .reader => blk: {
                var parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const cfg = self.routedReaderConfig(parsed.value);
                try cfg.validate();
                remote = !isLocalReaderProvider(cfg.provider, cfg.resolvedUrl());
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    allocator_owner = try localInvocationAllocatorOwner(local);
                    const binary = local.read_encoded_images != null or local.read_encoded_images_reported != null or
                        local.read_encoded_images_with_context != null or
                        local.read_encoded_images_reported_with_context != null or
                        local.read_raster_images_reported != null or
                        local.read_raster_images_reported_with_context != null;
                    if (!binary and local.read_images == null and local.read_images_with_context == null)
                        return error.InferenceInvocationMemoryUnavailable;
                    break :blk if (binary) .borrowed_binary else .data_uri;
                }
                break :blk remoteAttachmentTransport(try self.readerCapabilities(alloc, cfg), .data_uri);
            },
            .generator => blk: {
                var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
                defer parsed.deinit(alloc);
                try self.routeGeneratorConfig(alloc, &parsed.generator);
                remote = parsed.generator.url.len > 0 or parsed.generator.provider != .antfly;
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    allocator_owner = try localInvocationAllocatorOwner(local);
                    if (local.generate_messages_with_attachments != null or
                        local.generate_messages_with_attachments_with_context != null) break :blk .borrowed_binary;
                    if (local.generate_messages == null and local.generate_messages_with_context == null)
                        return error.InferenceInvocationMemoryUnavailable;
                    break :blk .data_uri;
                }
                // Use the same model/task/auth-scoped capability cache as
                // execution. A remote node owns its model memory; this host
                // owns only the selected transport and bounded response.
                if (!self.canGenerateBatchWithConfig(parsed, requests)) break :blk .data_uri;
                const caps = try self.generatorCapabilities(alloc, parsed.generator);
                break :blk remoteAttachmentTransport(caps, if (caps == null) .data_uri else .base64_payload);
            },
            .extractor => blk: {
                var parsed = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
                defer parsed.deinit(alloc);
                try self.routeExtractorConfig(alloc, &parsed);
                remote = !isLocalExtractionProvider(parsed.provider, parsed.resolvedUrl());
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    allocator_owner = try localInvocationAllocatorOwner(local);
                    if (local.extract == null and local.extract_with_context == null)
                        return error.InferenceInvocationMemoryUnavailable;
                }
                break :blk if (remote) remoteAttachmentTransport(try self.extractorCapabilities(alloc, parsed), extractorAttachmentTransport(parsed)) else extractorAttachmentTransport(parsed);
            },
            .transcriber => blk: {
                var parsed = try std.json.parseFromSlice(transcribing.Config, alloc, requests[0].config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const cfg = self.routedTranscriberConfig(parsed.value);
                remote = !isLocalTranscriberProvider(cfg.provider, cfg.resolvedUrl());
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    allocator_owner = try localInvocationAllocatorOwner(local);
                    if (local.transcribe_audio == null and local.transcribe_audio_with_context == null)
                        return error.InferenceInvocationMemoryUnavailable;
                }
                const inline_source = inference_work.hasDataUriScheme(requests[0].source_text);
                for (requests[1..]) |request| {
                    if (inference_work.hasDataUriScheme(request.source_text) != inline_source)
                        return error.InferenceInvocationMemoryUnavailable;
                }
                break :blk if (inline_source)
                    .data_uri
                else
                    .borrowed_binary;
            },
        };

        var fixed = std.math.mul(usize, nonmedia_bytes, invocation_nonmedia_allocator_multiplier) catch
            return error.InferenceEncodedBytesExceeded;
        const control = std.math.mul(usize, requests.len, invocation_control_bytes_per_item) catch
            return error.InferenceEncodedBytesExceeded;
        fixed = std.math.add(usize, fixed, control) catch return error.InferenceEncodedBytesExceeded;
        const configured_results = try self.configuredResultLimit(requests[0].producer_type, requests.len);
        const results = if (remote)
            self.remoteLogicalResultLimit(requests[0].producer_type, requests.len)
        else
            configured_results;
        const result_per_item = @min(
            self.result_limits.forProducer(requests[0].producer_type),
            results,
        );
        if (results == 0) return error.InvalidInferenceInvocationMemory;
        fixed = std.math.add(usize, fixed, results) catch return error.InferenceEncodedBytesExceeded;
        // Both HTTP routes and the linked inference-node ABI serialize a
        // bounded response and materialize typed caller-owned results. Model
        // memory is executor-owned for the linked route, but its request and
        // result bridge remains part of this boundary's hard allocator cap.
        const response_limit = self.responseLimitForTask(requests[0].producer_type, requests.len);
        const parser_and_copy_limit = std.math.mul(
            usize,
            response_limit,
            invocation_response_resident_multiplier - 1,
        ) catch return error.InferenceEncodedBytesExceeded;
        const allocator_limit = std.math.add(usize, fixed, parser_and_copy_limit) catch
            return error.InferenceEncodedBytesExceeded;
        const response_peak = std.math.mul(
            usize,
            response_limit,
            invocation_response_resident_multiplier,
        ) catch return error.InferenceEncodedBytesExceeded;
        fixed = std.math.add(usize, fixed, response_peak) catch return error.InferenceEncodedBytesExceeded;
        return .{
            .attachment_transport = transport,
            .fixed_bytes = fixed,
            .allocator_limit_bytes = allocator_limit,
            .allocator_owner = allocator_owner,
            .max_result_bytes_per_item = result_per_item,
            .max_result_bytes = results,
        };
    }

    fn responseLimitForTask(
        self: *const Runtime,
        producer_type: asset_producer.ProducerType,
        item_count: usize,
    ) usize {
        const result_bytes = self.configuredResultLimit(producer_type, item_count) catch
            std.math.maxInt(usize);
        const encoded_results = std.math.mul(
            usize,
            result_bytes,
            provider_json_result_wire_multiplier,
        ) catch std.math.maxInt(usize);
        const envelope = std.math.add(
            usize,
            encoded_results,
            self.provider_response_envelope_bytes,
        ) catch std.math.maxInt(usize);
        return self.execution.boundedResponseBytes(
            @min(self.http.maxResponseSize(), @min(self.max_provider_response_bytes, envelope)),
        );
    }

    fn configuredResultLimit(
        self: *const Runtime,
        producer_type: asset_producer.ProducerType,
        item_count: usize,
    ) !usize {
        return std.math.mul(
            usize,
            @max(item_count, 1),
            self.result_limits.forProducer(producer_type),
        ) catch error.InferenceEncodedBytesExceeded;
    }

    fn remoteLogicalResultLimit(
        self: *const Runtime,
        producer_type: asset_producer.ProducerType,
        item_count: usize,
    ) usize {
        const configured = self.configuredResultLimit(producer_type, item_count) catch
            std.math.maxInt(usize);
        const response_limit = self.responseLimitForTask(producer_type, item_count);
        const wire_capacity = response_limit -| self.provider_response_envelope_bytes;
        return @min(configured, wire_capacity / provider_json_result_wire_multiplier);
    }

    fn requestForegroundBounded(self: *Runtime, alloc: Allocator, request: asset_producer.Request) !bool {
        return switch (request.producer_type) {
            // These routes never enter an external callback. Unsupported
            // document extraction also fails synchronously in produceOne.
            .copy, .document_extraction => true,
            .generator => blk: {
                var parsed = try parseGeneratorProducerConfig(alloc, request.config_json);
                defer parsed.deinit(alloc);
                try self.routeGeneratorConfig(alloc, &parsed.generator);
                const local = self.antfly_provider orelse break :blk true;
                if (parsed.generator.provider != .antfly or parsed.generator.url.len != 0)
                    break :blk true;
                if (request.media.len > 0)
                    break :blk local.generate_messages_with_attachments_with_context != null;
                break :blk local.generate_messages_with_context != null or
                    (request.source_parts_json == null and local.generate_text_with_context != null);
            },
            .reader => blk: {
                var parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const cfg = self.routedReaderConfig(parsed.value);
                const local = self.antfly_provider orelse break :blk true;
                if (!isLocalReaderProvider(cfg.provider, cfg.resolvedUrl())) break :blk true;
                break :blk if (request.media.len > 0)
                    local.read_encoded_images_reported_with_context != null or
                        local.read_encoded_images_with_context != null
                else
                    local.read_images_with_context != null;
            },
            .transcriber => blk: {
                var parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const cfg = self.routedTranscriberConfig(parsed.value);
                const local = self.antfly_provider orelse break :blk true;
                if (!isLocalTranscriberProvider(cfg.provider, cfg.resolvedUrl())) break :blk true;
                break :blk local.transcribe_audio_with_context != null;
            },
            .extractor => blk: {
                var parsed = try extracting.parseConfigFromSlice(alloc, request.config_json);
                defer parsed.deinit(alloc);
                try self.routeExtractorConfig(alloc, &parsed);
                const local = self.antfly_provider orelse break :blk true;
                if (!isLocalExtractionProvider(parsed.provider, parsed.resolvedUrl())) break :blk true;
                break :blk local.extract_with_context != null;
            },
        };
    }

    fn deinitProducer(ptr: *anyopaque, alloc: Allocator) void {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        self.deinit();
        alloc.destroy(self);
    }

    fn produce(ptr: *anyopaque, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        return try self.produceOne(alloc, request);
    }

    const RuntimeInvocationCancellation = struct {
        configured: CancellationToken,
        invocation: CancellationToken,

        fn isCancelled(ptr: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            return self.configured.isCancelled() or self.invocation.isCancelled();
        }
    };

    fn invocationRuntime(
        ptr: *anyopaque,
        context: asset_producer.InvocationContext,
        cancellation: *RuntimeInvocationCancellation,
    ) Runtime {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        var scoped = self.*;
        // A copied Runtime must never operate on its copied cache mutex. Bind
        // every scoped call to the original synchronized cache explicitly.
        scoped.execution.capability_cache = self.capabilityCache();
        scoped.execution.io = context.io orelse self.execution.io;
        scoped.execution.deadline_ns = if (context.deadline_ns) |invocation_deadline|
            if (self.execution.deadline_ns) |configured_deadline| @min(invocation_deadline, configured_deadline) else invocation_deadline
        else
            self.execution.deadline_ns;
        cancellation.* = .{
            .configured = self.execution.cancellation,
            .invocation = context.cancellation,
        };
        scoped.execution.cancellation = .{
            .ptr = cancellation,
            .is_cancelled_fn = RuntimeInvocationCancellation.isCancelled,
        };
        scoped.execution.max_response_bytes = if (context.max_response_bytes) |requested|
            if (self.execution.max_response_bytes) |configured| @min(requested, configured) else requested
        else
            self.execution.max_response_bytes;
        scoped.request_progress = context.progress orelse self.request_progress;
        return scoped;
    }

    fn produceWithContext(ptr: *anyopaque, alloc: Allocator, request: asset_producer.Request, context: asset_producer.InvocationContext) ![]u8 {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        const output = try scoped.produceOne(alloc, request);
        errdefer alloc.free(output);
        try scoped.execution.check(platform.time.monotonicNs());
        return output;
    }

    fn produceBatchWithContext(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request, context: asset_producer.InvocationContext) ![][]u8 {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        const outputs = try produceBatch(&scoped, alloc, requests);
        errdefer {
            for (outputs) |output| if (output.len > 0) alloc.free(output);
            alloc.free(outputs);
        }
        try scoped.execution.check(platform.time.monotonicNs());
        return outputs;
    }

    fn produceBatchReportedWithContext(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request, context: asset_producer.InvocationContext) !asset_producer.ProducedBatch {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        var batch = try produceBatchReported(&scoped, alloc, requests);
        errdefer batch.deinit(alloc);
        try scoped.execution.check(platform.time.monotonicNs());
        return batch;
    }

    fn produceBorrowedRasterBatchReportedWithContext(
        ptr: *anyopaque,
        alloc: Allocator,
        requests: []const asset_producer.Request,
        rasters: []const readers.RasterImage,
        context: asset_producer.InvocationContext,
    ) !asset_producer.ProducedBatch {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        var batch = try produceBorrowedRasterBatchReported(&scoped, alloc, requests, rasters);
        errdefer batch.deinit(alloc);
        try scoped.execution.check(platform.time.monotonicNs());
        return batch;
    }

    fn borrowedRasterBatchAvailableWithContext(
        ptr: *anyopaque,
        alloc: Allocator,
        requests: []const asset_producer.Request,
        context: asset_producer.InvocationContext,
    ) !bool {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        const available = try borrowedRasterBatchAvailable(&scoped, alloc, requests);
        try scoped.execution.check(platform.time.monotonicNs());
        return available;
    }

    fn batchModeWithContext(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request, context: asset_producer.InvocationContext) !inference_work.BatchMode {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        const mode = try batchMode(&scoped, alloc, requests);
        try scoped.execution.check(platform.time.monotonicNs());
        return mode;
    }

    fn capabilitiesForRequestsWithContext(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request, context: asset_producer.InvocationContext) !?inference_work.InferenceCapabilities {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        const capabilities = try capabilitiesForRequests(&scoped, alloc, requests);
        try scoped.execution.check(platform.time.monotonicNs());
        return capabilities;
    }

    fn invocationMemoryForRequestsWithContext(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request, context: asset_producer.InvocationContext) !inference_work.InvocationMemoryPlan {
        var cancellation: RuntimeInvocationCancellation = undefined;
        var scoped = invocationRuntime(ptr, context, &cancellation);
        try scoped.execution.check(platform.time.monotonicNs());
        const plan = try invocationMemoryForRequests(&scoped, alloc, requests);
        try scoped.execution.check(platform.time.monotonicNs());
        return plan;
    }

    fn canProduceBatch(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        return try batchMode(ptr, alloc, requests) != .none;
    }

    fn capabilitiesForRequests(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !?inference_work.InferenceCapabilities {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0 or !requestsShareRoute(requests)) return null;
        return switch (requests[0].producer_type) {
            .reader => blk: {
                var cfg = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer cfg.deinit();
                break :blk try self.readerCapabilities(alloc, cfg.value);
            },
            .generator => blk: {
                var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
                defer parsed.deinit(alloc);
                try self.routeGeneratorConfig(alloc, &parsed.generator);
                break :blk try self.generatorCapabilities(alloc, parsed.generator);
            },
            .extractor => blk: {
                var cfg = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
                defer cfg.deinit(alloc);
                break :blk try self.extractorCapabilities(alloc, cfg);
            },
            else => null,
        };
    }

    fn batchMode(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !inference_work.BatchMode {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0) return .native;
        const first = requests[0];
        for (requests) |request| {
            if (request.producer_type != first.producer_type) return .none;
        }
        return switch (first.producer_type) {
            .copy => .native,
            .document_extraction => .none,
            .reader => blk: {
                if (!try self.canReadBatch(alloc, requests)) break :blk .none;
                var cfg = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer cfg.deinit();
                const capabilities = (try self.readerCapabilities(alloc, cfg.value)) orelse break :blk .none;
                break :blk capabilities.batch.mode;
            },
            // The HTTP endpoint and embedded boundary accept independent
            // multimodal requests but currently serialize projector/session
            // use for thread safety. Report that honestly until the resolved
            // model advertises a native multimodal generation batch.
            .generator => blk: {
                if (!try self.canGenerateBatch(alloc, requests)) break :blk .none;
                var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
                defer parsed.deinit(alloc);
                try self.routeGeneratorConfig(alloc, &parsed.generator);
                // The embedded callback is one invocation per item today.
                if (parsed.generator.url.len == 0) break :blk .serial_compatibility;
                const capabilities = (try capabilitiesForRequests(ptr, alloc, requests)) orelse
                    break :blk .serial_compatibility;
                break :blk capabilities.batch.mode;
            },
            // Transcription has one input per provider call. The compatibility
            // batch preserves result order and uses bounded concurrency only
            // for remote/stateless routes; linked model state stays serial.
            .transcriber => .serial_compatibility,
            .extractor => blk: {
                if (!try self.canExtractBatch(alloc, requests)) break :blk .none;
                const capabilities = (try capabilitiesForRequests(ptr, alloc, requests)) orelse break :blk .none;
                break :blk capabilities.batch.mode;
            },
        };
    }

    fn requestsShareRoute(requests: []const asset_producer.Request) bool {
        for (requests[1..]) |request| {
            if (request.producer_type != requests[0].producer_type) return false;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return false;
        }
        return true;
    }

    fn borrowedRasterBatchAvailable(
        ptr: *anyopaque,
        alloc: Allocator,
        requests: []const asset_producer.Request,
    ) !bool {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0 or !requestsShareRoute(requests)) return false;
        for (requests) |request| {
            if (request.producer_type != .reader or request.media.len != 0 or
                !request.inline_media_trusted) return false;
        }
        var parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        const cfg = self.routedReaderConfig(parsed.value);
        try cfg.validate();
        // Physical locality is checked before the descriptor. A distributed
        // node cannot opt a storage node into an in-process borrowed ABI by
        // publishing a logically valid but physically unusable capability.
        if (!isLocalReaderProvider(cfg.provider, cfg.resolvedUrl())) return false;
        const local = self.antfly_provider orelse return false;
        if (local.read_raster_images_reported == null and
            local.read_raster_images_reported_with_context == null) return false;
        const capabilities = (try self.readerCapabilities(alloc, cfg)) orelse return false;
        return capabilities.borrowed_rasters and
            capabilities.result_cardinality == .one_per_item and
            capabilities.supports(.{ .image = true }) and
            capabilities.batch.acceptsItems(requests.len);
    }

    fn canReadBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareRoute(requests)) return false;
        const uses_encoded_media = requests[0].media.len > 0;
        if (uses_encoded_media and !requests[0].inline_media_trusted) return false;
        for (requests[1..]) |request| {
            if (request.inline_media_trusted != requests[0].inline_media_trusted) return false;
            if ((request.media.len > 0) != uses_encoded_media) return false;
        }
        var cfg = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg.deinit();
        const capabilities = try self.readerCapabilities(alloc, cfg.value);
        if (capabilities == null or capabilities.?.batch.mode == .none or
            capabilities.?.result_cardinality != .one_per_item or
            !capabilities.?.supports(.{ .image = true })) return false;

        var owned_shared_prompt: ?[]u8 = null;
        defer if (owned_shared_prompt) |prompt| alloc.free(prompt);
        var shared_prompt: ?[]const u8 = cfg.value.prompt;
        for (requests, 0..) |request, i| {
            var source = try parseReaderSourceWithMetadataOnly(
                alloc,
                request.source_text,
                request.source_parts_json,
                request.media.len > 0,
            );
            defer source.deinit(alloc);
            if (uses_encoded_media) {
                if (request.media.len != 1) return false;
            } else if (source.images.len != 1) return false;
            const effective_prompt = source.prompt orelse cfg.value.prompt;
            if (i == 0) {
                if (source.prompt) |prompt| {
                    owned_shared_prompt = try alloc.dupe(u8, prompt);
                    shared_prompt = owned_shared_prompt;
                } else {
                    shared_prompt = cfg.value.prompt;
                }
            } else if (!optionalStringsEqual(shared_prompt, effective_prompt)) {
                return false;
            }
        }
        return true;
    }

    fn readerCapabilities(
        self: *Runtime,
        alloc: Allocator,
        raw_cfg: readers.Config,
    ) !?inference_work.InferenceCapabilities {
        const cfg = self.routedReaderConfig(raw_cfg);
        try cfg.validate();
        if (!isLocalReaderProvider(cfg.provider, cfg.resolvedUrl())) {
            if (cfg.provider != .antfly) return null;
            const endpoint = cfg.resolvedUrl() orelse return null;
            var auth_value: ?[]u8 = null;
            defer if (auth_value) |value| alloc.free(value);
            var header_storage: [2][2][]const u8 = undefined;
            var header_count: usize = 0;
            if (cfg.bearer_token orelse cfg.api_key) |token| {
                auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
                header_storage[header_count] = .{ "Authorization", auth_value.? };
                header_count += 1;
            }
            header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
            const headers = header_storage[0..header_count];
            return self.capabilityCache().getOrDiscoverWithContext(
                self.http,
                endpoint,
                cfg.model orelse "",
                .read,
                headers,
                self.execution.waitContext(),
            ) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => null,
            };
        }
        const local = self.antfly_provider orelse return null;
        const capabilities = (try self.linkedModelCapabilities(alloc, local, cfg.model orelse "", .read)) orelse return null;
        try capabilities.validate();
        if (capabilities.task != .read) return error.InvalidInferenceCapabilities;
        return capabilities;
    }

    fn canGenerateBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareRoute(requests)) return false;
        var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
        defer parsed.deinit(alloc);
        try self.routeGeneratorConfig(alloc, &parsed.generator);
        return self.canGenerateBatchWithConfig(parsed, requests);
    }

    // Planning already owns a parsed, routed configuration. Reuse it so the
    // bounded contract resolver need not retain a second JSON/config tree.
    fn canGenerateBatchWithConfig(self: *Runtime, parsed: GeneratorProducerConfig, requests: []const asset_producer.Request) bool {
        var all_have_media = true;
        for (requests) |request| {
            if (request.media.len > 0 and !request.inline_media_trusted) return false;
            all_have_media = all_have_media and request.media.len > 0;
        }
        const cfg = parsed.generator;
        const local_serial = all_have_media and cfg.provider == .antfly and cfg.url.len == 0 and
            self.antfly_provider != null and
            (self.antfly_provider.?.generate_messages_with_attachments_with_context != null or
                self.antfly_provider.?.generate_messages_with_attachments != null);
        const remote_batch = cfg.provider == .antfly and cfg.url.len > 0;
        return (local_serial or remote_batch) and
            (remote_batch or cfg.api_key == null) and
            cfg.project_id == null and
            cfg.location == null and
            cfg.credentials_path == null and
            cfg.tools_json == null and
            cfg.tool_choice_json == null and
            parsed.tool_output == .content;
    }

    fn generatorCapabilities(self: *Runtime, alloc: Allocator, cfg: generating_runtime.GeneratorConfig) !?inference_work.InferenceCapabilities {
        if (cfg.provider != .antfly) return null;
        if (cfg.url.len == 0) {
            const local = self.antfly_provider orelse return null;
            return try self.linkedModelCapabilities(alloc, local, cfg.model, .generate);
        }
        var secret = try common_secrets.SecretValue.initConfigOrEnv(alloc, cfg.api_key, "ANTFLY_INFERENCE_API_KEY");
        defer secret.deinit(alloc);
        const token = secret.resolveOwned(alloc, self.secret_store) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return null,
        };
        defer if (token) |value| alloc.free(value);
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [2][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (token) |value| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        return self.capabilityCache().getOrDiscoverWithContext(
            self.http,
            cfg.url,
            cfg.model,
            .generate_batch,
            header_storage[0..header_count],
            self.execution.waitContext(),
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => null,
        };
    }

    fn canExtractBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareRoute(requests)) return false;
        const capabilities = (try capabilitiesForRequests(self, alloc, requests)) orelse return false;
        if (capabilities.task != .extract or capabilities.result_cardinality != .one_per_item or
            capabilities.batch.mode == .none) return false;
        var cfg = extracting.parseConfigFromSlice(alloc, requests[0].config_json) catch return false;
        defer cfg.deinit(alloc);
        self.routeExtractorConfig(alloc, &cfg) catch return false;
        validateExtractorBatchPlan(
            alloc,
            capabilities,
            extractorAttachmentTransport(cfg),
            requests,
        ) catch return false;
        return true;
    }

    fn extractorCapabilities(
        self: *Runtime,
        alloc: Allocator,
        cfg: extracting.Config,
    ) !?inference_work.InferenceCapabilities {
        const endpoint = if (cfg.provider == .antfly)
            self.execution.resolveAntflyEndpoint(cfg.resolvedUrl(), self.linkedExtractorAvailable())
        else
            cfg.resolvedUrl();
        if (isLocalExtractionProvider(cfg.provider, endpoint)) {
            const local = self.antfly_provider orelse return null;
            if (local.extract == null and local.extract_with_context == null) return null;
            const capabilities = (try self.linkedModelCapabilities(alloc, local, cfg.model, .extract)) orelse return null;
            try capabilities.validate();
            if (capabilities.task != .extract) return error.InvalidInferenceCapabilities;
            return capabilities;
        }
        if (cfg.provider != .antfly) return null;
        const remote_endpoint = endpoint orelse return null;
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [2][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (cfg.bearer_token orelse cfg.api_key) |token| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        const headers = header_storage[0..header_count];
        return self.capabilityCache().getOrDiscoverWithContext(
            self.http,
            remote_endpoint,
            cfg.model,
            .extract,
            headers,
            self.execution.waitContext(),
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => null,
        };
    }

    fn bindExtractorCapabilityLease(self: *Runtime, alloc: Allocator, cfg: *extracting.Config) !?inference_work.InferenceCapabilities {
        if (cfg.provider != .antfly or cfg.resolvedUrl() == null) return null;
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [2][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (cfg.bearer_token orelse cfg.api_key) |token| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        const headers = header_storage[0..header_count];
        const lease = try self.capabilityCache().getOrDiscoverLeaseWithContext(
            self.http,
            cfg.resolvedUrl().?,
            cfg.model,
            .extract,
            headers,
            self.execution.waitContext(),
        );
        try replaceCapabilityLeaseFields(alloc, cfg, lease);
        cfg.framed_attachments = lease.capabilities != null and lease.capabilities.?.framed_attachments;
        return lease.capabilities;
    }

    fn invalidateExtractorCapabilityLease(self: *Runtime, alloc: Allocator, cfg: extracting.Config) !void {
        if (cfg.provider != .antfly or cfg.resolvedUrl() == null) return;
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [2][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (cfg.bearer_token orelse cfg.api_key) |token| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        const headers = header_storage[0..header_count];
        try self.capabilityCache().invalidate(cfg.resolvedUrl().?, cfg.model, .extract, headers);
    }

    fn produceOne(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        return switch (request.producer_type) {
            .copy => try alloc.dupe(u8, request.source_text),
            .document_extraction => error.UnsupportedAssetProducer,
            .generator => try self.generate(alloc, request),
            .reader => try self.read(alloc, request),
            .transcriber => try self.transcribe(alloc, request),
            .extractor => try self.extract(alloc, request),
        };
    }

    fn produceBatch(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0) return try alloc.alloc([]u8, 0);

        const first_type = requests[0].producer_type;
        for (requests) |request| {
            if (request.producer_type != first_type) return try self.produceBatchSequential(alloc, requests);
        }

        const batch_result = switch (first_type) {
            .copy => self.produceCopyBatch(alloc, requests),
            .reader => self.tryReadBatch(alloc, requests),
            .generator => self.tryGenerateBatch(alloc, requests),
            .extractor => self.tryExtractBatch(alloc, requests),
            .transcriber => self.tryTranscribeBatch(alloc, requests),
            .document_extraction => error.BatchIncompatible,
        };
        if (batch_result) |items| {
            return items;
        } else |err| switch (err) {
            error.BatchIncompatible => {},
            else => return err,
        }
        return try self.produceBatchSequential(alloc, requests);
    }

    fn produceBatchReported(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !asset_producer.ProducedBatch {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0) return .{
            .items = try alloc.alloc(asset_producer.ProducedItem, 0),
            .execution = inference_work.ExecutionReport.serial(0),
        };
        const first_type = requests[0].producer_type;
        for (requests) |request| if (request.producer_type != first_type) {
            const outputs = try self.produceBatchSequential(alloc, requests);
            return try asset_producer.producedBatchFromOutputs(
                alloc,
                requests,
                outputs,
                inference_work.ExecutionReport.fallback(requests.len, "mixed_producer_types"),
            );
        };
        if (first_type == .reader) return self.tryReadBatchReported(alloc, requests) catch |err| switch (err) {
            error.BatchIncompatible => blk: {
                const outputs = try self.produceBatchSequential(alloc, requests);
                break :blk try asset_producer.producedBatchFromOutputs(
                    alloc,
                    requests,
                    outputs,
                    inference_work.ExecutionReport.fallback(requests.len, "batch_incompatible"),
                );
            },
            else => return err,
        };
        if (first_type == .generator) return self.tryGenerateBatchReported(alloc, requests) catch |err| switch (err) {
            error.BatchIncompatible => blk: {
                const outputs = try self.produceBatchSequential(alloc, requests);
                break :blk try asset_producer.producedBatchFromOutputs(
                    alloc,
                    requests,
                    outputs,
                    inference_work.ExecutionReport.fallback(requests.len, "capabilities_unavailable"),
                );
            },
            else => return err,
        };
        if (first_type == .extractor) return self.tryExtractBatchReported(alloc, requests) catch |err| switch (err) {
            error.BatchIncompatible => blk: {
                const outputs = try self.produceBatchSequential(alloc, requests);
                break :blk try asset_producer.producedBatchFromOutputs(
                    alloc,
                    requests,
                    outputs,
                    inference_work.ExecutionReport.fallback(requests.len, "capabilities_unavailable"),
                );
            },
            else => return err,
        };

        const mode = try batchMode(ptr, alloc, requests);
        const items = try produceBatch(self, alloc, requests);
        return try asset_producer.producedBatchFromOutputs(
            alloc,
            requests,
            items,
            switch (mode) {
                .native => inference_work.ExecutionReport.native(requests.len),
                .serial_compatibility, .none => inference_work.ExecutionReport.serial(requests.len),
            },
        );
    }

    fn produceBorrowedRasterBatchReported(
        ptr: *anyopaque,
        alloc: Allocator,
        requests: []const asset_producer.Request,
        rasters: []const readers.RasterImage,
    ) !asset_producer.ProducedBatch {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0 or requests.len != rasters.len)
            return error.InvalidBorrowedRasterCardinality;
        if (!requestsShareRoute(requests)) return error.BatchIncompatible;
        for (requests) |request| {
            if (request.producer_type != .reader or request.media.len != 0)
                return error.BatchIncompatible;
            if (!request.inline_media_trusted) return error.UntrustedInlineMedia;
        }

        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();
        const execution_cfg = self.routedReaderConfig(cfg_parsed.value);
        if (!isLocalReaderProvider(execution_cfg.provider, execution_cfg.resolvedUrl()))
            return error.BorrowedRasterUnsupported;
        const capabilities = (try self.readerCapabilities(alloc, cfg_parsed.value)) orelse
            return error.BorrowedRasterUnsupported;
        if (!capabilities.borrowed_rasters or capabilities.result_cardinality != .one_per_item or
            !capabilities.supports(.{ .image = true })) return error.BorrowedRasterUnsupported;

        var owned_shared_prompt: ?[]u8 = null;
        defer if (owned_shared_prompt) |prompt| alloc.free(prompt);
        var shared_prompt: ?[]const u8 = cfg_parsed.value.prompt;
        for (requests, 0..) |request, i| {
            var source = try parseReaderSourceWithMetadataOnly(
                alloc,
                request.source_text,
                request.source_parts_json,
                true,
            );
            defer source.deinit(alloc);
            const effective_prompt = source.prompt orelse cfg_parsed.value.prompt;
            if (i == 0) {
                if (source.prompt) |prompt| {
                    owned_shared_prompt = try alloc.dupe(u8, prompt);
                    shared_prompt = owned_shared_prompt;
                }
            } else if (!optionalStringsEqual(shared_prompt, effective_prompt)) {
                return error.BatchIncompatible;
            }
        }

        var batch = try self.readRasterImagesWithConfigReported(alloc, execution_cfg, .{
            .images = rasters,
            .prompt = shared_prompt,
            .max_tokens = execution_cfg.max_tokens,
            .source_fingerprint = requests[0].source_fingerprint,
            .max_response_bytes = self.responseLimitForTask(.reader, requests.len),
        });
        defer batch.deinit(alloc);
        if (batch.items.len != requests.len) return error.InvalidReaderResponse;
        try batch.execution.validate(batch.items.len);

        const outputs = try alloc.alloc([]u8, requests.len);
        var filled: usize = 0;
        var outputs_owned = true;
        errdefer if (outputs_owned) {
            for (outputs[0..filled]) |output| alloc.free(output);
            alloc.free(outputs);
        };
        for (batch.items, requests, rasters, 0..) |result, request, raster, i| {
            if (!readerResultMatchesRasterIdentity(result, raster))
                return error.InvalidReaderResponseIdentity;
            outputs[i] = try encodeReaderResults(alloc, request.content_type, batch.items[i .. i + 1]);
            filled += 1;
        }
        const execution = inference_work.ExecutionReport{
            .requested_items = batch.execution.requested_items,
            .native_items = batch.execution.native_items,
            .serial_items = batch.execution.serial_items,
            .fallback_items = batch.execution.fallback_items,
            .native_batches = batch.execution.native_batches,
            .fallback_reason = batch.execution.fallback_reason,
        };
        // producedBatchFromOutputs owns the output array on both success and
        // error, so disarm our construction cleanup before transferring it.
        outputs_owned = false;
        return try asset_producer.producedBatchFromOutputs(alloc, requests, outputs, execution);
    }

    fn produceCopyBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        _ = self;
        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            out[i] = try alloc.dupe(u8, request.source_text);
        }
        return out;
    }

    fn produceBatchSequential(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            try self.execution.check(platform.time.monotonicNs());
            out[i] = try self.produceOne(alloc, request);
        }
        return out;
    }

    /// Execute independent remote items through a bounded ordered window. Task
    /// outputs use the process thread-safe allocator while workers are active,
    /// then transfer to the caller allocator after the join. This keeps caller
    /// allocator thread-safety out of the executor contract and bounds retained
    /// response memory even when a provider route fans out across nodes.
    fn produceRemoteCompatibilityBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        if (requests.len == 0) return try alloc.alloc([]u8, 0);
        const io = self.execution.io orelse return self.produceBatchSequential(alloc, requests);
        const per_item_response_bytes = @max(
            @as(usize, 1),
            try self.configuredResultLimit(requests[0].producer_type, 1),
        );
        const response_budget: usize = 64 << 20;
        const width = @max(
            @as(usize, 1),
            @min(
                @as(usize, 8),
                @min(requests.len, response_budget / @min(per_item_response_bytes, response_budget)),
            ),
        );

        const Task = struct {
            runtime: *Runtime,
            request: asset_producer.Request,
            output: ?[]u8 = null,
            failure: ?anyerror = null,

            fn run(task: *@This()) std.Io.Cancelable!void {
                task.runtime.execution.check(platform.time.monotonicNs()) catch |err| {
                    task.failure = err;
                    return;
                };
                task.output = task.runtime.produceOne(std.heap.smp_allocator, task.request) catch |err| {
                    task.failure = err;
                    return;
                };
                task.runtime.execution.check(platform.time.monotonicNs()) catch |err| {
                    std.heap.smp_allocator.free(task.output.?);
                    task.output = null;
                    task.failure = err;
                };
            }
        };

        const tasks = try alloc.alloc(Task, requests.len);
        defer alloc.free(tasks);
        for (tasks, requests) |*task, request| task.* = .{ .runtime = self, .request = request };
        errdefer for (tasks) |*task| if (task.output) |output| std.heap.smp_allocator.free(output);

        const out = try alloc.alloc([]u8, requests.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |output| alloc.free(output);
            alloc.free(out);
        }

        var start: usize = 0;
        while (start < tasks.len) : (start += width) {
            const end = @min(start + width, tasks.len);
            var group: std.Io.Group = .init;
            for (tasks[start..end]) |*task| group.async(io, Task.run, .{task});
            try group.await(io);
            for (tasks[start..end]) |task| if (task.failure) |err| return err;

            // Transfer and release this wave before admitting the next one.
            // Final caller-owned results necessarily scale with item count, but
            // the thread-safe compatibility copies stay bounded by `width`
            // instead of accumulating a second complete result set.
            for (tasks[start..end], out[start..end]) |*task, *output| {
                const temporary = task.output orelse return error.MissingAssetProducerOutput;
                output.* = try alloc.dupe(u8, temporary);
                initialized += 1;
                std.heap.smp_allocator.free(temporary);
                task.output = null;
            }
        }
        return out;
    }

    fn tryExtractBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        var batch = try self.tryExtractBatchReported(alloc, requests);
        defer batch.deinit(alloc);
        return try batch.intoOutputs(alloc);
    }

    fn tryExtractBatchReported(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !asset_producer.ProducedBatch {
        for (requests) |request| {
            if (request.producer_type != .extractor) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
        }

        const capabilities = (try capabilitiesForRequests(self, alloc, requests)) orelse return error.BatchIncompatible;
        var cfg = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
        defer cfg.deinit(alloc);
        try self.routeExtractorConfig(alloc, &cfg);
        const attachment_transport: inference_work.AttachmentTransport = if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) .borrowed_binary else if (capabilities.framed_attachments) .framed_binary else .base64_payload;
        try validateExtractorBatchCompatibility(alloc, capabilities, attachment_transport, requests);
        const outputs = try alloc.alloc([]u8, requests.len);
        var outputs_owned = true;
        errdefer if (outputs_owned) {
            for (outputs) |output| if (output.len > 0) alloc.free(output);
            alloc.free(outputs);
        };
        for (outputs) |*output| output.* = &.{};
        var windows: usize = 0;
        var start: usize = 0;
        while (start < requests.len) {
            const end = try extractorBatchEnd(alloc, capabilities, attachment_transport, requests, start);
            try validateExtractorInvocation(alloc, capabilities, attachment_transport, requests[start..end]);
            const chunk_outputs = try self.tryExtractBatchChunk(alloc, requests[start..end]);
            defer alloc.free(chunk_outputs);
            if (chunk_outputs.len != end - start) {
                for (chunk_outputs) |output| if (output.len > 0) alloc.free(output);
                return error.InvalidProducedBatchCardinality;
            }
            for (chunk_outputs, 0..) |output, i| outputs[start + i] = output;
            windows += 1;
            start = end;
        }
        const execution = switch (capabilities.batch.mode) {
            .native => inference_work.ExecutionReport{
                .requested_items = requests.len,
                .native_batches = windows,
                .native_items = requests.len,
            },
            .serial_compatibility => inference_work.ExecutionReport.serial(requests.len),
            .none => return error.BatchIncompatible,
        };
        outputs_owned = false;
        return try asset_producer.producedBatchFromOutputs(alloc, requests, outputs, execution);
    }

    fn tryExtractBatchChunk(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        var cfg = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
        defer cfg.deinit(alloc);
        try self.routeExtractorConfig(alloc, &cfg);
        _ = try self.bindExtractorCapabilityLease(alloc, &cfg);

        const inputs = try alloc.alloc(extracting.Input, requests.len);
        var inputs_filled: usize = 0;
        defer {
            for (inputs[0..inputs_filled]) |input| alloc.free(input.content_json);
            alloc.free(inputs);
        }
        const input_ids = try alloc.alloc([]u8, requests.len);
        var input_ids_filled: usize = 0;
        defer {
            for (input_ids[0..input_ids_filled]) |id| alloc.free(id);
            alloc.free(input_ids);
        }
        const output_ids = try alloc.alloc([]const u8, requests.len);
        defer alloc.free(output_ids);

        var attachment_count: usize = 0;
        for (requests) |request| attachment_count = try std.math.add(usize, attachment_count, request.media.len);
        const attachments = try alloc.alloc(extracting.Attachment, attachment_count);
        defer alloc.free(attachments);
        var attachment_index: usize = 0;

        for (requests, 0..) |request, i| {
            // The wire identifier is invocation-local demultiplexing state,
            // never the caller's durable identity. Distinct documents may
            // legitimately reuse page-local item IDs such as page:000001.
            input_ids[i] = try std.fmt.allocPrint(alloc, "antfly-batch-item-{d}", .{i});
            input_ids_filled += 1;
            output_ids[i] = request.item_id;
            inputs[i] = .{
                .id = input_ids[i],
                .content_json = try extractionContentJsonAlloc(alloc, request.source_text, request.source_parts_json),
            };
            inputs_filled += 1;
            for (request.media) |media| {
                try validateEncodedMedia(media);
                attachments[attachment_index] = .{
                    .input_index = i,
                    .bytes = media.bytes,
                    .mime_type = media.mime_type,
                };
                attachment_index += 1;
            }
        }

        const extract_request = extracting.Request{
            .inputs = inputs,
            .schema_json = cfg.schema_json,
            .options_json = cfg.options_json,
            .attachments = attachments,
            .max_response_bytes = self.responseLimitForTask(.extractor, requests.len),
        };
        var response = if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.BatchIncompatible;
            break :blk if (local.extract_with_context) |extract_fn|
                try managed_embedder.AntflyProviderBoundary.call(
                    "extract_with_context",
                    local.boundary_dispatch,
                    extract_fn,
                    .{ local.ptr, alloc, cfg.model, extract_request, self.requestContext() },
                )
            else if (local.extract) |extract_fn|
                try managed_embedder.AntflyProviderBoundary.call(
                    "extract",
                    local.boundary_dispatch,
                    extract_fn,
                    .{ local.ptr, alloc, cfg.model, extract_request },
                )
            else
                return error.BatchIncompatible;
        } else extracting.extractWithConfigAndOptions(alloc, self.http, cfg, extract_request, .{
            .source_table = self.execution.routing.source_table,
            .timeout_ms = try self.execution.remainingTimeoutMs(platform.time.monotonicNs(), max_asset_provider_timeout_ms),
            .cancellation = httpx.CancellationToken.fromCallback(
                self.execution.cancellation.ptr,
                self.execution.cancellation.is_cancelled_fn,
            ),
        }) catch |err| {
            if (err == error.InferenceCapabilitiesStale)
                try self.invalidateExtractorCapabilityLease(alloc, cfg);
            return err;
        };
        defer response.deinit();

        return try extractionResultsJsonAlloc(alloc, response.json, cfg.model, input_ids, output_ids);
    }

    fn tryGenerateBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        var batch = try self.tryGenerateBatchReported(alloc, requests);
        defer batch.deinit(alloc);
        return try batch.intoOutputs(alloc);
    }

    fn tryGenerateBatchReported(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !asset_producer.ProducedBatch {
        for (requests) |request| {
            if (request.producer_type != .generator) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
            if (request.media.len > 0 and !request.inline_media_trusted) return error.UntrustedInlineMedia;
            for (request.media) |media| try validateEncodedMedia(media);
        }

        var parsed_cfg = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
        defer parsed_cfg.deinit(alloc);
        try self.routeGeneratorConfig(alloc, &parsed_cfg.generator);
        const cfg = parsed_cfg.generator;
        try generating_runtime.validateGenerationOutputTokenBudget(cfg, requests.len);
        const generation_policy = try provider_limits.Policy.fromConfig(cfg.rate_limit);
        if (generation_policy.tokens_per_minute != 0) for (requests) |request| {
            if (request.media.len > 0) return error.UnsupportedMediaTokenBudget;
            if (request.source_parts_json) |raw_parts| {
                const content_parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts, &.{}, false);
                defer freeGeneratorContentParts(alloc, content_parts);
                for (content_parts) |part| if (part != .text) return error.UnsupportedMediaTokenBudget;
            }
        };
        if (cfg.provider != .antfly) return error.BatchIncompatible;
        if (cfg.project_id != null or cfg.location != null or cfg.credentials_path != null) return error.BatchIncompatible;
        if (cfg.tools_json != null or cfg.tool_choice_json != null or parsed_cfg.tool_output != .content) return error.BatchIncompatible;
        if (cfg.url.len == 0) {
            const local = self.antfly_provider orelse return error.BatchIncompatible;
            if (local.generate_messages_with_attachments == null and
                local.generate_messages_with_attachments_with_context == null) return error.BatchIncompatible;
            const outputs = try self.produceBatchSequential(alloc, requests);
            return try asset_producer.producedBatchFromOutputs(
                alloc,
                requests,
                outputs,
                inference_work.ExecutionReport.serial(requests.len),
            );
        }

        var capabilities = (try capabilitiesForRequests(self, alloc, requests)) orelse
            return error.BatchIncompatible;
        if (capabilities.task != .generate or capabilities.result_cardinality != .one_per_item)
            return error.InvalidInferenceCapabilities;
        const batch_url = try antflyGenerateBatchUrlAlloc(alloc, cfg.url);
        defer alloc.free(batch_url);
        var secret = try common_secrets.SecretValue.initConfigOrEnv(
            alloc,
            cfg.api_key,
            "ANTFLY_INFERENCE_API_KEY",
        );
        defer secret.deinit(alloc);
        const token = try secret.resolveOwned(alloc, self.secret_store);
        defer if (token) |value| alloc.free(value);
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [4][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (token) |value| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        const auth_headers = header_storage[0..header_count];
        var capability_lease = try self.capabilityCache().getOrDiscoverLeaseWithContext(
            self.http,
            cfg.url,
            cfg.model,
            .generate_batch,
            auth_headers,
            self.execution.waitContext(),
        );
        capabilities = capability_lease.capabilities orelse return error.InvalidInferenceCapabilities;
        if (capability_lease.routing_token) |*value| {
            header_storage[header_count] = .{ remote_capabilities.capability_token_header, value.slice() };
            header_count += 1;
        }
        if (capability_lease.descriptor_revision) |*value| {
            header_storage[header_count] = .{ remote_capabilities.capability_revision_header, value.slice() };
            header_count += 1;
        }
        const headers = header_storage[0..header_count];
        var quota = try generating_runtime.acquireGenerationQuota(alloc, cfg, .{
            .limits = self.limits,
        });
        defer quota.release();

        const items = try alloc.alloc(asset_producer.ProducedItem, requests.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |item| switch (item.result) {
                .value => |value| if (value.len > 0) alloc.free(value),
                .item_error => {},
            };
            alloc.free(items);
        }
        var native_batches: usize = 0;
        var native_items: usize = 0;
        var serial_items: usize = 0;
        var rejected_items: usize = 0;
        var fallback_items: usize = 0;
        var start: usize = 0;
        while (start < requests.len) {
            try self.execution.check(platform.time.monotonicNs());
            const attachment_transport = remoteAttachmentTransport(capabilities, .base64_payload);
            const end = try generatorBatchEnd(alloc, capabilities, attachment_transport, requests, start);
            const chunk = requests[start..end];
            try validateGeneratorInvocation(alloc, capabilities, attachment_transport, chunk);
            var body = try antflyGenerateBatchRequestAlloc(alloc, cfg, chunk, attachment_transport);
            defer body.deinit(alloc);
            var request_options = httpx.RequestOptions{
                .attempt_observer = quota.limiter().observer(
                    try generating_runtime.generationOutputTokenBudget(cfg, chunk.len),
                ),
                .headers = headers,
                .timeout_ms = try self.execution.remainingTimeoutMs(
                    platform.time.monotonicNs(),
                    max_asset_provider_timeout_ms,
                ),
                .max_response_size = self.responseLimitForTask(.generator, chunk.len),
                .cancellation = httpx.CancellationToken.fromCallback(
                    self.execution.cancellation.ptr,
                    self.execution.cancellation.is_cancelled_fn,
                ),
            };
            var framed_headers: [5][2][]const u8 = undefined;
            if (attachment_transport == .framed_binary) {
                if (headers.len >= framed_headers.len) return error.InvalidGeneratorConfig;
                @memcpy(framed_headers[0..headers.len], headers);
                framed_headers[headers.len] = .{ "Content-Type", httpx.attachment_envelope.content_type };
                request_options.headers = framed_headers[0 .. headers.len + 1];
                request_options.borrowed_body_segments = body.envelope.?.segments;
            } else {
                request_options.json = body.metadata_or_json;
            }
            var resp = try self.http.post(batch_url, request_options);
            defer resp.deinit();
            if (!resp.ok()) {
                const stale_value = resp.headers.get(remote_capabilities.capability_stale_header);
                if (resp.status.code == 409 and stale_value != null and
                    std.ascii.eqlIgnoreCase(std.mem.trim(u8, stale_value.?, " \t"), "true"))
                {
                    try self.capabilityCache().invalidate(cfg.url, cfg.model, .generate_batch, auth_headers);
                    return error.InferenceCapabilitiesStale;
                }
                return mapAntflyGenerateBatchStatus(resp.status.code);
            }
            const payload = resp.body orelse return error.EmptyGenerateBatchResponse;
            const chunk_response = try parseAntflyGenerateBatchResponseAlloc(alloc, payload, chunk);
            defer alloc.free(chunk_response.items);
            for (chunk_response.items) |item| {
                items[initialized] = item;
                initialized += 1;
            }
            native_batches += chunk_response.execution.native_batches;
            native_items += chunk_response.execution.native_items;
            serial_items += chunk_response.execution.serial_items;
            rejected_items += chunk_response.execution.rejected_items;
            fallback_items += chunk_response.execution.fallback_items;
            start = end;
        }
        return .{
            .items = items,
            .execution = .{
                .requested_items = requests.len,
                .native_batches = native_batches,
                .native_items = native_items,
                .serial_items = serial_items,
                .rejected_items = rejected_items,
                .fallback_items = fallback_items,
                .fallback_reason = if (fallback_items > 0) "remote_generator_fallback" else null,
            },
        };
    }

    fn mapAntflyGenerateBatchStatus(status: u16) anyerror {
        return switch (status) {
            408, 409, 425, 429 => error.GenerateBatchTransientFailure,
            else => if (status >= 500 and status <= 599) error.GenerateBatchTransientFailure else error.GenerateBatchRequestFailed,
        };
    }

    fn bindGeneratorCapabilityLease(
        self: *Runtime,
        alloc: Allocator,
        cfg: *generating_runtime.GeneratorConfig,
    ) !?inference_work.InferenceCapabilities {
        if (cfg.provider != .antfly or cfg.url.len == 0) return null;
        var secret = try common_secrets.SecretValue.initConfigOrEnv(
            alloc,
            cfg.api_key,
            "ANTFLY_INFERENCE_API_KEY",
        );
        defer secret.deinit(alloc);
        const secret_value = try secret.resolveOwned(alloc, self.secret_store);
        defer if (secret_value) |value| alloc.free(value);
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [2][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (secret_value) |value| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        const headers = header_storage[0..header_count];
        const lease = try self.capabilityCache().getOrDiscoverLeaseWithContext(
            self.http,
            cfg.url,
            cfg.model,
            .generate,
            headers,
            self.execution.waitContext(),
        );
        try replaceCapabilityLeaseFields(alloc, cfg, lease);
        return lease.capabilities;
    }

    fn invalidateGeneratorCapabilityLease(
        self: *Runtime,
        alloc: Allocator,
        cfg: generating_runtime.GeneratorConfig,
    ) !void {
        if (cfg.provider != .antfly or cfg.url.len == 0) return;
        var secret = try common_secrets.SecretValue.initConfigOrEnv(
            alloc,
            cfg.api_key,
            "ANTFLY_INFERENCE_API_KEY",
        );
        defer secret.deinit(alloc);
        const secret_value = try secret.resolveOwned(alloc, self.secret_store);
        defer if (secret_value) |value| alloc.free(value);
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [2][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (secret_value) |value| {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
            header_storage[header_count] = .{ "Authorization", auth_value.? };
            header_count += 1;
        }
        header_count = try self.execution.routing.appendHeaders(&header_storage, header_count);
        const headers = header_storage[0..header_count];
        try self.capabilityCache().invalidate(cfg.url, cfg.model, .generate, headers);
    }

    fn tryTranscribeBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        for (requests) |request| {
            if (request.producer_type != .transcriber) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
        }

        var cfg_parsed = try std.json.parseFromSlice(transcribing.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();
        // Transport features are route-leased runtime facts, never trusted
        // from durable user configuration.
        cfg_parsed.value.framed_attachments = false;
        cfg_parsed.value = self.routedTranscriberConfig(cfg_parsed.value);
        if (!isLocalTranscriberProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl()))
            return self.produceRemoteCompatibilityBatch(alloc, requests);
        const model = requiredAntflyTranscriberModel(cfg_parsed.value) catch return error.BatchIncompatible;
        const local = self.antfly_provider orelse return error.BatchIncompatible;

        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            const transcribe_request = transcribing.Request{
                .url = request.source_text,
                .language = cfg_parsed.value.language_code,
            };
            var result = if (local.transcribe_audio_with_context) |transcribe_audio|
                try managed_embedder.AntflyProviderBoundary.call(
                    "transcribe_audio_with_context",
                    local.boundary_dispatch,
                    transcribe_audio,
                    .{ local.ptr, alloc, model, transcribe_request, self.requestContext() },
                )
            else if (local.transcribe_audio) |transcribe_audio|
                try managed_embedder.AntflyProviderBoundary.call(
                    "transcribe_audio",
                    local.boundary_dispatch,
                    transcribe_audio,
                    .{ local.ptr, alloc, model, transcribe_request },
                )
            else
                return error.BatchIncompatible;
            defer transcribing.deinitResponse(alloc, &result);

            out[i] = if (isJsonContentType(request.content_type))
                try std.json.Stringify.valueAlloc(alloc, result, .{})
            else
                try alloc.dupe(u8, result.text orelse "");
        }
        return out;
    }

    fn tryReadBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        var batch = try self.tryReadBatchReported(alloc, requests);
        defer batch.deinit(alloc);
        return try batch.intoOutputs(alloc);
    }

    fn tryReadBatchReported(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !asset_producer.ProducedBatch {
        const uses_encoded_media = requests[0].media.len > 0;
        for (requests) |request| {
            if (request.producer_type != .reader) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
            // Never collapse external and internally generated media into one
            // request: trust is intentionally carried at the request boundary.
            if (request.inline_media_trusted != requests[0].inline_media_trusted) return error.BatchIncompatible;
            if ((request.media.len > 0) != uses_encoded_media) return error.BatchIncompatible;
        }
        if (uses_encoded_media and !requests[0].inline_media_trusted) return error.UntrustedInlineMedia;

        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();
        // The Antfly reader contract returns one independently addressable
        // result per input image. OpenAI and Vertex accept multiple images in
        // one prompt, but produce one response for the prompt as a whole, so
        // flattening requests across those providers loses the request/result
        // boundary. Let the generic batch path execute them sequentially.
        const capabilities = (try self.readerCapabilities(alloc, cfg_parsed.value)) orelse
            return error.BatchIncompatible;
        if (capabilities.batch.mode == .none or capabilities.result_cardinality != .one_per_item or
            !capabilities.supports(.{ .image = true })) return error.BatchIncompatible;
        const sources = try alloc.alloc(ReaderSource, requests.len);
        var sources_filled: usize = 0;
        defer {
            for (sources[0..sources_filled]) |*source| source.deinit(alloc);
            alloc.free(sources);
        }
        const image_counts = try alloc.alloc(usize, requests.len);
        defer alloc.free(image_counts);
        var flat_images = std.ArrayListUnmanaged([]const u8).empty;
        defer flat_images.deinit(alloc);
        var flat_encoded = std.ArrayListUnmanaged(readers.EncodedImage).empty;
        defer flat_encoded.deinit(alloc);
        var flat_source_fingerprints = std.ArrayListUnmanaged(?[]const u8).empty;
        defer flat_source_fingerprints.deinit(alloc);

        var shared_prompt: ?[]const u8 = cfg_parsed.value.prompt;
        for (requests, 0..) |request, i| {
            sources[i] = try parseReaderSourceWithMetadataOnly(
                alloc,
                request.source_text,
                request.source_parts_json,
                request.media.len > 0,
            );
            sources_filled += 1;
            const effective_prompt = sources[i].prompt orelse cfg_parsed.value.prompt;
            if (!optionalStringsEqual(shared_prompt, effective_prompt)) {
                if (i == 0) shared_prompt = effective_prompt else return error.BatchIncompatible;
            }
            if (uses_encoded_media) {
                image_counts[i] = request.media.len;
                for (request.media) |media| {
                    try flat_encoded.append(alloc, .{
                        .bytes = media.bytes,
                        .mime_type = media.mime_type,
                        .item_id = request.item_id,
                        .source_fingerprint = request.source_fingerprint,
                        .page_number = request.page_number,
                    });
                    try flat_source_fingerprints.append(alloc, request.source_fingerprint);
                }
            } else {
                image_counts[i] = sources[i].images.len;
                try flat_images.appendSlice(alloc, sources[i].images);
                for (sources[i].images) |_| try flat_source_fingerprints.append(alloc, request.source_fingerprint);
            }
            // ProducedBatch is one typed result and one execution item per
            // outer request. A multi-image prompt is a single request whose
            // nested reader result has different cardinality, so it must stay
            // on the compatibility path instead of being flattened into an
            // ambiguous cross-request execution report. PDF preparation emits
            // one page image per request and retains native batching here.
            if (image_counts[i] != 1) return error.BatchIncompatible;
        }
        const total_images = if (uses_encoded_media) flat_encoded.items.len else flat_images.items.len;
        if (total_images == 0) return error.BatchIncompatible;
        std.debug.assert(flat_source_fingerprints.items.len == total_images);

        const results = try alloc.alloc(readers.Result, total_images);
        var results_filled: usize = 0;
        var reader_execution = inference_work.ExecutionReport{};
        var results_errdefer_active = true;
        errdefer if (results_errdefer_active) {
            for (results[0..results_filled]) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        };
        var image_offset: usize = 0;
        const local_reader = isLocalReaderProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl());
        const reader_chunk_max_images = if (local_reader)
            @min(localReaderBatchMaxImages(), capabilities.batch.max_items)
        else
            capabilities.batch.max_items;
        while (image_offset < total_images) {
            // Encoded images carry identity per item, so compatible pages from
            // different documents may share a model batch without losing
            // attribution. URL-only inputs retain the conservative legacy
            // source boundary until their transport also carries per-item
            // identity.
            const image_end = if (uses_encoded_media)
                try encodedReaderBatchEnd(
                    flat_encoded.items,
                    image_offset,
                    capabilities.batch,
                    if (local_reader) .borrowed_binary else .data_uri,
                )
            else if (local_reader)
                readerBatchEnd(flat_source_fingerprints.items, image_offset, reader_chunk_max_images)
            else
                @min(image_offset +| reader_chunk_max_images, total_images);
            var chunk_fingerprint_buffer: [32]u8 = undefined;
            const chunk_source_fingerprint = readerBatchSourceFingerprint(
                flat_source_fingerprints.items[image_offset..image_end],
                &chunk_fingerprint_buffer,
            );
            const chunk_batch = if (uses_encoded_media)
                try self.readEncodedImagesWithConfigReported(alloc, cfg_parsed.value, .{
                    .images = flat_encoded.items[image_offset..image_end],
                    .prompt = shared_prompt,
                    .max_tokens = cfg_parsed.value.max_tokens,
                    .source_fingerprint = chunk_source_fingerprint,
                    .max_response_bytes = self.responseLimitForTask(.reader, image_end - image_offset),
                })
            else blk: {
                break :blk try self.readImagesWithConfigReported(alloc, cfg_parsed.value, .{
                    .images = flat_images.items[image_offset..image_end],
                    .prompt = shared_prompt,
                    .max_tokens = cfg_parsed.value.max_tokens,
                    .inline_content_trust = if (requests[0].inline_media_trusted) .trusted_internal else .untrusted,
                    .source_fingerprint = chunk_source_fingerprint,
                    .max_response_bytes = self.responseLimitForTask(.reader, image_end - image_offset),
                });
            };
            const chunk_results = chunk_batch.items;
            if (chunk_results.len != image_end - image_offset) {
                for (chunk_results) |*result| readers.deinitResult(alloc, result);
                alloc.free(chunk_results);
                return error.InvalidReaderResponse;
            }
            chunk_batch.execution.validate(chunk_results.len) catch {
                for (chunk_results) |*result| readers.deinitResult(alloc, result);
                alloc.free(chunk_results);
                return error.InvalidReadExecutionReport;
            };
            mergeReaderExecution(&reader_execution, chunk_batch.execution) catch |err| {
                for (chunk_results) |*result| readers.deinitResult(alloc, result);
                alloc.free(chunk_results);
                return err;
            };
            for (chunk_results, 0..) |result, j| {
                if (uses_encoded_media and !readerResultMatchesImageIdentity(result, flat_encoded.items[image_offset + j])) {
                    for (chunk_results) |*item| readers.deinitResult(alloc, item);
                    alloc.free(chunk_results);
                    return error.InvalidReaderResponseIdentity;
                }
                results[image_offset + j] = result;
            }
            results_filled += chunk_results.len;
            alloc.free(chunk_results);
            image_offset = image_end;
        }
        results_errdefer_active = false;
        defer {
            for (results) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        }

        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        var offset: usize = 0;
        for (requests, image_counts, 0..) |request, count, i| {
            out[i] = try encodeReaderResults(alloc, request.content_type, results[offset .. offset + count]);
            offset += count;
        }
        try reader_execution.validate();
        std.debug.assert(reader_execution.requested_items == requests.len);
        return try asset_producer.producedBatchFromOutputs(alloc, requests, out, reader_execution);
    }

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request) anyerror![]u8 {
        var parsed_cfg = try parseGeneratorProducerConfig(alloc, request.config_json);
        defer parsed_cfg.deinit(alloc);
        try self.routeGeneratorConfig(alloc, &parsed_cfg.generator);
        var cfg = parsed_cfg.generator;
        if (request.media.len > 0 and !request.inline_media_trusted) return error.UntrustedInlineMedia;
        for (request.media) |media| try validateEncodedMedia(media);

        // A singleton borrowed-media request must use the same transport as
        // its memory plan, not expand back into data URIs on a serial path.
        if (request.media.len > 0 and cfg.provider == .antfly and cfg.url.len > 0 and
            self.canGenerateBatchWithConfig(parsed_cfg, &.{request}))
        {
            if (try self.generatorCapabilities(alloc, cfg) != null) {
                const outputs = try self.tryGenerateBatch(alloc, &.{request});
                defer alloc.free(outputs);
                return outputs[0];
            }
        }

        const local_attachments = cfg.provider == .antfly and cfg.url.len == 0 and
            self.antfly_provider != null and
            (self.antfly_provider.?.generate_messages_with_attachments_with_context != null or
                self.antfly_provider.?.generate_messages_with_attachments != null) and
            request.media.len > 0;
        var parts: ?[]generating_runtime.ContentPart = null;
        defer if (parts) |items| freeGeneratorContentParts(alloc, items);
        const content: generating_runtime.ChatMessageContent = if (request.source_parts_json != null or request.media.len > 0) blk: {
            const raw_parts = request.source_parts_json orelse "[]";
            parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts, request.media, !local_attachments);
            break :blk .{ .parts = parts.? };
        } else .{ .text = request.source_text };
        const messages = [_]generating_runtime.ChatMessage{.{ .role = .user, .content = content }};
        try generating_runtime.validateGenerationMessagesTokenBudget(cfg, &messages);
        if (local_attachments) {
            if ((try provider_limits.Policy.fromConfig(cfg.rate_limit)).enabled())
                return error.UnsupportedLocalRateLimit;
            const local = self.antfly_provider.?;
            const capabilities = (try self.linkedModelCapabilities(alloc, local, cfg.model, .generate)) orelse
                return error.InvalidInferenceCapabilities;
            var encoded_bytes: usize = 0;
            var modalities = inference_work.Modalities{ .text = true };
            for (request.media) |media| {
                try capabilities.validateMimeType(media.mime_type);
                encoded_bytes = std.math.add(usize, encoded_bytes, media.bytes.len) catch
                    return error.InferenceEncodedBytesExceeded;
                mergeInferenceModalities(&modalities, try modalityForGeneratorMime(media.mime_type));
            }
            try capabilities.validateInvocation(.generate, .{
                .item_count = 1,
                .modalities = modalities,
                .encoded_media_bytes = encoded_bytes,
                .max_media_parts_per_item = request.media.len,
            });
            const attachments = try alloc.alloc(inference_work.Attachment, request.media.len);
            defer alloc.free(attachments);
            for (request.media, 0..) |media, i| attachments[i] = .{
                .bytes = media.bytes,
                .content_type = media.mime_type,
                .identity = .{
                    .item_id = request.item_id,
                    .source_fingerprint = request.source_fingerprint,
                    .page_number = request.page_number,
                },
            };
            const result = if (local.generate_messages_with_attachments_with_context) |generate_fn|
                try managed_embedder.AntflyProviderBoundary.call(
                    "generate_messages_with_attachments_with_context",
                    local.boundary_dispatch,
                    generate_fn,
                    .{ local.ptr, alloc, cfg.model, &messages, attachments, self.requestContext() },
                )
            else
                try managed_embedder.AntflyProviderBoundary.call(
                    "generate_messages_with_attachments",
                    local.boundary_dispatch,
                    local.generate_messages_with_attachments.?,
                    .{ local.ptr, alloc, cfg.model, &messages, attachments },
                );
            return result;
        }
        if (cfg.provider == .antfly and cfg.url.len > 0) {
            const capabilities = (try self.bindGeneratorCapabilityLease(alloc, &cfg)) orelse
                return error.InvalidInferenceCapabilities;
            try validateGeneratorInvocation(alloc, capabilities, .base64_payload, &.{request});
            parsed_cfg.generator.capability_token = cfg.capability_token;
            parsed_cfg.generator.capability_revision = cfg.capability_revision;
        }
        const link = generating_runtime.ChainLink{ .generator = cfg };
        var result = generating_runtime.executeChainWithOptions(alloc, self.http, &.{link}, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
            .max_response_bytes = self.responseLimitForTask(.generator, 1),
            .source_table = self.execution.routing.source_table,
            .execution = self.execution,
            .request_context = self.requestContext(),
            .limits = self.limits,
        }, &messages) catch |err| {
            if (err == error.InferenceCapabilitiesStale)
                try self.invalidateGeneratorCapabilityLease(alloc, cfg);
            return err;
        };
        defer result.deinit();
        if (parsed_cfg.tool_output == .arguments) {
            return try toolCallArgumentsOutputAlloc(alloc, result.tool_calls, parsed_cfg.tool_name);
        }
        return try alloc.dupe(u8, result.content);
    }

    fn read(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        if (request.media.len > 0 and !request.inline_media_trusted) return error.UntrustedInlineMedia;
        var source = try parseReaderSourceWithMetadataOnly(
            alloc,
            request.source_text,
            request.source_parts_json,
            request.media.len > 0,
        );
        defer source.deinit(alloc);

        const results = if (request.media.len > 0) blk: {
            const encoded = try alloc.alloc(readers.EncodedImage, request.media.len);
            defer alloc.free(encoded);
            for (request.media, 0..) |media, i| encoded[i] = .{
                .bytes = media.bytes,
                .mime_type = media.mime_type,
                .item_id = request.item_id,
                .source_fingerprint = request.source_fingerprint,
                .page_number = request.page_number,
            };
            break :blk try self.readEncodedImagesWithConfig(alloc, cfg_parsed.value, .{
                .images = encoded,
                .prompt = source.prompt orelse cfg_parsed.value.prompt,
                .max_tokens = cfg_parsed.value.max_tokens,
                .source_fingerprint = request.source_fingerprint,
                .max_response_bytes = self.responseLimitForTask(.reader, @max(request.media.len, 1)),
            });
        } else try self.readImagesWithConfig(alloc, cfg_parsed.value, .{
            .images = source.images,
            .prompt = source.prompt orelse cfg_parsed.value.prompt,
            .max_tokens = cfg_parsed.value.max_tokens,
            .inline_content_trust = if (request.inline_media_trusted) .trusted_internal else .untrusted,
            .source_fingerprint = request.source_fingerprint,
            .max_response_bytes = self.responseLimitForTask(.reader, @max(source.images.len, 1)),
        });
        defer {
            for (results) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        }
        return try encodeReaderResults(alloc, request.content_type, results);
    }

    fn readImagesWithConfig(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.Request) ![]readers.Result {
        return (try self.readImagesWithConfigReported(alloc, cfg, request)).items;
    }

    fn readImagesWithConfigReported(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.Request) !readers.BatchResult {
        var execution_cfg = self.routedReaderConfig(cfg);
        try execution_cfg.validate();
        const local_reader = isLocalReaderProvider(execution_cfg.provider, execution_cfg.resolvedUrl());
        var capability_lease_fields = CapabilityLeaseFields{};
        var capability_auth_value: ?[]u8 = null;
        defer if (capability_auth_value) |value| alloc.free(value);
        var capability_auth_storage: [2][2][]const u8 = undefined;
        var capability_auth_count: usize = 0;
        if (!local_reader and execution_cfg.provider == .antfly and
            (cfg.bearer_token orelse cfg.api_key) != null)
        {
            capability_auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{cfg.bearer_token orelse cfg.api_key.?});
            capability_auth_storage[capability_auth_count] = .{ "Authorization", capability_auth_value.? };
            capability_auth_count += 1;
        }
        capability_auth_count = try self.execution.routing.appendHeaders(&capability_auth_storage, capability_auth_count);
        const capability_auth_headers = capability_auth_storage[0..capability_auth_count];
        const discovered_capabilities: ?inference_work.InferenceCapabilities = if (!local_reader and execution_cfg.provider == .antfly) blk: {
            const endpoint = execution_cfg.resolvedUrl() orelse return error.InvalidReaderConfig;
            const lease = try self.capabilityCache().getOrDiscoverLeaseWithContext(
                self.http,
                endpoint,
                execution_cfg.model orelse "",
                .read,
                capability_auth_headers,
                self.execution.waitContext(),
            );
            capability_lease_fields = CapabilityLeaseFields.init(lease);
            capability_lease_fields.apply(&execution_cfg);
            break :blk lease.capabilities;
        } else try self.readerCapabilities(alloc, execution_cfg);
        if (discovered_capabilities) |capabilities| {
            const inline_shape = try readerUriInvocationShape(alloc, capabilities, request);
            try capabilities.validateInvocation(.read, .{
                .item_count = request.images.len,
                .modalities = .{ .image = true },
                .text_bytes = if (request.prompt) |prompt| prompt.len else 0,
                .max_text_bytes_per_item = if (request.prompt) |prompt| prompt.len else 0,
                .requested_output_tokens_per_item = if (request.max_tokens) |tokens|
                    if (tokens > 0) std.math.cast(usize, tokens) orelse std.math.maxInt(usize) else 0
                else
                    0,
                .encoded_media_bytes = inline_shape.encoded_media_bytes,
                .decoded_pixels = inline_shape.decoded_pixels,
                .max_media_parts_per_item = if (request.images.len > 0) 1 else 0,
            });
        } else if (local_reader) {
            // Embedded model execution is under Antfly's control and must
            // provide the model contract. Third-party compatibility readers
            // retain their provider-owned validation until they expose one.
            return error.InvalidInferenceCapabilities;
        }
        if (local_reader) {
            const local = self.antfly_provider orelse return error.UnsupportedReaderProvider;
            const items = if (local.read_images_with_context) |read_images|
                try managed_embedder.AntflyProviderBoundary.call(
                    "read_images_with_context",
                    local.boundary_dispatch,
                    read_images,
                    .{ local.ptr, alloc, execution_cfg.model orelse "", request, self.requestContext() },
                )
            else if (local.read_images) |read_images|
                try managed_embedder.AntflyProviderBoundary.call(
                    "read_images",
                    local.boundary_dispatch,
                    read_images,
                    .{ local.ptr, alloc, execution_cfg.model orelse "", request },
                )
            else
                return error.UnsupportedReaderProvider;
            return .{ .items = items, .execution = .{ .requested_items = items.len, .serial_items = items.len } };
        }

        return readers.readWithConfigReported(alloc, self.http, execution_cfg, request, .{
            .source_table = self.execution.routing.source_table,
            .timeout_ms = try self.execution.remainingTimeoutMs(platform.time.monotonicNs(), max_asset_provider_timeout_ms),
            .cancellation = httpx.CancellationToken.fromCallback(
                self.execution.cancellation.ptr,
                self.execution.cancellation.is_cancelled_fn,
            ),
        }) catch |err| {
            if (err == error.InferenceCapabilitiesStale and execution_cfg.provider == .antfly) {
                const endpoint = execution_cfg.resolvedUrl() orelse return err;
                try self.capabilityCache().invalidate(endpoint, execution_cfg.model orelse "", .read, capability_auth_headers);
            }
            return err;
        };
    }

    const ReaderUriInvocationShape = struct {
        encoded_media_bytes: usize = 0,
        decoded_pixels: u64 = 0,
    };

    /// Inspect inline payloads before selecting a local callback or remote
    /// adapter. Network URLs remain provider-owned until download, where the
    /// inference server applies its own byte/MIME limits.
    fn readerUriInvocationShape(
        alloc: Allocator,
        capabilities: inference_work.InferenceCapabilities,
        request: readers.Request,
    ) !ReaderUriInvocationShape {
        var shape = ReaderUriInvocationShape{};
        for (request.images) |url| {
            const parsed = (try inference_work.parseInlineDataUri(url)) orelse continue;
            try capabilities.validateMimeType(parsed.mime_type);
            if (parsed.decoded_size == 0) return error.InvalidDataURI;
            shape.encoded_media_bytes = std.math.add(usize, shape.encoded_media_bytes, url.len) catch
                return error.InferenceEncodedBytesExceeded;
            if (capabilities.batch.max_encoded_media_bytes) |limit| {
                if (shape.encoded_media_bytes > limit) return error.InferenceEncodedBytesExceeded;
            }
            const pixels = try inlineImagePixelsAlloc(alloc, parsed.mime_type, url);
            shape.decoded_pixels = std.math.add(u64, shape.decoded_pixels, pixels) catch
                return error.InferenceDecodedPixelsExceeded;
        }
        return shape;
    }

    fn readEncodedImagesWithConfig(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.EncodedRequest) ![]readers.Result {
        return (try self.readEncodedImagesWithConfigReported(alloc, cfg, request)).items;
    }

    fn readEncodedImagesWithConfigReported(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.EncodedRequest) !readers.BatchResult {
        try readers.validateEncodedRequest(request);
        var execution_cfg = self.routedReaderConfig(cfg);
        try execution_cfg.validate();
        const local_reader = isLocalReaderProvider(execution_cfg.provider, execution_cfg.resolvedUrl());
        var capability_lease_fields = CapabilityLeaseFields{};
        var capability_auth_value: ?[]u8 = null;
        defer if (capability_auth_value) |value| alloc.free(value);
        var capability_auth_storage: [2][2][]const u8 = undefined;
        var capability_auth_count: usize = 0;
        if (!local_reader and execution_cfg.provider == .antfly and
            (execution_cfg.bearer_token orelse execution_cfg.api_key) != null)
        {
            capability_auth_value = try std.fmt.allocPrint(
                alloc,
                "Bearer {s}",
                .{execution_cfg.bearer_token orelse execution_cfg.api_key.?},
            );
            capability_auth_storage[capability_auth_count] = .{ "Authorization", capability_auth_value.? };
            capability_auth_count += 1;
        }
        capability_auth_count = try self.execution.routing.appendHeaders(
            &capability_auth_storage,
            capability_auth_count,
        );
        const capability_auth_headers = capability_auth_storage[0..capability_auth_count];
        const capabilities: ?inference_work.InferenceCapabilities = if (!local_reader and execution_cfg.provider == .antfly) blk: {
            const endpoint = execution_cfg.resolvedUrl() orelse return error.InvalidReaderConfig;
            const lease = try self.capabilityCache().getOrDiscoverLeaseWithContext(
                self.http,
                endpoint,
                execution_cfg.model orelse "",
                .read,
                capability_auth_headers,
                self.execution.waitContext(),
            );
            capability_lease_fields = CapabilityLeaseFields.init(lease);
            capability_lease_fields.apply(&execution_cfg);
            break :blk lease.capabilities;
        } else try self.readerCapabilities(alloc, execution_cfg);
        var use_framed_transport = false;
        if (capabilities) |resolved| {
            var encoded_bytes: usize = 0;
            var decoded_pixels: u64 = 0;
            const transport: inference_work.AttachmentTransport = if (local_reader)
                .borrowed_binary
            else
                remoteAttachmentTransport(resolved, .data_uri);
            use_framed_transport = transport == .framed_binary;
            for (request.images) |image| {
                try resolved.validateMimeType(image.mime_type);
                const resident = try transport.wireSize(image.bytes.len, image.mime_type.len);
                encoded_bytes = std.math.add(usize, encoded_bytes, resident) catch
                    return error.InferenceEncodedBytesExceeded;
                const pixels = try inference_work.encodedImagePixels(image.mime_type, image.bytes);
                decoded_pixels = std.math.add(u64, decoded_pixels, pixels) catch
                    return error.InferenceDecodedPixelsExceeded;
            }
            try resolved.validateInvocation(.read, .{
                .item_count = request.images.len,
                .modalities = .{ .image = true },
                .text_bytes = if (request.prompt) |prompt| prompt.len else 0,
                .max_text_bytes_per_item = if (request.prompt) |prompt| prompt.len else 0,
                .requested_output_tokens_per_item = if (request.max_tokens) |tokens|
                    if (tokens > 0) std.math.cast(usize, tokens) orelse std.math.maxInt(usize) else 0
                else
                    0,
                .encoded_media_bytes = encoded_bytes,
                .decoded_pixels = decoded_pixels,
                .max_media_parts_per_item = if (request.images.len > 0) 1 else 0,
            });
        } else if (local_reader) {
            return error.InvalidInferenceCapabilities;
        }
        if (local_reader) {
            const local = self.antfly_provider orelse return error.UnsupportedReaderProvider;
            if (local.read_encoded_images_reported_with_context) |read_reported|
                return try managed_embedder.AntflyProviderBoundary.call(
                    "read_encoded_images_reported_with_context",
                    local.boundary_dispatch,
                    read_reported,
                    .{ local.ptr, alloc, execution_cfg.model orelse "", request, self.requestContext() },
                );
            if (local.read_encoded_images_reported) |read_reported|
                return try managed_embedder.AntflyProviderBoundary.call(
                    "read_encoded_images_reported",
                    local.boundary_dispatch,
                    read_reported,
                    .{ local.ptr, alloc, execution_cfg.model orelse "", request },
                );
            if (local.read_encoded_images_with_context != null or local.read_encoded_images != null) {
                const items = if (local.read_encoded_images_with_context) |read_encoded_images|
                    try managed_embedder.AntflyProviderBoundary.call(
                        "read_encoded_images_with_context",
                        local.boundary_dispatch,
                        read_encoded_images,
                        .{ local.ptr, alloc, execution_cfg.model orelse "", request, self.requestContext() },
                    )
                else
                    try managed_embedder.AntflyProviderBoundary.call(
                        "read_encoded_images",
                        local.boundary_dispatch,
                        local.read_encoded_images.?,
                        .{ local.ptr, alloc, execution_cfg.model orelse "", request },
                    );
                errdefer {
                    for (items) |*item| readers.deinitResult(alloc, item);
                    alloc.free(items);
                }
                if (items.len != request.images.len) return error.InvalidReaderResponse;
                for (items, request.images) |*item, image| {
                    if (item.item_id.len == 0 and image.item_id.len > 0) item.item_id = try alloc.dupe(u8, image.item_id);
                    if (item.source_fingerprint == null) item.source_fingerprint = if (image.source_fingerprint) |value| try alloc.dupe(u8, value) else null;
                    if (item.page_number == null) item.page_number = image.page_number;
                }
                return .{
                    .items = items,
                    .execution = .{ .requested_items = request.images.len, .serial_items = request.images.len },
                };
            }
        }

        if (use_framed_transport) {
            return readers.readEncodedWithConfigReported(alloc, self.http, execution_cfg, request, .{
                .source_table = self.execution.routing.source_table,
                .timeout_ms = try self.execution.remainingTimeoutMs(platform.time.monotonicNs(), max_asset_provider_timeout_ms),
                .cancellation = httpx.CancellationToken.fromCallback(
                    self.execution.cancellation.ptr,
                    self.execution.cancellation.is_cancelled_fn,
                ),
            }) catch |err| {
                if (err == error.InferenceCapabilitiesStale and execution_cfg.provider == .antfly) {
                    const endpoint = execution_cfg.resolvedUrl() orelse return err;
                    try self.capabilityCache().invalidate(endpoint, execution_cfg.model orelse "", .read, capability_auth_headers);
                }
                return err;
            };
        }

        // Remote providers and older embedded runtimes retain compatibility by
        // adapting at the transport boundary. The PDF/enrichment path itself
        // never materializes base64 when the local binary callback exists.
        const urls = try alloc.alloc([]const u8, request.images.len);
        var initialized: usize = 0;
        defer {
            for (urls[0..initialized]) |url| alloc.free(@constCast(url));
            alloc.free(urls);
        }
        for (request.images, 0..) |image, i| {
            const encoded_len = std.base64.standard.Encoder.calcSize(image.bytes.len);
            const prefix_len = "data:;base64,".len + image.mime_type.len;
            const url = try alloc.alloc(u8, prefix_len + encoded_len);
            errdefer alloc.free(url);
            const prefix = try std.fmt.bufPrint(url[0..prefix_len], "data:{s};base64,", .{image.mime_type});
            std.debug.assert(prefix.len == prefix_len);
            _ = std.base64.standard.Encoder.encode(url[prefix_len..], image.bytes);
            urls[i] = url;
            initialized += 1;
        }
        var adapted = try self.readImagesWithConfigReported(alloc, execution_cfg, .{
            .images = urls,
            .prompt = request.prompt,
            .max_tokens = request.max_tokens,
            .inline_content_trust = .trusted_internal,
            .source_fingerprint = request.source_fingerprint,
            .max_response_bytes = request.max_response_bytes,
        });
        errdefer adapted.deinit(alloc);
        const items = adapted.items;
        if (items.len != request.images.len) return error.InvalidReaderResponse;
        for (items, request.images) |*item, image| {
            if (item.item_id.len == 0 and image.item_id.len > 0) item.item_id = try alloc.dupe(u8, image.item_id);
            if (item.source_fingerprint == null) item.source_fingerprint = if (image.source_fingerprint) |value| try alloc.dupe(u8, value) else null;
            if (item.page_number == null) item.page_number = image.page_number;
        }
        return .{
            .items = items,
            .execution = adapted.execution,
        };
    }

    fn readRasterImagesWithConfigReported(
        self: *Runtime,
        alloc: Allocator,
        cfg: readers.Config,
        request: readers.RasterRequest,
    ) !readers.BatchResult {
        try readers.validateRasterRequest(request);
        const execution_cfg = self.routedReaderConfig(cfg);
        try execution_cfg.validate();
        if (!isLocalReaderProvider(execution_cfg.provider, execution_cfg.resolvedUrl()))
            return error.BorrowedRasterUnsupported;
        const capabilities = (try self.readerCapabilities(alloc, execution_cfg)) orelse
            return error.InvalidInferenceCapabilities;
        if (!capabilities.borrowed_rasters) return error.BorrowedRasterUnsupported;
        const local = self.antfly_provider orelse return error.UnsupportedReaderProvider;
        if (local.read_raster_images_reported == null and
            local.read_raster_images_reported_with_context == null)
            return error.BorrowedRasterUnsupported;

        var decoded_pixels: u64 = 0;
        for (request.images) |raster| {
            try raster.validate();
            decoded_pixels = std.math.add(u64, decoded_pixels, try raster.pixels()) catch
                return error.InferenceDecodedPixelsExceeded;
        }
        try capabilities.validateInvocation(.read, .{
            .item_count = request.images.len,
            .modalities = .{ .image = true },
            .text_bytes = if (request.prompt) |prompt| prompt.len else 0,
            .max_text_bytes_per_item = if (request.prompt) |prompt| prompt.len else 0,
            .requested_output_tokens_per_item = if (request.max_tokens) |tokens|
                if (tokens > 0) std.math.cast(usize, tokens) orelse std.math.maxInt(usize) else 0
            else
                0,
            // The model receives decoded pixels. Raw resident bytes are
            // covered by the caller's composite window and local admission;
            // they are not encoded codec input and must not consume that cap.
            .encoded_media_bytes = 0,
            .decoded_pixels = decoded_pixels,
            .max_media_parts_per_item = 1,
        });

        var batch = if (local.read_raster_images_reported_with_context) |read_reported|
            try managed_embedder.AntflyProviderBoundary.call(
                "read_raster_images_reported_with_context",
                local.boundary_dispatch,
                read_reported,
                .{ local.ptr, alloc, execution_cfg.model orelse "", request, self.requestContext() },
            )
        else
            try managed_embedder.AntflyProviderBoundary.call(
                "read_raster_images_reported",
                local.boundary_dispatch,
                local.read_raster_images_reported.?,
                .{ local.ptr, alloc, execution_cfg.model orelse "", request },
            );
        errdefer batch.deinit(alloc);
        if (batch.items.len != request.images.len) return error.InvalidReaderResponse;
        try batch.execution.validate(batch.items.len);
        for (batch.items, request.images) |result, raster| {
            if (!readerResultMatchesRasterIdentity(result, raster))
                return error.InvalidReaderResponseIdentity;
        }
        return batch;
    }

    fn transcribe(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();
        cfg_parsed.value.framed_attachments = false;
        cfg_parsed.value = self.routedTranscriberConfig(cfg_parsed.value);
        cfg_parsed.value.max_response_bytes = self.responseLimitForTask(.transcriber, 1);
        const antfly_model = if (cfg_parsed.value.provider == .antfly)
            try requiredAntflyTranscriberModel(cfg_parsed.value)
        else
            null;

        if (isLocalTranscriberProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl())) {
            const local = self.antfly_provider orelse return error.UnsupportedTranscriberProvider;
            const transcribe_request = transcribing.Request{
                .url = request.source_text,
                .language = cfg_parsed.value.language_code,
            };
            var result = if (local.transcribe_audio_with_context) |transcribe_audio|
                try managed_embedder.AntflyProviderBoundary.call(
                    "transcribe_audio_with_context",
                    local.boundary_dispatch,
                    transcribe_audio,
                    .{ local.ptr, alloc, antfly_model.?, transcribe_request, self.requestContext() },
                )
            else if (local.transcribe_audio) |transcribe_audio|
                try managed_embedder.AntflyProviderBoundary.call(
                    "transcribe_audio",
                    local.boundary_dispatch,
                    transcribe_audio,
                    .{ local.ptr, alloc, antfly_model.?, transcribe_request },
                )
            else
                return error.UnsupportedTranscriberProvider;
            defer transcribing.deinitResponse(alloc, &result);

            if (isJsonContentType(request.content_type)) {
                return try std.json.Stringify.valueAlloc(alloc, result, .{});
            }
            return try alloc.dupe(u8, result.text orelse "");
        }

        var capability_auth_value: ?[]u8 = null;
        defer if (capability_auth_value) |value| alloc.free(value);
        var capability_auth_storage: [2][2][]const u8 = undefined;
        var capability_auth_count: usize = 0;
        var capability_lease_fields = CapabilityLeaseFields{};
        if (cfg_parsed.value.provider == .antfly and
            (cfg_parsed.value.bearer_token orelse cfg_parsed.value.api_key) != null)
        {
            capability_auth_value = try std.fmt.allocPrint(
                alloc,
                "Bearer {s}",
                .{cfg_parsed.value.bearer_token orelse cfg_parsed.value.api_key.?},
            );
            capability_auth_storage[capability_auth_count] = .{ "Authorization", capability_auth_value.? };
            capability_auth_count += 1;
        }
        capability_auth_count = try self.execution.routing.appendHeaders(&capability_auth_storage, capability_auth_count);
        const capability_auth_headers = capability_auth_storage[0..capability_auth_count];
        if (cfg_parsed.value.provider == .antfly) {
            const endpoint = cfg_parsed.value.resolvedUrl() orelse return error.InvalidTranscribingConfig;
            const lease = try self.capabilityCache().getOrDiscoverLeaseWithContext(
                self.http,
                endpoint,
                antfly_model.?,
                .transcribe,
                capability_auth_headers,
                self.execution.waitContext(),
            );
            if (lease.capabilities) |capabilities| {
                try capabilities.validateInvocation(.transcribe, .{
                    .item_count = 1,
                    .modalities = .{ .audio = true },
                    .max_media_parts_per_item = 1,
                });
                cfg_parsed.value.framed_attachments = capabilities.framed_attachments;
            }
            capability_lease_fields = CapabilityLeaseFields.init(lease);
            capability_lease_fields.apply(&cfg_parsed.value);
        }

        var result = transcribing.transcribeWithConfig(
            alloc,
            self.http,
            cfg_parsed.value,
            .{ .url = request.source_text },
            .{
                .source_table = self.execution.routing.source_table,
                .timeout_ms = try self.execution.remainingTimeoutMs(platform.time.monotonicNs(), max_asset_provider_timeout_ms),
                .cancellation = httpx.CancellationToken.fromCallback(
                    self.execution.cancellation.ptr,
                    self.execution.cancellation.is_cancelled_fn,
                ),
            },
        ) catch |err| {
            if (err == error.InferenceCapabilitiesStale and cfg_parsed.value.provider == .antfly) {
                const endpoint = cfg_parsed.value.resolvedUrl() orelse return err;
                try self.capabilityCache().invalidate(
                    endpoint,
                    antfly_model.?,
                    .transcribe,
                    capability_auth_headers,
                );
            }
            return err;
        };
        defer transcribing.deinitResponse(alloc, &result);

        if (isJsonContentType(request.content_type)) {
            return try std.json.Stringify.valueAlloc(alloc, result, .{});
        }
        return try alloc.dupe(u8, result.text orelse "");
    }

    fn extract(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        if (request.media.len > 0 and !request.inline_media_trusted) return error.UntrustedInlineMedia;
        for (request.media) |media| try validateEncodedMedia(media);
        var cfg = try extracting.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);
        try self.routeExtractorConfig(alloc, &cfg);

        const local_extractor = isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl());
        const capabilities = if (local_extractor)
            try self.extractorCapabilities(alloc, cfg)
        else
            try self.bindExtractorCapabilityLease(alloc, &cfg);
        if (capabilities) |resolved| {
            try validateExtractorInvocation(
                alloc,
                resolved,
                extractorAttachmentTransport(cfg),
                &.{request},
            );
        }

        const content_json = try extractionContentJsonAlloc(alloc, request.source_text, request.source_parts_json);
        defer alloc.free(content_json);
        const input = extracting.Input{ .content_json = content_json };
        const attachments = try alloc.alloc(extracting.Attachment, request.media.len);
        defer alloc.free(attachments);
        for (request.media, attachments) |media, *attachment| attachment.* = .{
            .input_index = 0,
            .bytes = media.bytes,
            .mime_type = media.mime_type,
        };
        const extract_request = extracting.Request{
            .inputs = &.{input},
            .schema_json = cfg.schema_json,
            .options_json = cfg.options_json,
            .attachments = attachments,
            .max_response_bytes = self.responseLimitForTask(.extractor, 1),
        };

        var response = if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.UnsupportedExtractionProvider;
            break :blk if (local.extract_with_context) |extract_fn|
                try managed_embedder.AntflyProviderBoundary.call(
                    "extract_with_context",
                    local.boundary_dispatch,
                    extract_fn,
                    .{ local.ptr, alloc, cfg.model, extract_request, self.requestContext() },
                )
            else if (local.extract) |extract_fn|
                try managed_embedder.AntflyProviderBoundary.call(
                    "extract",
                    local.boundary_dispatch,
                    extract_fn,
                    .{ local.ptr, alloc, cfg.model, extract_request },
                )
            else
                return error.UnsupportedExtractionProvider;
        } else extracting.extractWithConfigAndOptions(alloc, self.http, cfg, extract_request, .{
            .source_table = self.execution.routing.source_table,
            .timeout_ms = try self.execution.remainingTimeoutMs(platform.time.monotonicNs(), max_asset_provider_timeout_ms),
            .cancellation = httpx.CancellationToken.fromCallback(
                self.execution.cancellation.ptr,
                self.execution.cancellation.is_cancelled_fn,
            ),
        }) catch |err| {
            if (err == error.InferenceCapabilitiesStale)
                try self.invalidateExtractorCapabilityLease(alloc, cfg);
            return err;
        };
        defer response.deinit();

        if (isJsonContentType(request.content_type) or request.content_type.len == 0) {
            return try extracting.firstResultJsonAlloc(alloc, response.json);
        }
        return try alloc.dupe(u8, response.json);
    }

    fn effectiveGeneratorConfig(self: *const Runtime, cfg: generating_runtime.GeneratorConfig) generating_runtime.GeneratorConfig {
        var effective = cfg;
        if (effective.provider == .antfly and effective.url.len == 0 and self.antfly_provider == null) {
            if (self.inference_api_url) |url| effective.url = url;
        }
        return effective;
    }

    fn effectiveReaderConfig(self: *const Runtime, cfg: readers.Config) readers.Config {
        var effective = cfg;
        if (effective.provider == .antfly and effective.resolvedUrl() == null and self.antfly_provider == null) {
            effective.url = self.inference_api_url;
        }
        return effective;
    }

    fn effectiveTranscriberConfig(self: *const Runtime, cfg: transcribing.Config) transcribing.Config {
        var effective = cfg;
        if (effective.provider == .antfly and effective.resolvedUrl() == null and self.antfly_provider == null) {
            effective.url = self.inference_api_url;
        }
        return effective;
    }

    fn effectiveExtractorConfig(self: *const Runtime, cfg: extracting.Config) extracting.Config {
        var effective = cfg;
        if (effective.provider == .antfly and effective.resolvedUrl() == null and self.antfly_provider == null) {
            if (self.inference_api_url) |url| effective.url = url;
        }
        return effective;
    }
};

test "asset producer runtime invocation context can only tighten configured controls" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();
    var configured_canceled = std.atomic.Value(bool).init(false);
    var invocation_canceled = std.atomic.Value(bool).init(false);
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    runtime.execution.deadline_ns = 200;
    runtime.execution.cancellation = CancellationToken.fromAtomic(&configured_canceled);
    runtime.execution.max_response_bytes = 1024;

    var cancellation: Runtime.RuntimeInvocationCancellation = undefined;
    const scoped = Runtime.invocationRuntime(&runtime, .{
        .deadline_ns = 300,
        .cancellation = CancellationToken.fromAtomic(&invocation_canceled),
        .max_response_bytes = 2048,
    }, &cancellation);
    try std.testing.expectEqual(@as(?u64, 200), scoped.execution.deadline_ns);
    try std.testing.expectEqual(@as(?usize, 1024), scoped.execution.max_response_bytes);
    try std.testing.expect(!scoped.execution.cancellation.isCancelled());
    invocation_canceled.store(true, .release);
    try std.testing.expect(scoped.execution.cancellation.isCancelled());
    invocation_canceled.store(false, .release);
    configured_canceled.store(true, .release);
    try std.testing.expect(scoped.execution.cancellation.isCancelled());
}

const GeneratorToolOutput = enum {
    arguments,
    content,
};

const GeneratorProducerConfig = struct {
    generator: generating_runtime.GeneratorConfig,
    parsed: std.json.Parsed(std.json.Value),
    tool_choice: ?std.json.Value = null,
    tool_name: ?[]const u8 = null,
    tool_output: GeneratorToolOutput = .content,

    fn deinit(self: *GeneratorProducerConfig, alloc: Allocator) void {
        self.generator.deinit(alloc);
        self.parsed.deinit();
        self.* = undefined;
    }
};

fn parseGeneratorProducerConfig(alloc: Allocator, raw: []const u8) !GeneratorProducerConfig {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGeneratorConfig;

    var cfg = try generatorConfigFromValue(alloc, parsed.value);
    errdefer cfg.deinit(alloc);

    const tools = parsed.value.object.get("tools");
    const tool_choice = parsed.value.object.get("tool_choice");
    const tool_name = if (parsed.value.object.get("tool_name")) |value|
        if (value == .string) value.string else null
    else
        forcedToolName(tool_choice);
    const tool_output = if (parsed.value.object.get("tool_output")) |value| blk: {
        if (value != .string) return error.InvalidGeneratorToolConfig;
        if (std.mem.eql(u8, value.string, "arguments")) break :blk GeneratorToolOutput.arguments;
        if (std.mem.eql(u8, value.string, "content")) break :blk GeneratorToolOutput.content;
        return error.InvalidGeneratorToolConfig;
    } else if (tools != null) GeneratorToolOutput.arguments else GeneratorToolOutput.content;

    return .{
        .generator = cfg,
        .parsed = parsed,
        .tool_choice = tool_choice,
        .tool_name = tool_name,
        .tool_output = tool_output,
    };
}

fn generatorConfigFromValue(alloc: Allocator, value: std.json.Value) !generating_runtime.GeneratorConfig {
    if (value != .object) return error.InvalidGeneratorConfig;
    // Asset-only envelope fields are handled here; all provider configuration
    // is parsed by the canonical generator contract (including future fields).
    var object: std.json.ObjectMap = .empty;
    defer object.deinit(alloc);
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        if (std.mem.eql(u8, key, "tools") or std.mem.eql(u8, key, "tool_choice") or
            std.mem.eql(u8, key, "tool_name") or std.mem.eql(u8, key, "tool_output")) continue;
        try object.put(alloc, key, entry.value_ptr.*);
    }
    var cfg = try generating_runtime.parseConfigFromValue(alloc, .{ .object = object });
    errdefer cfg.deinit(alloc);
    if (value.object.get("tools")) |tools| cfg.tools_json = try std.json.Stringify.valueAlloc(alloc, tools, .{});
    if (value.object.get("tool_choice")) |choice| cfg.tool_choice_json = try std.json.Stringify.valueAlloc(alloc, choice, .{});
    return cfg;
}

fn jsonStringField(value: std.json.Value, field: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return if (found == .string) found.string else null;
}

fn forcedToolName(tool_choice: ?std.json.Value) ?[]const u8 {
    const choice = tool_choice orelse return null;
    switch (choice) {
        .object => |obj| {
            const function = obj.get("function") orelse return null;
            if (function != .object) return null;
            const name = function.object.get("name") orelse return null;
            return if (name == .string) name.string else null;
        },
        else => return null,
    }
}

fn toolCallArgumentsOutputAlloc(alloc: Allocator, calls: []const generating_runtime.ToolCall, expected_name: ?[]const u8) ![]u8 {
    for (calls) |call| {
        if (expected_name) |name| {
            if (!std.mem.eql(u8, call.name, name)) continue;
        }
        return try alloc.dupe(u8, call.arguments);
    }
    return error.MissingToolCall;
}

fn isLocalReaderProvider(provider: readers.Provider, url: ?[]const u8) bool {
    return provider == .antfly and url == null;
}

fn isLocalTranscriberProvider(provider: transcribing.Provider, url: ?[]const u8) bool {
    return provider == .antfly and url == null;
}

fn requiredAntflyTranscriberModel(cfg: transcribing.Config) ![]const u8 {
    if (cfg.provider != .antfly) return error.InvalidTranscribingConfig;
    const model = cfg.model orelse return error.InvalidTranscribingConfig;
    const canonical = std.mem.trim(u8, model, " \t\r\n");
    if (canonical.len == 0) return error.InvalidTranscribingConfig;
    return canonical;
}

test "antfly transcription requires an explicit routing model" {
    try std.testing.expectError(
        error.InvalidTranscribingConfig,
        requiredAntflyTranscriberModel(.{ .provider = .antfly }),
    );
    try std.testing.expectError(
        error.InvalidTranscribingConfig,
        requiredAntflyTranscriberModel(.{ .provider = .antfly, .model = " \t" }),
    );
    try std.testing.expectEqualStrings(
        "openai/whisper-tiny",
        try requiredAntflyTranscriberModel(.{ .provider = .antfly, .model = "openai/whisper-tiny" }),
    );
}

test "asset producer runtime routes every remote model family through the distributed default before admission" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.init(alloc, io_impl.io());
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{
        .inference_api_url = "https://inference.example/ai/v1",
        .source_table = "docs",
    });
    defer runtime.deinit();
    const producer = runtime.producer();

    const requests = [_]asset_producer.Request{
        .{ .producer_type = .reader, .config_json = "{\"provider\":\"antfly\",\"model\":\"reader\"}", .source_text = "image" },
        .{ .producer_type = .generator, .config_json = "{\"provider\":\"antfly\",\"model\":\"generator\"}", .source_text = "prompt" },
        .{ .producer_type = .extractor, .config_json = "{\"provider\":\"antfly\",\"model\":\"extractor\"}", .source_text = "content" },
        .{ .producer_type = .transcriber, .config_json = "{\"provider\":\"antfly\",\"model\":\"transcriber\"}", .source_text = "audio" },
    };
    for (requests) |request| {
        const plan = try producer.invocationMemoryForRequests(alloc, &.{request});
        try std.testing.expect(plan.fixed_bytes > 0);
    }
}

fn isLocalExtractionProvider(provider: extracting.Provider, url: ?[]const u8) bool {
    return provider == .antfly and url == null;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

/// Request-owned copies of fixed-size capability lease values. `slice()` on a
/// RoutingToken or CapabilityRevision points into that struct, so assigning a
/// slice captured from a block-local lease would leave an HTTP config pointing
/// at expired stack storage before the request is sent.
const CapabilityLeaseFields = struct {
    token: ?remote_capabilities.RoutingToken = null,
    revision: ?remote_capabilities.CapabilityRevision = null,

    fn init(lease: remote_capabilities.CapabilityLease) CapabilityLeaseFields {
        return .{
            .token = lease.routing_token,
            .revision = lease.descriptor_revision,
        };
    }

    fn apply(self: *const CapabilityLeaseFields, cfg: anytype) void {
        cfg.capability_token = if (self.token) |*value| value.slice() else null;
        cfg.capability_revision = if (self.revision) |*value| value.slice() else null;
    }
};

test "capability lease HTTP fields own storage beyond the lease stack frame" {
    const lease = remote_capabilities.CapabilityLease{
        .capabilities = null,
        .routing_token = try remote_capabilities.RoutingToken.init("route-token"),
        .descriptor_revision = try remote_capabilities.CapabilityRevision.init(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ),
    };
    var fields = CapabilityLeaseFields.init(lease);
    var cfg = readers.Config{ .provider = .antfly };
    fields.apply(&cfg);
    try std.testing.expectEqualStrings("route-token", cfg.capability_token.?);
    try std.testing.expectEqualStrings(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        cfg.capability_revision.?,
    );
    try std.testing.expect(cfg.capability_token.?.ptr != lease.routing_token.?.slice().ptr);
    try std.testing.expect(cfg.capability_revision.?.ptr != lease.descriptor_revision.?.slice().ptr);
}

fn replaceCapabilityLeaseFields(
    alloc: Allocator,
    cfg: anytype,
    lease: remote_capabilities.CapabilityLease,
) !void {
    const new_token = if (lease.routing_token) |token|
        try alloc.dupe(u8, token.slice())
    else
        null;
    errdefer if (new_token) |value| alloc.free(value);
    const new_revision = if (lease.descriptor_revision) |revision|
        try alloc.dupe(u8, revision.slice())
    else
        null;
    errdefer if (new_revision) |value| alloc.free(value);

    if (cfg.capability_token) |value| alloc.free(@constCast(value));
    if (cfg.capability_revision) |value| alloc.free(@constCast(value));
    cfg.capability_token = new_token;
    cfg.capability_revision = new_revision;
}

fn readerResultMatchesImageIdentity(result: readers.Result, image: readers.EncodedImage) bool {
    return std.mem.eql(u8, result.item_id, image.item_id) and
        optionalStringsEqual(result.source_fingerprint, image.source_fingerprint) and
        result.page_number == image.page_number;
}

fn readerResultMatchesRasterIdentity(result: readers.Result, raster: readers.RasterImage) bool {
    return std.mem.eql(u8, result.item_id, raster.item_id) and
        optionalStringsEqual(result.source_fingerprint, raster.source_fingerprint) and
        result.page_number == raster.page_number;
}

/// Mirrors the native Florence chunk policy at the caller boundary. Keeping
/// the exact environment contract here prevents an outer 64-image invocation
/// from being silently repartitioned into differently attributed inner
/// chunks. The server retains the same hard ceiling independently.
fn localReaderBatchMaxImages() usize {
    return reader_config.nativeBatchSize(platform.env.getenvUsize("ANTFLY_INFERENCE_READ_BATCH_SIZE"));
}

fn readerBatchEnd(fingerprints: []const ?[]const u8, start: usize, max_images: usize) usize {
    std.debug.assert(start < fingerprints.len);
    std.debug.assert(max_images > 0);
    const limit = @min(start +| max_images, fingerprints.len);
    var end = start + 1;
    while (end < limit and optionalStringsEqual(fingerprints[start], fingerprints[end])) : (end += 1) {}
    return end;
}

fn encodedReaderBatchEnd(
    images: []const readers.EncodedImage,
    start: usize,
    capabilities: inference_work.BatchCapabilities,
    transport: inference_work.AttachmentTransport,
) !usize {
    const item_end = @min(start +| capabilities.max_items, images.len);
    var end = start;
    var bytes: usize = 0;
    var pixels: u64 = 0;
    while (end < item_end) : (end += 1) {
        const resident = transport.wireSize(
            images[end].bytes.len,
            images[end].mime_type.len,
        ) catch break;
        const next = std.math.add(usize, bytes, resident) catch break;
        const item_pixels = try inference_work.encodedImagePixels(images[end].mime_type, images[end].bytes);
        const next_pixels = std.math.add(u64, pixels, item_pixels) catch break;
        if (capabilities.max_encoded_media_bytes) |limit| {
            if (next > limit and end > start) break;
        }
        if (capabilities.max_decoded_pixels) |limit| {
            if (next_pixels > limit) {
                if (end == start) return error.InferenceDecodedPixelsExceeded;
                break;
            }
        }
        bytes = next;
        pixels = next_pixels;
    }
    return @max(start + 1, end);
}

const GeneratorItemShape = struct {
    modalities: inference_work.Modalities = .{},
    text_bytes: usize = 0,
    encoded_media_bytes: usize = 0,
    decoded_pixels: u64 = 0,
    media_parts: usize = 0,
};

fn mergeInferenceModalities(
    target: *inference_work.Modalities,
    value: inference_work.Modalities,
) void {
    const target_bits: u8 = @bitCast(target.*);
    const value_bits: u8 = @bitCast(value);
    target.* = @bitCast(target_bits | value_bits);
}

fn modalityForGeneratorMime(mime_type: []const u8) !inference_work.Modalities {
    const essence = inference_work.mimeTypeEssence(mime_type) catch
        return error.UnsupportedInferenceMimeType;
    if (std.ascii.startsWithIgnoreCase(essence, "image/")) return .{ .image = true };
    if (std.ascii.startsWithIgnoreCase(essence, "audio/")) return .{ .audio = true };
    if (std.ascii.eqlIgnoreCase(essence, "text/plain")) return .{ .text = true };
    if (std.ascii.eqlIgnoreCase(essence, "application/pdf")) return .{ .document = true };
    return error.UnsupportedInferenceMimeType;
}

fn validateEncodedMedia(media: asset_producer.EncodedMedia) !void {
    if (media.bytes.len == 0) return error.InvalidInferenceMedia;
    _ = inference_work.mimeTypeEssence(media.mime_type) catch
        return error.UnsupportedInferenceMimeType;
}

test "asset producer runtime rejects empty borrowed media" {
    try std.testing.expectError(
        error.InvalidInferenceMedia,
        validateEncodedMedia(.{ .bytes = &.{}, .mime_type = "image/png" }),
    );
    try std.testing.expectError(
        error.UnsupportedInferenceMimeType,
        validateEncodedMedia(.{ .bytes = "png", .mime_type = "" }),
    );
}

fn dataUriMimeType(uri: []const u8) ?[]const u8 {
    const parsed = inference_work.parseInlineDataUri(uri) catch return null;
    return if (parsed) |value| value.mime_type else null;
}

fn dataUriDecodedSize(uri: []const u8) !?usize {
    const parsed = (try inference_work.parseInlineDataUri(uri)) orelse return null;
    return parsed.decoded_size;
}

/// Validate the complete padded standard-base64 representation without
/// allocating its decoded payload. Return the exact decoded size.
fn validateStandardBase64(data: []const u8) !usize {
    return inference_work.validateCanonicalStandardBase64(data);
}

// Admission is defined in terms of the representation resident at the task
// boundary. Inline strings coexist with their decoded buffers, so charge the
// encoded source length after validating it rather than only its decoded size.
fn dataUriResidentSize(uri: []const u8) !?usize {
    const decoded_size = (try dataUriDecodedSize(uri)) orelse return null;
    if (decoded_size == 0) return error.InvalidDataURI;
    return uri.len;
}

fn inlineBase64ResidentSize(data: []const u8) !usize {
    if (try dataUriResidentSize(data)) |bytes| return bytes;
    if (try validateStandardBase64(data) == 0) return error.InvalidDataURI;
    return data.len;
}

fn inlineImagePixelsAlloc(
    alloc: Allocator,
    declared_mime_type: []const u8,
    encoded: []const u8,
) !u64 {
    if (try inference_work.parseInlineDataUri(encoded)) |_| {
        var decoded = try inference_work.decodeInlineDataUriAlloc(alloc, encoded);
        defer decoded.deinit(alloc);
        if (declared_mime_type.len > 0 and !inference_work.mediaTypesCompatible(declared_mime_type, decoded.mime_type))
            return error.InvalidInferenceMedia;
        return try inference_work.encodedImagePixels(decoded.mime_type, decoded.data);
    }
    const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(encoded) catch
        return error.InvalidDataURI;
    if (decoded_len == 0) return error.InvalidDataURI;
    const decoded = try alloc.alloc(u8, decoded_len);
    defer alloc.free(decoded);
    std.base64.standard.Decoder.decode(decoded, encoded) catch return error.InvalidDataURI;
    return try inference_work.encodedImagePixels(declared_mime_type, decoded);
}

fn remoteAttachmentTransport(capabilities: ?inference_work.InferenceCapabilities, fallback: inference_work.AttachmentTransport) inference_work.AttachmentTransport {
    return if (capabilities != null and capabilities.?.framed_attachments) .framed_binary else fallback;
}

fn extractorAttachmentTransport(cfg: extracting.Config) inference_work.AttachmentTransport {
    return if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl()))
        .borrowed_binary
    else if (cfg.provider == .antfly and cfg.framed_attachments)
        .framed_binary
    else
        .base64_payload;
}

fn generatorRequestShape(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    request: asset_producer.Request,
    encoded_byte_limit: ?usize,
) !GeneratorItemShape {
    var shape = GeneratorItemShape{};
    if (request.source_parts_json) |raw_parts| {
        const parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts, &.{}, false);
        defer freeGeneratorContentParts(alloc, parts);
        for (parts) |part| switch (part) {
            .text => |text| {
                shape.modalities.text = true;
                shape.text_bytes = std.math.add(usize, shape.text_bytes, text.len) catch
                    return error.InferenceTextBytesExceeded;
                try capabilities.validateMimeType("text/plain");
            },
            .image_url => |image| {
                shape.modalities.image = true;
                shape.media_parts += 1;
                if (dataUriMimeType(image.url)) |mime_type| try capabilities.validateMimeType(mime_type);
                if (try dataUriResidentSize(image.url)) |media_bytes| {
                    shape.encoded_media_bytes = std.math.add(usize, shape.encoded_media_bytes, media_bytes) catch
                        return error.InferenceEncodedBytesExceeded;
                    if (encoded_byte_limit) |limit| {
                        if (shape.encoded_media_bytes > limit) return error.InferenceEncodedBytesExceeded;
                    }
                    const pixels = try inlineImagePixelsAlloc(alloc, dataUriMimeType(image.url).?, image.url);
                    shape.decoded_pixels = std.math.add(u64, shape.decoded_pixels, pixels) catch
                        return error.InferenceDecodedPixelsExceeded;
                }
            },
            .media => |media| {
                shape.media_parts += 1;
                if (media.mime_type.len == 0) return error.UnsupportedInferenceMimeType;
                try capabilities.validateMimeType(media.mime_type);
                mergeInferenceModalities(&shape.modalities, try modalityForGeneratorMime(media.mime_type));
                const media_bytes = if (media.url) |url|
                    (try dataUriResidentSize(url)) orelse 0
                else
                    try inlineBase64ResidentSize(media.data);
                shape.encoded_media_bytes = std.math.add(usize, shape.encoded_media_bytes, media_bytes) catch
                    return error.InferenceEncodedBytesExceeded;
                if (encoded_byte_limit) |limit| {
                    if (shape.encoded_media_bytes > limit) return error.InferenceEncodedBytesExceeded;
                }
                const essence = inference_work.mimeTypeEssence(media.mime_type) catch
                    return error.UnsupportedInferenceMimeType;
                if (std.ascii.startsWithIgnoreCase(essence, "image/")) {
                    const inline_value = media.url orelse media.data;
                    if (media.url == null or dataUriMimeType(inline_value) != null) {
                        const pixels = try inlineImagePixelsAlloc(alloc, media.mime_type, inline_value);
                        shape.decoded_pixels = std.math.add(u64, shape.decoded_pixels, pixels) catch
                            return error.InferenceDecodedPixelsExceeded;
                    }
                }
            },
        };
    } else {
        shape.modalities.text = true;
        shape.text_bytes = request.source_text.len;
        try capabilities.validateMimeType("text/plain");
    }
    for (request.media) |media| {
        try validateEncodedMedia(media);
        try capabilities.validateMimeType(media.mime_type);
        mergeInferenceModalities(&shape.modalities, try modalityForGeneratorMime(media.mime_type));
        shape.media_parts += 1;
        const resident = try attachment_transport.wireSize(media.bytes.len, media.mime_type.len);
        shape.encoded_media_bytes = std.math.add(usize, shape.encoded_media_bytes, resident) catch
            return error.InferenceEncodedBytesExceeded;
        if (encoded_byte_limit) |limit| {
            if (shape.encoded_media_bytes > limit) return error.InferenceEncodedBytesExceeded;
        }
        const essence = inference_work.mimeTypeEssence(media.mime_type) catch
            return error.UnsupportedInferenceMimeType;
        if (std.ascii.startsWithIgnoreCase(essence, "image/")) {
            const pixels = try inference_work.encodedImagePixels(media.mime_type, media.bytes);
            shape.decoded_pixels = std.math.add(u64, shape.decoded_pixels, pixels) catch
                return error.InferenceDecodedPixelsExceeded;
        }
    }
    return shape;
}

fn validateGeneratorInvocation(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    requests: []const asset_producer.Request,
) !void {
    var invocation = inference_work.InvocationShape{ .item_count = requests.len };
    for (requests) |request| {
        const remaining_bytes = if (capabilities.batch.max_encoded_media_bytes) |limit|
            limit -| invocation.encoded_media_bytes
        else
            null;
        const item = try generatorRequestShape(alloc, capabilities, attachment_transport, request, remaining_bytes);
        mergeInferenceModalities(&invocation.modalities, item.modalities);
        invocation.text_bytes = std.math.add(usize, invocation.text_bytes, item.text_bytes) catch
            return error.InferenceTextBytesExceeded;
        invocation.max_text_bytes_per_item = @max(invocation.max_text_bytes_per_item, item.text_bytes);
        invocation.encoded_media_bytes = std.math.add(usize, invocation.encoded_media_bytes, item.encoded_media_bytes) catch
            return error.InferenceEncodedBytesExceeded;
        invocation.decoded_pixels = std.math.add(u64, invocation.decoded_pixels, item.decoded_pixels) catch
            return error.InferenceDecodedPixelsExceeded;
        invocation.max_media_parts_per_item = @max(invocation.max_media_parts_per_item, item.media_parts);
    }
    try capabilities.validateInvocation(.generate, invocation);
}

const ExtractorItemShape = struct {
    modalities: inference_work.Modalities = .{},
    encoded_media_bytes: usize = 0,
    decoded_pixels: u64 = 0,
    media_parts: usize = 0,
    prompt: []u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.prompt);
        self.* = undefined;
    }
};

fn addExtractorMediaShape(
    capabilities: inference_work.InferenceCapabilities,
    request: asset_producer.Request,
    shape: *ExtractorItemShape,
    mime_type: ?[]const u8,
    encoded_bytes: ?usize,
    decoded_pixels: ?u64,
    inline_data: bool,
) !void {
    if (inline_data and !request.inline_media_trusted) return error.UntrustedInlineMedia;
    if (mime_type) |mime| {
        if (!std.ascii.startsWithIgnoreCase(mime, "image/")) return error.UnsupportedInferenceMimeType;
        try capabilities.validateMimeType(mime);
    }
    shape.modalities.image = true;
    shape.media_parts = std.math.add(usize, shape.media_parts, 1) catch
        return error.InferenceMediaPartLimitExceeded;
    if (encoded_bytes) |bytes| shape.encoded_media_bytes = std.math.add(
        usize,
        shape.encoded_media_bytes,
        bytes,
    ) catch return error.InferenceEncodedBytesExceeded;
    if (decoded_pixels) |pixels| shape.decoded_pixels = std.math.add(
        u64,
        shape.decoded_pixels,
        pixels,
    ) catch return error.InferenceDecodedPixelsExceeded;
}

fn ensureExtractorEncodedBudget(
    shape: ExtractorItemShape,
    encoded_bytes: ?usize,
    encoded_byte_limit: ?usize,
) !void {
    const bytes = encoded_bytes orelse return;
    const limit = encoded_byte_limit orelse return;
    const next = std.math.add(usize, shape.encoded_media_bytes, bytes) catch
        return error.InferenceEncodedBytesExceeded;
    if (next > limit) return error.InferenceEncodedBytesExceeded;
}

fn extractorRequestShape(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    request: asset_producer.Request,
    encoded_byte_limit: ?usize,
) !ExtractorItemShape {
    var shape = ExtractorItemShape{ .prompt = try alloc.dupe(u8, "") };
    errdefer shape.deinit(alloc);
    var prompt = std.ArrayListUnmanaged(u8).empty;
    defer prompt.deinit(alloc);
    var parsed_parts_array = false;
    var saw_text_part = false;

    if (request.source_parts_json) |raw_parts| parts: {
        if (raw_parts.len == 0) break :parts;
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
        defer parsed.deinit();
        if (parsed.value != .array) {
            shape.modalities.text = true;
            try capabilities.validateMimeType("text/plain");
            break :parts;
        }
        parsed_parts_array = true;
        for (parsed.value.array.items) |part| {
            if (part != .object) return error.InvalidExtractionContent;
            const type_value = part.object.get("type") orelse return error.InvalidExtractionContent;
            if (type_value != .string) return error.InvalidExtractionContent;
            if (std.mem.eql(u8, type_value.string, "text")) {
                const text_value = part.object.get("text") orelse return error.InvalidExtractionContent;
                if (text_value != .string) return error.InvalidExtractionContent;
                saw_text_part = true;
                if (prompt.items.len > 0) try prompt.append(alloc, '\n');
                try prompt.appendSlice(alloc, text_value.string);
            } else if (std.mem.eql(u8, type_value.string, "image_url")) {
                const image_value = part.object.get("image_url") orelse return error.InvalidExtractionContent;
                const url = if (image_value == .string)
                    image_value.string
                else if (image_value == .object)
                    if (image_value.object.get("url")) |value|
                        if (value == .string) value.string else return error.InvalidExtractionContent
                    else
                        return error.InvalidExtractionContent
                else
                    return error.InvalidExtractionContent;
                const is_inline = dataUriMimeType(url) != null;
                const mime_type = dataUriMimeType(url);
                const encoded_bytes = try dataUriResidentSize(url);
                try ensureExtractorEncodedBudget(shape, encoded_bytes, encoded_byte_limit);
                try addExtractorMediaShape(
                    capabilities,
                    request,
                    &shape,
                    mime_type,
                    encoded_bytes,
                    if (is_inline) try inlineImagePixelsAlloc(alloc, mime_type.?, url) else null,
                    is_inline,
                );
            } else if (std.mem.eql(u8, type_value.string, "media")) {
                const declared_mime = if (part.object.get("mime_type")) |value|
                    if (value == .string) value.string else return error.InvalidExtractionContent
                else
                    null;
                if (part.object.get("url")) |url_value| {
                    if (url_value != .string) return error.InvalidExtractionContent;
                    const inferred_mime = dataUriMimeType(url_value.string);
                    const effective_mime = declared_mime orelse inferred_mime;
                    const encoded_bytes = try dataUriResidentSize(url_value.string);
                    try ensureExtractorEncodedBudget(shape, encoded_bytes, encoded_byte_limit);
                    try addExtractorMediaShape(
                        capabilities,
                        request,
                        &shape,
                        effective_mime,
                        encoded_bytes,
                        if (inferred_mime != null) try inlineImagePixelsAlloc(alloc, effective_mime.?, url_value.string) else null,
                        inferred_mime != null,
                    );
                } else if (part.object.get("data")) |data_value| {
                    if (data_value != .string) return error.InvalidExtractionContent;
                    const inferred_mime = dataUriMimeType(data_value.string);
                    const effective_mime = declared_mime orelse inferred_mime;
                    const encoded_bytes = try inlineBase64ResidentSize(data_value.string);
                    try ensureExtractorEncodedBudget(shape, encoded_bytes, encoded_byte_limit);
                    try addExtractorMediaShape(
                        capabilities,
                        request,
                        &shape,
                        effective_mime,
                        encoded_bytes,
                        if (effective_mime) |mime| try inlineImagePixelsAlloc(alloc, mime, data_value.string) else null,
                        true,
                    );
                } else return error.InvalidExtractionContent;
            } else if (!std.mem.eql(u8, type_value.string, "metadata")) {
                return error.InvalidExtractionContent;
            }
        }
    } else {
        try prompt.appendSlice(alloc, request.source_text);
    }

    for (request.media) |media| {
        try validateEncodedMedia(media);
        const encoded_bytes = try attachment_transport.wireSize(media.bytes.len, media.mime_type.len);
        try ensureExtractorEncodedBudget(shape, encoded_bytes, encoded_byte_limit);
        try addExtractorMediaShape(
            capabilities,
            request,
            &shape,
            media.mime_type,
            encoded_bytes,
            try inference_work.encodedImagePixels(media.mime_type, media.bytes),
            true,
        );
    }

    if (shape.media_parts == 0) {
        if (parsed_parts_array and (!saw_text_part or prompt.items.len == 0)) return error.InvalidExtractionContent;
        shape.modalities.text = true;
        try capabilities.validateMimeType("text/plain");
    } else {
        const owned_prompt = try prompt.toOwnedSlice(alloc);
        alloc.free(shape.prompt);
        shape.prompt = owned_prompt;
    }
    return shape;
}

fn validateExtractorInvocation(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    requests: []const asset_producer.Request,
) !void {
    if (capabilities.task != .extract or capabilities.result_cardinality != .one_per_item)
        return error.InvalidInferenceCapabilities;
    var invocation = inference_work.InvocationShape{ .item_count = requests.len };
    var uses_media: ?bool = null;
    for (requests) |request| {
        const remaining_bytes = if (capabilities.batch.max_encoded_media_bytes) |limit|
            limit -| invocation.encoded_media_bytes
        else
            null;
        var item = try extractorRequestShape(alloc, capabilities, attachment_transport, request, remaining_bytes);
        defer item.deinit(alloc);
        const item_uses_media = item.media_parts > 0;
        if (uses_media) |expected| {
            if (expected != item_uses_media) return error.BatchIncompatible;
        } else uses_media = item_uses_media;
        mergeInferenceModalities(&invocation.modalities, item.modalities);
        invocation.text_bytes = std.math.add(usize, invocation.text_bytes, item.prompt.len) catch
            return error.InferenceTextBytesExceeded;
        invocation.max_text_bytes_per_item = @max(invocation.max_text_bytes_per_item, item.prompt.len);
        invocation.schema_bytes = @max(invocation.schema_bytes, request.config_json.len);
        invocation.max_media_parts_per_item = @max(invocation.max_media_parts_per_item, item.media_parts);
        invocation.encoded_media_bytes = std.math.add(usize, invocation.encoded_media_bytes, item.encoded_media_bytes) catch
            return error.InferenceEncodedBytesExceeded;
        invocation.decoded_pixels = std.math.add(u64, invocation.decoded_pixels, item.decoded_pixels) catch
            return error.InferenceDecodedPixelsExceeded;
    }
    try capabilities.validateInvocation(.extract, invocation);
}

fn validateExtractorBatchCompatibility(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    requests: []const asset_producer.Request,
) !void {
    if (capabilities.task != .extract or capabilities.result_cardinality != .one_per_item or
        capabilities.batch.mode == .none) return error.InvalidInferenceCapabilities;
    var uses_media: ?bool = null;
    var media_prompt: ?[]u8 = null;
    defer if (media_prompt) |prompt| alloc.free(prompt);
    for (requests) |request| {
        if (request.content_type.len > 0 and !isJsonContentType(request.content_type)) return error.BatchIncompatible;
        var item = try extractorRequestShape(alloc, capabilities, attachment_transport, request, capabilities.batch.max_encoded_media_bytes);
        defer item.deinit(alloc);
        const item_uses_media = item.media_parts > 0;
        if (uses_media) |expected| {
            if (expected != item_uses_media) return error.BatchIncompatible;
        } else uses_media = item_uses_media;
        if (item_uses_media) {
            if (media_prompt) |expected| {
                if (!std.mem.eql(u8, expected, item.prompt)) return error.BatchIncompatible;
            } else {
                media_prompt = try alloc.dupe(u8, item.prompt);
            }
        }
    }
}

fn validateExtractorBatchPlan(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    requests: []const asset_producer.Request,
) !void {
    try validateExtractorBatchCompatibility(alloc, capabilities, attachment_transport, requests);
    var start: usize = 0;
    while (start < requests.len) {
        const end = try extractorBatchEnd(alloc, capabilities, attachment_transport, requests, start);
        try validateExtractorInvocation(alloc, capabilities, attachment_transport, requests[start..end]);
        start = end;
    }
}

test "asset producer runtime generator admission accepts PDF only for document-capable models" {
    const capabilities = inference_work.InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .document = true },
        .accepted_mime_types = .{ .text_plain = true, .application_pdf = true },
        .input_granularity = .document,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 2, .max_encoded_media_bytes = 16, .max_media_parts_per_item = 1 },
        .output = .generated_text,
    };
    const pdf = [_]u8{ '%', 'P', 'D', 'F' };
    const requests = [_]asset_producer.Request{.{
        .producer_type = .generator,
        .config_json = "{}",
        .source_text = "ocr",
        .media = &.{.{ .bytes = &pdf, .mime_type = "application/pdf" }},
    }};
    try validateGeneratorInvocation(std.testing.allocator, capabilities, .borrowed_binary, &requests);

    var image_only = capabilities;
    image_only.input_modalities = .{ .text = true, .image = true };
    image_only.accepted_mime_types = .{ .text_plain = true, .image_png = true };
    image_only.input_granularity = .page;
    try std.testing.expectError(
        error.UnsupportedInferenceMimeType,
        validateGeneratorInvocation(std.testing.allocator, image_only, .borrowed_binary, &requests),
    );
}

test "asset producer runtime generator admission accounts for resident inline media" {
    const data_uri = "data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD";
    const capabilities = inference_work.InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .image = true },
        .accepted_mime_types = .{ .text_plain = true, .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = data_uri.len + 1, .max_media_parts_per_item = 1 },
        .output = .generated_text,
    };
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .generator, .config_json = "{}", .source_text = "", .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD\"}}]" },
        .{ .producer_type = .generator, .config_json = "{}", .source_text = "", .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD\"}}]" },
    };
    try std.testing.expectEqual(@as(usize, 1), try generatorBatchEnd(std.testing.allocator, capabilities, .base64_payload, &requests, 0));
    try validateGeneratorInvocation(std.testing.allocator, capabilities, .base64_payload, requests[0..1]);
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        validateGeneratorInvocation(std.testing.allocator, capabilities, .base64_payload, &requests),
    );
    var pixel_limited = capabilities;
    pixel_limited.batch.max_encoded_media_bytes = null;
    pixel_limited.batch.max_decoded_pixels = 6;
    try std.testing.expectEqual(@as(usize, 1), try generatorBatchEnd(std.testing.allocator, pixel_limited, .base64_payload, &requests, 0));
    try std.testing.expectError(
        error.InferenceDecodedPixelsExceeded,
        validateGeneratorInvocation(std.testing.allocator, pixel_limited, .base64_payload, &requests),
    );
}

test "asset producer runtime media accounting follows attachment transport" {
    var bytes = [_]u8{0} ** 24;
    @memcpy(bytes[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, bytes[16..20], 2, .big);
    std.mem.writeInt(u32, bytes[20..24], 3, .big);
    const request = asset_producer.Request{
        .producer_type = .generator,
        .config_json = "{}",
        .source_text = "",
        .media = &.{.{ .bytes = &bytes, .mime_type = "image/png" }},
    };
    const capabilities = inference_work.InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .image = true },
        .accepted_mime_types = .{ .text_plain = true, .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 2, .max_encoded_media_bytes = 32, .max_media_parts_per_item = 1 },
        .output = .generated_text,
        .borrowed_attachments = false,
    };
    const encoded = try generatorRequestShape(std.testing.allocator, capabilities, .base64_payload, request, capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(usize, 32), encoded.encoded_media_bytes);

    const borrowed = try generatorRequestShape(std.testing.allocator, capabilities, .borrowed_binary, request, capabilities.batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(usize, 24), borrowed.encoded_media_bytes);

    const extractor_request = asset_producer.Request{
        .producer_type = .extractor,
        .config_json = "{}",
        .source_text = "ocr",
        .inline_media_trusted = true,
        .media = request.media,
    };
    var extractor_capabilities = capabilities;
    extractor_capabilities.task = .extract;
    extractor_capabilities.input_modalities = .{ .image = true };
    extractor_capabilities.accepted_mime_types = .{ .image_png = true };
    extractor_capabilities.output = .extraction;
    extractor_capabilities.result_cardinality = .one_per_item;

    var extractor_borrowed = try extractorRequestShape(std.testing.allocator, extractor_capabilities, .borrowed_binary, extractor_request, extractor_capabilities.batch.max_encoded_media_bytes);
    defer extractor_borrowed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 24), extractor_borrowed.encoded_media_bytes);
    var extractor_encoded = try extractorRequestShape(std.testing.allocator, extractor_capabilities, .base64_payload, extractor_request, extractor_capabilities.batch.max_encoded_media_bytes);
    defer extractor_encoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 32), extractor_encoded.encoded_media_bytes);
}

test "asset producer runtime remote planning uses resolved framed transport" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    for ([_]bool{ false, true }) |framed| {
        inline for (.{ .reader, .generator, .extractor }) |kind| {
            const group = switch (kind) {
                .reader => "readers",
                .generator => "generators",
                .extractor => "extractors",
                else => unreachable,
            };
            const task = switch (kind) {
                .reader => "read",
                .generator => "generate",
                .extractor => "extract",
                else => unreachable,
            };
            const output = switch (kind) {
                .reader => "read_result",
                .generator => "generated_text",
                .extractor => "extraction",
                else => unreachable,
            };
            const descriptor = try std.json.Stringify.valueAlloc(alloc, .{
                .inputs = [_][]const u8{ "text", "image" },
                .inference_capabilities = .{
                    .version = 4,
                    .task = task,
                    .input_modalities = [_][]const u8{ "text", "image" },
                    .accepted_mime_types = [_][]const u8{ "text/plain", "image/png" },
                    .input_granularity = "page",
                    .output = output,
                    .result_cardinality = "one_per_item",
                    .prompt_policy = "explicit",
                    .borrowed_attachments = false,
                    .framed_attachments = framed,
                    .task_limits = .{ .max_text_bytes_per_item = null, .max_input_tokens_per_item = null, .max_output_tokens_per_item = null, .max_candidates_per_request = null, .max_schema_bytes = null },
                    .batch = .{ .mode = "native", .preferred_items = 4, .max_items = 8, .max_encoded_media_bytes = 1048576, .max_decoded_pixels = 16777216, .max_media_parts_per_item = 1, .per_item_failures = true },
                },
            }, .{});
            defer alloc.free(descriptor);
            const catalog = try std.fmt.allocPrint(alloc, "{{\"{s}\":{{\"vision\":{s}}}}}", .{ group, descriptor });
            defer alloc.free(catalog);
            const Assert = struct {
                fn binary(req: httpx.testing_mod.RequestInfo) !void {
                    var envelope = try httpx.attachment_envelope.parseAlloc(std.testing.allocator, req.body, .{});
                    defer envelope.deinit();
                }
                fn json(req: httpx.testing_mod.RequestInfo) !void {
                    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, req.body, .{});
                    defer parsed.deinit();
                }
            };
            var server = try httpx.TestServer.start(alloc, io_impl.io(), &.{
                .{ .method = .GET, .path = "/ai/v1/models", .respond = .{ .body = catalog } },
                .{ .method = .POST, .path = "/generate/batch", .assert_request = if (framed) Assert.binary else Assert.json, .respond = .{ .body =
                \\{"object":"generate.batch","data":[{"custom_id":"0","index":0,"response":{"id":"gen-0","object":"chat.completion","created":1,"model":"vision","choices":[{"index":0,"message":{"role":"assistant","content":"generated"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}}],"summary":{"total":1,"succeeded":1,"failed":0},"execution":{"requested_items":1,"native_batches":0,"native_items":0,"serial_items":1,"fallback_items":0}}
                } },
            });
            defer server.deinit();
            var server_error: ?anyerror = null;
            var server_group = std.Io.Group.init;
            const Serve = struct {
                fn run(s: *httpx.TestServer, failure: *?anyerror) std.Io.Cancelable!void {
                    while (true) s.handleOne() catch |err| {
                        if (err != error.Canceled) failure.* = err;
                        return;
                    };
                }
            };
            try server_group.concurrent(io_impl.io(), Serve.run, .{ &server, &server_error });
            defer server_group.cancel(io_impl.io());
            var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
            defer client.deinit();
            var runtime = Runtime.init(alloc, &client);
            defer runtime.deinit();
            const config = try std.fmt.allocPrint(alloc, "{{\"provider\":\"antfly\",\"model\":\"vision\",\"url\":\"{s}\"}}", .{server.baseUrl()});
            defer alloc.free(config);
            var png = [_]u8{0} ** 24;
            @memcpy(png[0..8], "\x89PNG\r\n\x1a\n");
            std.mem.writeInt(u32, png[16..20], 2, .big);
            std.mem.writeInt(u32, png[20..24], 3, .big);
            const request = asset_producer.Request{ .producer_type = kind, .config_json = config, .source_text = "", .inline_media_trusted = true, .media = &.{.{ .bytes = &png, .mime_type = "image/png" }} };
            // The test HTTP server is explicitly driven; prime discovery on
            // a worker, then prove repeated planner lookups use that snapshot.
            var discovery_error: ?anyerror = null;
            var discovery = std.Io.Group.init;
            const Discover = struct {
                fn run(r: *Runtime, req: asset_producer.Request, failure: *?anyerror) std.Io.Cancelable!void {
                    _ = r.producer().capabilitiesForRequests(r.alloc, &.{req}) catch |err| {
                        failure.* = err;
                        return;
                    };
                }
            };
            try discovery.concurrent(io_impl.io(), Discover.run, .{ &runtime, request, &discovery_error });
            try discovery.await(io_impl.io());
            if (discovery_error) |err| return err;
            const caps = (try runtime.producer().capabilitiesForRequests(alloc, &.{request})).?;
            try std.testing.expectEqual(framed, caps.framed_attachments);
            const plan = try runtime.producer().invocationMemoryForRequests(alloc, &.{request});
            const fallback: inference_work.AttachmentTransport = if (kind == .reader) .data_uri else .base64_payload;
            try std.testing.expectEqual(if (framed) inference_work.AttachmentTransport.framed_binary else fallback, plan.attachment_transport);
            if (kind == .generator) try std.testing.expect(plan.fixed_bytes < client.maxResponseSize());
            if (kind == .generator) {
                var generated: ?[]u8 = null;
                var generation_error: ?anyerror = null;
                var generation_group = std.Io.Group.init;
                const Generate = struct {
                    fn run(r: *Runtime, req: asset_producer.Request, result: *?[]u8, failure: *?anyerror) std.Io.Cancelable!void {
                        result.* = r.producer().produce(r.alloc, req) catch |err| {
                            failure.* = err;
                            return;
                        };
                    }
                };
                try generation_group.concurrent(io_impl.io(), Generate.run, .{ &runtime, request, &generated, &generation_error });
                try generation_group.await(io_impl.io());
                if (generation_error) |err| return err;
                defer alloc.free(generated.?);
                try std.testing.expectEqualStrings("generated", generated.?);
            }
            server_group.cancel(io_impl.io());
            if (server_error) |err| return err;
        }
    }
}

test "asset producer runtime validates the complete base64 representation" {
    try std.testing.expectEqual(@as(usize, 3), try validateStandardBase64("AQID"));
    try std.testing.expectEqual(@as(usize, 1), try validateStandardBase64("YQ=="));
    try std.testing.expectEqual(@as(usize, 2), try validateStandardBase64("YWI="));
    try std.testing.expectError(error.InvalidDataURI, validateStandardBase64("!!!!"));
    try std.testing.expectError(error.InvalidDataURI, validateStandardBase64("YQ=A"));
    try std.testing.expectError(error.InvalidDataURI, validateStandardBase64("YR=="));
    try std.testing.expectError(error.InvalidDataURI, dataUriResidentSize("data:image/png;base64,!!!!"));
    try std.testing.expectError(error.InvalidDataURI, dataUriResidentSize("data:image/png;base64,"));
    try std.testing.expectError(error.InvalidDataURI, inlineBase64ResidentSize(""));
    try std.testing.expectEqual(
        @as(?usize, "data:image/png,%89PNG".len),
        try dataUriResidentSize("data:image/png,%89PNG"),
    );
    try std.testing.expectEqualStrings(
        "image/png;charset=binary",
        dataUriMimeType("data:image/png;charset=binary;base64,AQID").?,
    );
}

test "asset producer runtime reader URI admission measures data payloads before execution" {
    const request = readers.Request{
        .images = &.{"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD"},
        .inline_content_trust = .trusted_internal,
    };
    var capabilities = inference_work.InferenceCapabilities{
        .task = .read,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .native, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = request.images[0].len, .max_decoded_pixels = 6, .max_media_parts_per_item = 1 },
        .output = .read_result,
    };
    const shape = try Runtime.readerUriInvocationShape(std.testing.allocator, capabilities, request);
    try std.testing.expectEqual(request.images[0].len, shape.encoded_media_bytes);
    try std.testing.expectEqual(@as(u64, 6), shape.decoded_pixels);
    capabilities.batch.max_encoded_media_bytes = 2;
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        Runtime.readerUriInvocationShape(std.testing.allocator, capabilities, request),
    );
    capabilities.batch.max_encoded_media_bytes = request.images[0].len;
    try std.testing.expectError(
        error.InvalidDataURI,
        Runtime.readerUriInvocationShape(std.testing.allocator, capabilities, .{
            .images = &.{"data:image/png;base64,"},
            .inline_content_trust = .trusted_internal,
        }),
    );
}

fn generatorBatchEnd(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    requests: []const asset_producer.Request,
    start: usize,
) !usize {
    const item_end = @min(start +| capabilities.batch.max_items, requests.len);
    if (capabilities.batch.max_encoded_media_bytes == null and capabilities.batch.max_decoded_pixels == null)
        return item_end;
    var end = start;
    var bytes: usize = 0;
    var pixels: u64 = 0;
    while (end < item_end) : (end += 1) {
        const remaining_bytes = if (capabilities.batch.max_encoded_media_bytes) |limit|
            limit -| bytes
        else
            null;
        const item = generatorRequestShape(alloc, capabilities, attachment_transport, requests[end], remaining_bytes) catch |err| {
            if (err == error.InferenceEncodedBytesExceeded and end > start) break;
            return err;
        };
        const next = std.math.add(usize, bytes, item.encoded_media_bytes) catch break;
        const next_pixels = std.math.add(u64, pixels, item.decoded_pixels) catch break;
        if (capabilities.batch.max_encoded_media_bytes) |limit| {
            if (next > limit) {
                if (end == start) return error.InferenceEncodedBytesExceeded;
                break;
            }
        }
        if (capabilities.batch.max_decoded_pixels) |limit| {
            if (next_pixels > limit) {
                if (end == start) return error.InferenceDecodedPixelsExceeded;
                break;
            }
        }
        bytes = next;
        pixels = next_pixels;
    }
    return @max(start + 1, end);
}

fn extractorBatchEnd(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    requests: []const asset_producer.Request,
    start: usize,
) !usize {
    const item_end = @min(start +| capabilities.batch.max_items, requests.len);
    if (capabilities.batch.max_encoded_media_bytes == null and capabilities.batch.max_decoded_pixels == null)
        return item_end;
    var end = start;
    var bytes: usize = 0;
    var pixels: u64 = 0;
    while (end < item_end) : (end += 1) {
        const remaining_bytes = if (capabilities.batch.max_encoded_media_bytes) |limit|
            limit -| bytes
        else
            null;
        var item = extractorRequestShape(alloc, capabilities, attachment_transport, requests[end], remaining_bytes) catch |err| {
            if (err == error.InferenceEncodedBytesExceeded and end > start) break;
            return err;
        };
        defer item.deinit(alloc);
        const next = std.math.add(usize, bytes, item.encoded_media_bytes) catch return error.InferenceEncodedBytesExceeded;
        const next_pixels = std.math.add(u64, pixels, item.decoded_pixels) catch
            return error.InferenceDecodedPixelsExceeded;
        if (capabilities.batch.max_encoded_media_bytes) |limit| {
            if (next > limit) {
                if (end == start) return error.InferenceEncodedBytesExceeded;
                break;
            }
        }
        if (capabilities.batch.max_decoded_pixels) |limit| {
            if (next_pixels > limit) {
                if (end == start) return error.InferenceDecodedPixelsExceeded;
                break;
            }
        }
        bytes = next;
        pixels = next_pixels;
    }
    return @max(start + 1, end);
}

test "asset producer runtime extractor windows obey resolved item and encoded-byte ceilings" {
    var bytes = [_]u8{0} ** 24;
    @memcpy(bytes[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, bytes[16..20], 2, .big);
    std.mem.writeInt(u32, bytes[20..24], 3, .big);
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "ocr", .inline_media_trusted = true, .media = &.{.{ .bytes = &bytes, .mime_type = "image/png" }} },
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "ocr", .inline_media_trusted = true, .media = &.{.{ .bytes = &bytes, .mime_type = "image/png" }} },
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "ocr", .inline_media_trusted = true, .media = &.{.{ .bytes = &bytes, .mime_type = "image/png" }} },
    };
    const capabilities = inference_work.InferenceCapabilities{
        .task = .extract,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = 63, .max_media_parts_per_item = 1 },
        .output = .extraction,
        .prompt_policy = .structured_schema,
    };
    try std.testing.expectEqual(@as(usize, 1), try extractorBatchEnd(std.testing.allocator, capabilities, .base64_payload, &requests, 0));
    try validateExtractorInvocation(std.testing.allocator, capabilities, .base64_payload, requests[0..1]);
    try std.testing.expectError(error.InferenceEncodedBytesExceeded, validateExtractorInvocation(std.testing.allocator, capabilities, .base64_payload, requests[0..2]));
    try validateExtractorBatchPlan(std.testing.allocator, capabilities, .base64_payload, &requests);

    var different_prompts = requests;
    different_prompts[1].source_text = "different prompt";
    try std.testing.expectError(error.BatchIncompatible, validateExtractorBatchPlan(std.testing.allocator, capabilities, .base64_payload, &different_prompts));
}

test "asset producer runtime extractor admission accounts for resident inline source parts" {
    const first_uri = "data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD";
    const second_uri = "data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD";
    const capabilities = inference_work.InferenceCapabilities{
        .task = .extract,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = first_uri.len + 1, .max_media_parts_per_item = 1 },
        .output = .extraction,
        .prompt_policy = .structured_schema,
    };
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "", .content_type = "application/json", .inline_media_trusted = true, .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD\"}]" },
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "", .content_type = "application/json", .inline_media_trusted = true, .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD\"}]" },
    };
    try std.testing.expectEqual(first_uri.len, second_uri.len);
    try std.testing.expectEqual(@as(usize, 1), try extractorBatchEnd(std.testing.allocator, capabilities, .base64_payload, &requests, 0));
    try validateExtractorBatchPlan(std.testing.allocator, capabilities, .base64_payload, &requests);

    var untrusted = requests[0];
    untrusted.inline_media_trusted = false;
    try std.testing.expectError(
        error.UntrustedInlineMedia,
        validateExtractorInvocation(std.testing.allocator, capabilities, .base64_payload, &.{untrusted}),
    );

    var text_capabilities = capabilities;
    text_capabilities.input_modalities = .{ .text = true };
    text_capabilities.accepted_mime_types = .{ .text_plain = true };
    text_capabilities.input_granularity = .item;
    try std.testing.expectError(
        error.UnsupportedInferenceMimeType,
        validateExtractorInvocation(std.testing.allocator, text_capabilities, .base64_payload, requests[0..1]),
    );

    var plain = requests[0];
    plain.content_type = "text/plain";
    try std.testing.expectError(
        error.BatchIncompatible,
        validateExtractorBatchCompatibility(std.testing.allocator, capabilities, .base64_payload, &.{plain}),
    );
}

test "asset producer runtime extractor shape is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            const capabilities = inference_work.InferenceCapabilities{
                .task = .extract,
                .input_modalities = .{ .image = true },
                .accepted_mime_types = .{ .image_png = true },
                .input_granularity = .page,
                .batch = .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 1, .max_encoded_media_bytes = 256, .max_media_parts_per_item = 1 },
                .output = .extraction,
                .prompt_policy = .structured_schema,
            };
            var shape = try extractorRequestShape(alloc, capabilities, .base64_payload, .{
                .producer_type = .extractor,
                .config_json = "{}",
                .source_text = "",
                .inline_media_trusted = true,
                .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD\"}]",
            }, capabilities.batch.max_encoded_media_bytes);
            defer shape.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "asset producer runtime local reader chunks stop at source boundaries before the Florence cap" {
    const fingerprints = [_]?[]const u8{ "pdf-a", "pdf-a", "pdf-b", "pdf-b" };
    try std.testing.expectEqual(@as(usize, 2), readerBatchEnd(&fingerprints, 0, 8));
    try std.testing.expectEqual(@as(usize, 4), readerBatchEnd(&fingerprints, 2, 8));
    try std.testing.expectEqual(@as(usize, 1), readerBatchEnd(&fingerprints, 0, 1));
}

test "encoded reader chunks obey model item and byte limits" {
    var bytes = [_]u8{0} ** 24;
    @memcpy(bytes[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, bytes[16..20], 2, .big);
    std.mem.writeInt(u32, bytes[20..24], 3, .big);
    const images = [_]readers.EncodedImage{
        .{ .bytes = &bytes, .mime_type = "image/png" },
        .{ .bytes = &bytes, .mime_type = "image/png" },
        .{ .bytes = &bytes, .mime_type = "image/png" },
    };
    try std.testing.expectEqual(@as(usize, 1), try encodedReaderBatchEnd(&images, 0, .{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 2,
        .max_encoded_media_bytes = 47,
    }, .borrowed_binary));
    try std.testing.expectEqual(@as(usize, 2), try encodedReaderBatchEnd(&images, 0, .{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 2,
        .max_encoded_media_bytes = 48,
    }, .borrowed_binary));
    const data_uri_bytes = try inference_work.AttachmentTransport.data_uri.wireSize(
        bytes.len,
        "image/png".len,
    );
    try std.testing.expectEqual(@as(usize, 1), try encodedReaderBatchEnd(&images, 0, .{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 2,
        .max_encoded_media_bytes = data_uri_bytes * 2 - 1,
    }, .data_uri));
}

/// Preserve an exact source label for same-document batches. Mixed-document
/// batches receive a stable aggregate label so profiling cannot accidentally
/// attribute the entire model invocation to the first request.
fn readerBatchSourceFingerprint(
    fingerprints: []const ?[]const u8,
    mixed_buffer: *[32]u8,
) ?[]const u8 {
    if (fingerprints.len == 0) return null;
    const first = fingerprints[0];
    var all_equal = true;
    for (fingerprints[1..]) |fingerprint| {
        if (!optionalStringsEqual(first, fingerprint)) {
            all_equal = false;
            break;
        }
    }
    if (all_equal) return first;

    var hasher = std.hash.Wyhash.init(0x616e_7466_6c79_6f63);
    var length_bytes: [8]u8 = undefined;
    for (fingerprints) |maybe_fingerprint| {
        if (maybe_fingerprint) |fingerprint| {
            hasher.update(&.{1});
            std.mem.writeInt(u64, &length_bytes, @intCast(fingerprint.len), .big);
            hasher.update(&length_bytes);
            hasher.update(fingerprint);
        } else {
            hasher.update(&.{0});
        }
    }
    return std.fmt.bufPrint(mixed_buffer, "mixed-{x}", .{hasher.final()}) catch unreachable;
}

test "asset producer runtime preserves or aggregates reader batch source fingerprints" {
    const same = [_]?[]const u8{ "same", "same" };
    var same_buffer: [32]u8 = undefined;
    try std.testing.expectEqualStrings("same", readerBatchSourceFingerprint(&same, &same_buffer).?);

    const mixed = [_]?[]const u8{ "first", "second" };
    var first_buffer: [32]u8 = undefined;
    var second_buffer: [32]u8 = undefined;
    const first = readerBatchSourceFingerprint(&mixed, &first_buffer).?;
    const second = readerBatchSourceFingerprint(&mixed, &second_buffer).?;
    try std.testing.expect(std.mem.startsWith(u8, first, "mixed-"));
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expect(!std.mem.eql(u8, first, "first"));
}

const ReaderSource = struct {
    images: []const []const u8,
    prompt: ?[]const u8 = null,

    fn deinit(self: *ReaderSource, alloc: Allocator) void {
        for (self.images) |image| alloc.free(@constCast(image));
        alloc.free(self.images);
        if (self.prompt) |prompt| alloc.free(@constCast(prompt));
        self.* = undefined;
    }
};

fn parseReaderSource(alloc: Allocator, source_text: []const u8, source_parts_json: ?[]const u8) !ReaderSource {
    return try parseReaderSourceWithMetadataOnly(alloc, source_text, source_parts_json, false);
}

fn parseReaderSourceWithMetadataOnly(
    alloc: Allocator,
    source_text: []const u8,
    source_parts_json: ?[]const u8,
    allow_metadata_only: bool,
) !ReaderSource {
    if (source_parts_json) |raw_parts| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
        defer parsed.deinit();
        if (parsed.value == .array) {
            var images = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (images.items) |image| alloc.free(@constCast(image));
                images.deinit(alloc);
            }
            var prompt = std.ArrayListUnmanaged(u8).empty;
            errdefer prompt.deinit(alloc);

            for (parsed.value.array.items) |part| {
                if (part != .object) continue;
                const type_value = part.object.get("type") orelse continue;
                if (type_value != .string) continue;
                if (std.mem.eql(u8, type_value.string, "text")) {
                    const text = part.object.get("text") orelse continue;
                    if (text != .string) continue;
                    if (prompt.items.len > 0) try prompt.append(alloc, '\n');
                    try prompt.appendSlice(alloc, text.string);
                } else if (std.mem.eql(u8, type_value.string, "media")) {
                    if (part.object.get("url")) |url| {
                        if (url == .string) try images.append(alloc, try alloc.dupe(u8, url.string));
                    } else if (part.object.get("mime_type")) |mime| {
                        const data = part.object.get("data") orelse continue;
                        if (mime == .string and data == .string) {
                            try images.append(alloc, try std.fmt.allocPrint(alloc, "data:{s};base64,{s}", .{ mime.string, data.string }));
                        }
                    }
                }
            }

            if (images.items.len > 0 or allow_metadata_only) {
                const owned_images = try images.toOwnedSlice(alloc);
                errdefer {
                    for (owned_images) |image| alloc.free(@constCast(image));
                    alloc.free(owned_images);
                }
                return .{
                    .images = owned_images,
                    .prompt = if (prompt.items.len > 0) try prompt.toOwnedSlice(alloc) else null,
                };
            }
            // Preserve the established source_text fallback for ordinary URL
            // readers. Reset both lists after deinit so their errdefers remain
            // harmless if parsing source_text subsequently fails.
            prompt.deinit(alloc);
            prompt = .empty;
            images.deinit(alloc);
            images = .empty;
        }
    }

    return try parseReaderSourceText(alloc, source_text);
}

fn parseReaderSourceText(alloc: Allocator, source_text: []const u8) !ReaderSource {
    const trimmed = std.mem.trim(u8, source_text, &std.ascii.whitespace);
    if (trimmed.len > 0 and (trimmed[0] == '[' or trimmed[0] == '"')) {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, trimmed, .{}) catch |err| switch (err) {
            std.mem.Allocator.Error.OutOfMemory => return err,
            else => null,
        };
        if (parsed) |*value| {
            defer value.deinit();
            switch (value.value) {
                .string => |url| return try singleReaderImage(alloc, url),
                .array => |array| {
                    var images = std.ArrayListUnmanaged([]const u8).empty;
                    errdefer {
                        for (images.items) |image| alloc.free(@constCast(image));
                        images.deinit(alloc);
                    }
                    for (array.items) |item| {
                        if (item == .string) try images.append(alloc, try alloc.dupe(u8, item.string));
                    }
                    return .{ .images = try images.toOwnedSlice(alloc) };
                },
                else => {},
            }
        }
    }
    return try singleReaderImage(alloc, source_text);
}

fn singleReaderImage(alloc: Allocator, url: []const u8) !ReaderSource {
    const images = try alloc.alloc([]const u8, 1);
    errdefer alloc.free(images);
    images[0] = try alloc.dupe(u8, url);
    return .{ .images = images };
}

fn encodeReaderResults(alloc: Allocator, content_type: []const u8, results: []readers.Result) ![]u8 {
    if (isJsonContentType(content_type)) {
        return try std.json.Stringify.valueAlloc(alloc, results, .{});
    }

    // The document executor emits one page per outer request. Transfer that
    // reader-owned text directly into the common result envelope instead of
    // serializing/copying it only for enrichment to parse it back as text.
    if (results.len == 1) {
        const text = @constCast(results[0].text);
        results[0].text = &.{};
        return text;
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (results, 0..) |result, i| {
        if (i > 0) try out.append(alloc, '\n');
        try out.appendSlice(alloc, result.text);
    }
    return try out.toOwnedSlice(alloc);
}

test "plain singleton reader output transfers its text buffer" {
    const allocator = std.testing.allocator;
    const text = try allocator.dupe(u8, "page text");
    var results = [_]readers.Result{.{ .text = text }};
    defer readers.deinitResult(allocator, &results[0]);
    const output = try encodeReaderResults(allocator, "text/plain", &results);
    defer allocator.free(output);
    try std.testing.expectEqual(@intFromPtr(text.ptr), @intFromPtr(output.ptr));
    try std.testing.expectEqualStrings("page text", output);
    try std.testing.expectEqual(@as(usize, 0), results[0].text.len);
}

fn isJsonContentType(content_type: []const u8) bool {
    return std.mem.eql(u8, content_type, "application/json") or
        std.mem.endsWith(u8, content_type, "+json");
}

fn extractionContentJsonAlloc(alloc: Allocator, source_text: []const u8, source_parts_json: ?[]const u8) ![]u8 {
    if (source_parts_json) |raw_parts| {
        if (raw_parts.len > 0) return try alloc.dupe(u8, raw_parts);
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(source_text, .{})});
}

fn extractionResultsJsonAlloc(
    alloc: Allocator,
    response_json: []const u8,
    expected_model: []const u8,
    wire_ids: []const []const u8,
    output_ids: []const []const u8,
) ![][]u8 {
    if (wire_ids.len != output_ids.len) return error.InvalidWorkIdentity;
    var expected_by_id = std.StringHashMapUnmanaged(usize).empty;
    defer expected_by_id.deinit(alloc);
    for (wire_ids, 0..) |id, index| {
        if (id.len == 0) return error.InvalidWorkIdentity;
        const entry = try expected_by_id.getOrPut(alloc, id);
        if (entry.found_existing) return error.InvalidWorkIdentity;
        entry.value_ptr.* = index;
    }

    var raw = std.json.parseFromSlice(std.json.Value, alloc, response_json, .{
        .duplicate_field_behavior = .@"error",
    }) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidExtractorResponse,
    };
    defer raw.deinit();
    var typed = std.json.parseFromValue(extraction_api.ExtractionResponse, alloc, raw.value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
        .duplicate_field_behavior = .@"error",
    }) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidExtractorResponse,
    };
    defer typed.deinit();
    if (!std.mem.eql(u8, typed.value.object, "extraction") or
        !std.mem.eql(u8, typed.value.model, expected_model))
        return error.InvalidExtractorResponse;
    if (typed.value.data.len != wire_ids.len) return error.InvalidExtractorResponse;
    if (raw.value != .object) return error.InvalidExtractorResponse;
    const raw_data = raw.value.object.get("data") orelse return error.InvalidExtractorResponse;
    if (raw_data != .array or raw_data.array.items.len != wire_ids.len)
        return error.InvalidExtractorResponse;

    const out = try alloc.alloc([]u8, wire_ids.len);
    for (out) |*item| item.* = &.{};
    errdefer {
        for (out) |item| if (item.len > 0) alloc.free(item);
        alloc.free(out);
    }
    const seen = try alloc.alloc(bool, wire_ids.len);
    defer alloc.free(seen);
    @memset(seen, false);
    for (raw_data.array.items) |*item| {
        if (item.* != .object) return error.InvalidExtractorResponse;
        const id_value = item.object.get("id") orelse return error.InvalidExtractorResponse;
        if (id_value != .string) return error.InvalidExtractorResponse;
        const index = expected_by_id.get(id_value.string) orelse return error.InvalidExtractorResponse;
        if (seen[index]) return error.InvalidExtractorResponse;
        if (output_ids[index].len > 0) {
            try item.object.put(alloc, "id", .{ .string = output_ids[index] });
        } else {
            _ = item.object.orderedRemove("id");
        }
        out[index] = try std.json.Stringify.valueAlloc(alloc, item.*, .{});
        seen[index] = true;
    }
    for (seen) |was_seen| if (!was_seen) return error.InvalidExtractorResponse;
    return out;
}

test "asset producer runtime extractor batch response preserves extensions and maps identity" {
    const alloc = std.testing.allocator;
    const ids = [_][]const u8{ "page-a", "page-b" };
    const exact = try extractionResultsJsonAlloc(
        alloc,
        "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-b\",\"provider_extension\":{\"rank\":2}},{\"id\":\"page-a\",\"provider_extension\":{\"rank\":1}}]}",
        "owner/model",
        &ids,
        &ids,
    );
    defer {
        for (exact) |item| alloc.free(item);
        alloc.free(exact);
    }
    try std.testing.expectEqualStrings("{\"id\":\"page-a\",\"provider_extension\":{\"rank\":1}}", exact[0]);
    try std.testing.expectEqualStrings("{\"id\":\"page-b\",\"provider_extension\":{\"rank\":2}}", exact[1]);

    const wire_ids = [_][]const u8{ "wire-a", "wire-b" };
    const repeated_output_ids = [_][]const u8{ "page:000001", "page:000001" };
    const repeated = try extractionResultsJsonAlloc(
        alloc,
        "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"wire-b\",\"rank\":2},{\"id\":\"wire-a\",\"rank\":1}]}",
        "owner/model",
        &wire_ids,
        &repeated_output_ids,
    );
    defer {
        for (repeated) |item| alloc.free(item);
        alloc.free(repeated);
    }
    try std.testing.expectEqualStrings("{\"id\":\"page:000001\",\"rank\":1}", repeated[0]);
    try std.testing.expectEqualStrings("{\"id\":\"page:000001\",\"rank\":2}", repeated[1]);

    const no_output_ids = [_][]const u8{ "", "" };
    const anonymous = try extractionResultsJsonAlloc(
        alloc,
        "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"wire-a\"},{\"id\":\"wire-b\"}]}",
        "owner/model",
        &wire_ids,
        &no_output_ids,
    );
    defer {
        for (anonymous) |item| alloc.free(item);
        alloc.free(anonymous);
    }
    try std.testing.expectEqualStrings("{}", anonymous[0]);
    try std.testing.expectEqualStrings("{}", anonymous[1]);

    const duplicate_ids = [_][]const u8{ "page-a", "page-a" };
    try std.testing.expectError(
        error.InvalidWorkIdentity,
        extractionResultsJsonAlloc(alloc, "{}", "owner/model", &duplicate_ids, &ids),
    );
    try std.testing.expectError(
        error.InvalidExtractorResponse,
        extractionResultsJsonAlloc(alloc, "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-a\"}]}", "owner/model", &ids, &ids),
    );
    try std.testing.expectError(
        error.InvalidExtractorResponse,
        extractionResultsJsonAlloc(alloc, "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-a\"},{\"id\":\"page-a\"}]}", "owner/model", &ids, &ids),
    );
    try std.testing.expectError(
        error.InvalidExtractorResponse,
        extractionResultsJsonAlloc(alloc, "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-a\"},{\"id\":\"unknown\"}]}", "owner/model", &ids, &ids),
    );
    try std.testing.expectError(
        error.InvalidExtractorResponse,
        extractionResultsJsonAlloc(alloc, "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-a\"},{}]}", "owner/model", &ids, &ids),
    );
    try std.testing.expectError(
        error.InvalidExtractorResponse,
        extractionResultsJsonAlloc(alloc, "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-a\",\"entities\":[1]},{\"id\":\"page-b\"}]}", "owner/model", &ids, &ids),
    );
    try std.testing.expectError(
        error.InvalidExtractorResponse,
        extractionResultsJsonAlloc(alloc, "{\"object\":\"extraction\",\"model\":\"other/model\",\"data\":[{\"id\":\"page-a\"},{\"id\":\"page-b\"}]}", "owner/model", &ids, &ids),
    );
}

test "asset producer runtime typed extractor response parsing is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            const ids = [_][]const u8{ "page-a", "page-b" };
            const results = try extractionResultsJsonAlloc(
                alloc,
                "{\"object\":\"extraction\",\"model\":\"owner/model\",\"data\":[{\"id\":\"page-b\"},{\"id\":\"page-a\"}]}",
                "owner/model",
                &ids,
                &ids,
            );
            defer {
                for (results) |item| alloc.free(item);
                alloc.free(results);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

fn antflyGenerateBatchUrlAlloc(alloc: Allocator, base_url: []const u8) ![]u8 {
    const trimmed = trimRightSlash(base_url);
    if (trimmed.len == 0) return error.InvalidGeneratorConfig;
    return try std.fmt.allocPrint(alloc, "{s}/generate/batch", .{trimmed});
}

fn trimRightSlash(value: []const u8) []const u8 {
    var end = value.len;
    while (end > 0 and value[end - 1] == '/') : (end -= 1) {}
    return value[0..end];
}

const AntflyGenerateBatchRequest = struct {
    metadata_or_json: []u8,
    envelope: ?httpx.attachment_envelope.EncodedSegments = null,

    fn deinit(self: *AntflyGenerateBatchRequest, alloc: Allocator) void {
        if (self.envelope) |*envelope| envelope.deinit();
        alloc.free(self.metadata_or_json);
        self.* = undefined;
    }
};

fn antflyGenerateBatchRequestAlloc(
    alloc: Allocator,
    cfg: generating_runtime.GeneratorConfig,
    requests: []const asset_producer.Request,
    attachment_transport: inference_work.AttachmentTransport,
) !AntflyGenerateBatchRequest {
    const ContentMetadata = struct {
        elements_size: usize,
        element_count: usize,
    };
    const Helper = struct {
        fn metadata(allocator: Allocator, request: asset_producer.Request) !ContentMetadata {
            var count: usize = 0;
            var size: usize = 0;
            if (request.source_parts_json) |raw_parts| {
                var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_parts, .{});
                defer parsed.deinit();
                if (parsed.value != .array) return error.InvalidGeneratorContentParts;
                for (parsed.value.array.items) |part| {
                    if (part != .object) continue;
                    const type_value = part.object.get("type") orelse continue;
                    if (type_value != .string or std.mem.eql(u8, type_value.string, "metadata")) continue;
                    const encoded = try std.json.Stringify.valueAlloc(allocator, part, .{});
                    defer allocator.free(encoded);
                    if (count > 0) try addSize(&size, 1);
                    try addSize(&size, encoded.len);
                    count += 1;
                }
            }
            if (count == 0 and request.media.len == 0 and request.source_parts_json != null and request.source_text.len > 0) {
                try addSize(&size, "{\"type\":\"text\",\"text\":".len);
                try addSize(&size, try jsonStringSize(request.source_text));
                try addSize(&size, 1);
                count = 1;
            }
            return .{ .elements_size = size, .element_count = count };
        }

        fn writeElements(
            allocator: Allocator,
            writer: *std.Io.Writer,
            request: asset_producer.Request,
        ) !usize {
            var count: usize = 0;
            if (request.source_parts_json) |raw_parts| {
                var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_parts, .{});
                defer parsed.deinit();
                if (parsed.value != .array) return error.InvalidGeneratorContentParts;
                for (parsed.value.array.items) |part| {
                    if (part != .object) continue;
                    const type_value = part.object.get("type") orelse continue;
                    if (type_value != .string or std.mem.eql(u8, type_value.string, "metadata")) continue;
                    if (count > 0) try writer.writeByte(',');
                    var stringify: std.json.Stringify = .{ .writer = writer };
                    try stringify.write(part);
                    count += 1;
                }
            }
            if (count == 0 and request.media.len == 0 and request.source_parts_json != null and request.source_text.len > 0) {
                try writer.writeAll("{\"type\":\"text\",\"text\":");
                try writeJsonString(writer, request.source_text);
                try writer.writeByte('}');
                count = 1;
            }
            return count;
        }

        fn jsonStringSize(value: []const u8) !usize {
            if (!std.unicode.utf8ValidateSlice(value)) return error.InvalidGeneratorContentParts;
            var size: usize = 2;
            for (value) |byte| {
                const encoded: usize = switch (byte) {
                    '\\', '"', 0x08, 0x0c, '\n', '\r', '\t' => 2,
                    0x00...0x07, 0x0b, 0x0e...0x1f => 6,
                    else => 1,
                };
                size = std.math.add(usize, size, encoded) catch return error.OutOfMemory;
            }
            return size;
        }

        fn addSize(total: *usize, amount: usize) !void {
            total.* = std.math.add(usize, total.*, amount) catch return error.OutOfMemory;
        }

        fn writeJsonString(writer: *std.Io.Writer, value: []const u8) !void {
            const hex = "0123456789abcdef";
            try writer.writeByte('"');
            for (value) |byte| switch (byte) {
                '\\' => try writer.writeAll("\\\\"),
                '"' => try writer.writeAll("\\\""),
                0x08 => try writer.writeAll("\\b"),
                0x0c => try writer.writeAll("\\f"),
                '\n' => try writer.writeAll("\\n"),
                '\r' => try writer.writeAll("\\r"),
                '\t' => try writer.writeAll("\\t"),
                0x00...0x07, 0x0b, 0x0e...0x1f => {
                    try writer.writeAll("\\u00");
                    try writer.writeByte(hex[byte >> 4]);
                    try writer.writeByte(hex[byte & 0x0f]);
                },
                else => try writer.writeByte(byte),
            };
            try writer.writeByte('"');
        }

        fn writeBase64String(writer: *std.Io.Writer, bytes: []const u8) !void {
            try writer.writeByte('"');
            const encoded_len = std.base64.standard.Encoder.calcSize(bytes.len);
            const encoded = try writer.writableSlice(encoded_len);
            _ = std.base64.standard.Encoder.encode(encoded, bytes);
            try writer.writeByte('"');
        }
    };

    const model_json = try std.json.Stringify.valueAlloc(alloc, cfg.model, .{});
    defer alloc.free(model_json);
    var options = std.ArrayListUnmanaged(u8).empty;
    defer options.deinit(alloc);
    try options.appendSlice(alloc, ",\"mode\":\"eager\"");
    try appendBatchI64Field(alloc, &options, "max_tokens", cfg.max_tokens);
    if (cfg.temperature) |temperature| try appendBatchFloatField(alloc, &options, "temperature", temperature);
    if (cfg.top_p) |top_p| try appendBatchFloatField(alloc, &options, "top_p", top_p);
    if (cfg.top_k) |top_k| try appendBatchI64Field(alloc, &options, "top_k", top_k);
    if (cfg.frequency_penalty) |frequency_penalty| try appendBatchFloatField(alloc, &options, "frequency_penalty", frequency_penalty);
    if (cfg.presence_penalty) |presence_penalty| try appendBatchFloatField(alloc, &options, "presence_penalty", presence_penalty);

    const outer_prefix = "{\"mode\":\"sync\",\"requests\":[";
    const item_prefix = "{\"custom_id\":\"";
    const body_prefix = "\",\"body\":{\"model\":";
    const messages_prefix = ",\"messages\":[{\"role\":\"user\",\"content\":";
    const item_suffix = "}]";
    const framed = attachment_transport == .framed_binary;
    var attachments = std.ArrayListUnmanaged(httpx.attachment_envelope.Attachment).empty;
    defer attachments.deinit(alloc);
    if (framed) for (requests) |request| for (request.media) |media| {
        try attachments.append(alloc, .{ .mime_type = media.mime_type, .data = media.bytes });
    };
    var exact_size: usize = outer_prefix.len + "]}".len;
    var attachment_cursor: usize = 0;
    for (requests, 0..) |request, i| {
        const item_metadata = try Helper.metadata(alloc, request);
        if (i > 0) try Helper.addSize(&exact_size, 1);
        try Helper.addSize(&exact_size, item_prefix.len + std.fmt.count("{d}", .{i}) + body_prefix.len);
        try Helper.addSize(&exact_size, model_json.len + messages_prefix.len);
        if (request.source_parts_json == null and request.media.len == 0) {
            try Helper.addSize(&exact_size, try Helper.jsonStringSize(request.source_text));
        } else {
            try Helper.addSize(&exact_size, 2 + item_metadata.elements_size);
            var emitted = item_metadata.element_count;
            for (request.media) |media| {
                if (emitted > 0) try Helper.addSize(&exact_size, 1);
                try Helper.addSize(&exact_size, "{\"type\":\"media\",\"mime_type\":".len);
                try Helper.addSize(&exact_size, try Helper.jsonStringSize(media.mime_type));
                if (framed) {
                    try Helper.addSize(&exact_size, ",\"data\":\"attachment:".len + std.fmt.count("{d}", .{attachment_cursor}) + "\"}".len);
                    attachment_cursor += 1;
                } else {
                    try Helper.addSize(&exact_size, ",\"data\":\"".len + "\"}".len);
                    try Helper.addSize(&exact_size, std.base64.standard.Encoder.calcSize(media.bytes.len));
                }
                emitted += 1;
            }
        }
        try Helper.addSize(&exact_size, item_suffix.len + options.items.len + "}}".len);
    }

    var output: std.Io.Writer.Allocating = try .initCapacity(alloc, exact_size);
    defer output.deinit();
    try output.writer.writeAll(outer_prefix);
    attachment_cursor = 0;
    for (requests, 0..) |request, i| {
        if (i > 0) try output.writer.writeByte(',');
        try output.writer.writeAll(item_prefix);
        try output.writer.print("{d}", .{i});
        try output.writer.writeAll(body_prefix);
        try output.writer.writeAll(model_json);
        try output.writer.writeAll(messages_prefix);
        if (request.source_parts_json == null and request.media.len == 0) {
            try Helper.writeJsonString(&output.writer, request.source_text);
        } else {
            try output.writer.writeByte('[');
            const element_count = try Helper.writeElements(alloc, &output.writer, request);
            for (request.media, 0..) |media, media_index| {
                if (element_count > 0 or media_index > 0) try output.writer.writeByte(',');
                try output.writer.writeAll("{\"type\":\"media\",\"mime_type\":");
                try Helper.writeJsonString(&output.writer, media.mime_type);
                try output.writer.writeAll(",\"data\":");
                if (framed) {
                    try output.writer.print("\"attachment:{d}\"", .{attachment_cursor});
                    attachment_cursor += 1;
                } else {
                    try Helper.writeBase64String(&output.writer, media.bytes);
                }
                try output.writer.writeByte('}');
            }
            try output.writer.writeByte(']');
        }
        try output.writer.writeAll(item_suffix);
        try output.writer.writeAll(options.items);
        try output.writer.writeAll("}}");
    }
    try output.writer.writeAll("]}");
    if (output.writer.end != exact_size) return error.InvalidGeneratorRequestSize;
    const body = output.writer.buffer;
    output.writer.buffer = &.{};
    output.writer.end = 0;
    if (!framed) return .{ .metadata_or_json = body };
    errdefer alloc.free(body);
    return .{
        .metadata_or_json = body,
        .envelope = try httpx.attachment_envelope.encodeSegmentsAlloc(alloc, body, attachments.items),
    };
}

fn appendBatchI64Field(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: i64) !void {
    const fragment = try std.fmt.allocPrint(alloc, ",\"{s}\":{d}", .{ name, value });
    defer alloc.free(fragment);
    try out.appendSlice(alloc, fragment);
}

fn appendBatchFloatField(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), name: []const u8, value: f32) !void {
    const fragment = try std.fmt.allocPrint(alloc, ",\"{s}\":{f}", .{ name, std.json.fmt(value, .{}) });
    defer alloc.free(fragment);
    try out.appendSlice(alloc, fragment);
}

test "remote generator batch streams attachments into one exact JSON body" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            const media = [_]asset_producer.EncodedMedia{.{
                .bytes = &.{ 1, 2, 3 },
                .mime_type = "image/png",
            }};
            const requests = [_]asset_producer.Request{
                .{
                    .producer_type = .generator,
                    .config_json = "{}",
                    .source_text = "",
                    .source_parts_json = "[{\"type\":\"metadata\",\"page\":1},{\"type\":\"text\",\"text\":\"inspect\"}]",
                    .inline_media_trusted = true,
                    .media = &media,
                },
                .{
                    .producer_type = .generator,
                    .config_json = "{}",
                    .source_text = "line\nquoted \"text\"",
                },
            };
            var body = try antflyGenerateBatchRequestAlloc(alloc, .{
                .provider = .antfly,
                .model = "gemma\"4",
                .url = "http://inference.invalid",
                .max_tokens = 32,
                .temperature = 0.25,
            }, &requests, .base64_payload);
            defer body.deinit(alloc);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body.metadata_or_json, .{});
            defer parsed.deinit();
            const batch = parsed.value.object.get("requests").?.array.items;
            try std.testing.expectEqual(@as(usize, 2), batch.len);
            const content = batch[0].object.get("body").?.object.get("messages").?.array.items[0].object.get("content").?.array.items;
            try std.testing.expectEqual(@as(usize, 2), content.len);
            try std.testing.expectEqualStrings("AQID", content[1].object.get("data").?.string);

            var framed_body = try antflyGenerateBatchRequestAlloc(alloc, .{
                .provider = .antfly,
                .model = "gemma4",
                .url = "http://inference.invalid",
                .max_tokens = 32,
            }, &requests, .framed_binary);
            defer framed_body.deinit(alloc);
            const envelope = framed_body.envelope orelse return error.MissingAttachmentEnvelope;
            try std.testing.expectEqual(@as(usize, 4), envelope.segments.len);
            try std.testing.expectEqualStrings("image/png", envelope.segments[2]);
            try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, envelope.segments[3]);
            var framed_metadata = try std.json.parseFromSlice(std.json.Value, alloc, framed_body.metadata_or_json, .{});
            defer framed_metadata.deinit();
            const framed_content = framed_metadata.value.object.get("requests").?.array.items[0].object.get("body").?.object.get("messages").?.array.items[0].object.get("content").?.array.items;
            try std.testing.expectEqualStrings("attachment:0", framed_content[1].object.get("data").?.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

fn normalizeAntflyInferenceBaseUrl(alloc: Allocator, raw: []const u8) ![]u8 {
    const trimmed = std.mem.trim(u8, raw, " \t\r\n/");
    if (trimmed.len == 0) return error.InvalidAntflyInferenceBaseUrl;
    if (std.mem.endsWith(u8, trimmed, "/ai/v1")) return try alloc.dupe(u8, trimmed);

    const scheme_pos = std.mem.indexOf(u8, trimmed, "://");
    const host_start = if (scheme_pos) |pos| pos + 3 else 0;
    if (std.mem.indexOfPos(u8, trimmed, host_start, "/") != null)
        return error.InvalidAntflyInferenceBaseUrl;
    return try std.fmt.allocPrint(alloc, "{s}/ai/v1", .{trimmed});
}

const ParsedGenerateBatchResponse = struct {
    items: []asset_producer.ProducedItem,
    execution: inference_work.ExecutionReport,
};

fn parseAntflyGenerateBatchResponseAlloc(
    alloc: Allocator,
    payload: []const u8,
    requests: []const asset_producer.Request,
) !ParsedGenerateBatchResponse {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGenerateBatchResponse;
    const data = parsed.value.object.get("data") orelse return error.InvalidGenerateBatchResponse;
    if (data != .array) return error.InvalidGenerateBatchResponse;

    const out = try alloc.alloc(asset_producer.ProducedItem, requests.len);
    var seen = try alloc.alloc(bool, requests.len);
    defer alloc.free(seen);
    @memset(seen, false);
    errdefer {
        for (out, seen) |item, was_seen| if (was_seen) switch (item.result) {
            .value => |value| if (value.len > 0) alloc.free(value),
            .item_error => {},
        };
        alloc.free(out);
    }

    for (data.array.items) |item| {
        if (item != .object) return error.InvalidGenerateBatchResponse;
        const raw_index = item.object.get("index") orelse return error.InvalidGenerateBatchResponse;
        if (raw_index != .integer or raw_index.integer < 0) return error.InvalidGenerateBatchResponse;
        const index: usize = @intCast(raw_index.integer);
        if (index >= requests.len or seen[index]) return error.InvalidGenerateBatchResponse;

        const identity = inference_work.WorkIdentity{
            .item_id = requests[index].item_id,
            .source_fingerprint = requests[index].source_fingerprint,
            .page_number = requests[index].page_number,
        };

        if (item.object.get("error")) |err_value| {
            if (err_value != .null) {
                if (err_value != .object) return error.InvalidGenerateBatchResponse;
                const code_value = err_value.object.get("code") orelse return error.InvalidGenerateBatchResponse;
                const retryable_value = err_value.object.get("retryable") orelse return error.InvalidGenerateBatchResponse;
                if (code_value != .string or retryable_value != .bool) return error.InvalidGenerateBatchResponse;
                const retry_after_ms: ?u64 = if (err_value.object.get("retry_after_ms")) |value| switch (value) {
                    .null => null,
                    .integer => |raw| if (raw >= 0) std.math.cast(u64, raw) else null,
                    else => return error.InvalidGenerateBatchResponse,
                } else null;
                out[index] = .{
                    .identity = identity,
                    .result = .{ .item_error = .{
                        .cause = if (retryable_value.bool)
                            error.GenerateBatchItemRetryable
                        else
                            error.GenerateBatchItemRejected,
                        .code = inference_work.ItemFailure.Code.fromWire(code_value.string),
                        .retryable = retryable_value.bool,
                        .retry_after_ms = retry_after_ms,
                    } },
                };
                seen[index] = true;
                continue;
            }
        }
        const response = item.object.get("response") orelse return error.InvalidGenerateBatchResponse;
        if (response == .null) return error.InvalidGenerateBatchResponse;
        out[index] = .{
            .identity = identity,
            .result = .{ .value = try generateResponseContentAlloc(alloc, response) },
        };
        seen[index] = true;
    }

    for (seen) |was_seen| {
        if (!was_seen) return error.InvalidGenerateBatchResponse;
    }
    return .{
        .items = out,
        .execution = try executionReportFromGenerateWire(parsed.value.object.get("execution"), requests.len),
    };
}

fn executionReportFromGenerateWire(value: ?std.json.Value, item_count: usize) !inference_work.ExecutionReport {
    const raw = value orelse return inference_work.ExecutionReport.serial(item_count);
    if (raw != .object) return error.InvalidGenerateBatchExecutionReport;
    var report = inference_work.ExecutionReport{
        .requested_items = try nonNegativeJsonUsize(raw.object.get("requested_items")),
        .native_batches = try nonNegativeJsonUsize(raw.object.get("native_batches")),
        .native_items = try nonNegativeJsonUsize(raw.object.get("native_items")),
        .serial_items = try nonNegativeJsonUsize(raw.object.get("serial_items")),
        .rejected_items = try optionalNonNegativeJsonUsize(raw.object.get("rejected_items")),
        .fallback_items = try nonNegativeJsonUsize(raw.object.get("fallback_items")),
    };
    if (report.fallback_items > 0) report.fallback_reason = "remote_generator_fallback";
    try report.validate();
    if (report.requested_items != item_count) return error.InvalidGenerateBatchExecutionReport;
    return report;
}

fn optionalNonNegativeJsonUsize(value: ?std.json.Value) !usize {
    const raw = value orelse return 0;
    if (raw != .integer or raw.integer < 0) return error.InvalidGenerateBatchExecutionReport;
    return std.math.cast(usize, raw.integer) orelse error.InvalidGenerateBatchExecutionReport;
}

fn nonNegativeJsonUsize(value: ?std.json.Value) !usize {
    const raw = value orelse return error.InvalidGenerateBatchExecutionReport;
    if (raw != .integer or raw.integer < 0) return error.InvalidGenerateBatchExecutionReport;
    return std.math.cast(usize, raw.integer) orelse error.InvalidGenerateBatchExecutionReport;
}

fn generateResponseContentAlloc(alloc: Allocator, response: std.json.Value) ![]u8 {
    if (response != .object) return error.InvalidGenerateBatchResponse;
    const choices = response.object.get("choices") orelse return error.InvalidGenerateBatchResponse;
    if (choices != .array or choices.array.items.len == 0) return error.InvalidGenerateBatchResponse;
    const choice = choices.array.items[0];
    if (choice != .object) return error.InvalidGenerateBatchResponse;
    const message = choice.object.get("message") orelse return error.InvalidGenerateBatchResponse;
    if (message != .object) return error.InvalidGenerateBatchResponse;
    const content = message.object.get("content") orelse return try alloc.dupe(u8, "");
    return switch (content) {
        .string => |text| try alloc.dupe(u8, text),
        .null => try alloc.dupe(u8, ""),
        else => error.InvalidGenerateBatchResponse,
    };
}

fn parseGeneratorContentParts(
    alloc: Allocator,
    source_text: []const u8,
    raw_parts: []const u8,
    media_attachments: []const asset_producer.EncodedMedia,
    encode_attachments: bool,
) ![]generating_runtime.ContentPart {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_parts, .{});
    defer parsed.deinit();
    if (parsed.value != .array) {
        const items = try alloc.alloc(generating_runtime.ContentPart, 1);
        items[0] = .{ .text = try alloc.dupe(u8, source_text) };
        return items;
    }

    var parts = std.ArrayListUnmanaged(generating_runtime.ContentPart).empty;
    errdefer freeGeneratorContentParts(alloc, parts.items);
    for (parsed.value.array.items) |part| {
        if (part != .object) continue;
        const type_value = part.object.get("type") orelse continue;
        if (type_value != .string) continue;
        if (std.mem.eql(u8, type_value.string, "text")) {
            const text = part.object.get("text") orelse continue;
            if (text != .string) continue;
            try parts.append(alloc, .{ .text = try alloc.dupe(u8, text.string) });
        } else if (std.mem.eql(u8, type_value.string, "image_url")) {
            const image_url = part.object.get("image_url") orelse continue;
            if (image_url != .object) continue;
            const url = image_url.object.get("url") orelse continue;
            if (url != .string) continue;
            try parts.append(alloc, .{ .image_url = .{ .url = try alloc.dupe(u8, url.string) } });
        } else if (std.mem.eql(u8, type_value.string, "media")) {
            if (part.object.get("url")) |url| {
                if (url == .string) {
                    const mime_type = if (part.object.get("mime_type")) |mime|
                        if (mime == .string) mime.string else ""
                    else
                        "";
                    try parts.append(alloc, .{ .media = .{
                        .url = try alloc.dupe(u8, url.string),
                        .mime_type = if (mime_type.len > 0) try alloc.dupe(u8, mime_type) else "",
                    } });
                }
            } else if (part.object.get("mime_type")) |mime| {
                const data = part.object.get("data") orelse continue;
                if (mime == .string and data == .string) {
                    try parts.append(alloc, .{ .media = .{
                        .data = try alloc.dupe(u8, data.string),
                        .mime_type = try alloc.dupe(u8, mime.string),
                    } });
                }
            }
        }
    }

    for (media_attachments) |attachment| {
        const data = if (encode_attachments) blk: {
            const encoded_len = std.base64.standard.Encoder.calcSize(attachment.bytes.len);
            const encoded = try alloc.alloc(u8, encoded_len);
            _ = std.base64.standard.Encoder.encode(encoded, attachment.bytes);
            break :blk encoded;
        } else "";
        var data_owned = data.len > 0;
        errdefer if (data_owned) alloc.free(@constCast(data));
        const mime_type = try alloc.dupe(u8, attachment.mime_type);
        var mime_owned = true;
        errdefer if (mime_owned) alloc.free(mime_type);
        try parts.append(alloc, .{ .media = .{
            .data = data,
            .mime_type = mime_type,
        } });
        data_owned = false;
        mime_owned = false;
    }

    if (parts.items.len == 0) {
        try parts.append(alloc, .{ .text = try alloc.dupe(u8, source_text) });
    }
    return try parts.toOwnedSlice(alloc);
}

fn freeGeneratorContentParts(alloc: Allocator, parts: []generating_runtime.ContentPart) void {
    for (parts) |part| {
        switch (part) {
            .text => |text| alloc.free(@constCast(text)),
            .image_url => |image_url| alloc.free(@constCast(image_url.url)),
            .media => |media| {
                if (media.data.len > 0) alloc.free(@constCast(media.data));
                if (media.mime_type.len > 0) alloc.free(@constCast(media.mime_type));
                if (media.url) |url| alloc.free(@constCast(url));
            },
        }
    }
    alloc.free(parts);
}

test "asset producer runtime external inference endpoint is canonical and authoritative" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    const runtime = try Runtime.createOwned(alloc, io_impl.io(), .{
        .inference_api_url = "http://inference-worker.example/",
    });
    defer {
        runtime.deinit();
        alloc.destroy(runtime);
    }

    try std.testing.expectEqualStrings("http://inference-worker.example/ai/v1", runtime.inference_api_url.?);

    const generator = runtime.effectiveGeneratorConfig(.{
        .provider = .antfly,
        .model = "BAAI/bge-m3",
        .url = "",
    });
    try std.testing.expectEqualStrings(runtime.inference_api_url.?, generator.url);

    const reader = runtime.effectiveReaderConfig(.{
        .provider = .antfly,
        .model = "reader-model",
    });
    try std.testing.expectEqualStrings(runtime.inference_api_url.?, reader.resolvedUrl().?);

    const transcriber = runtime.effectiveTranscriberConfig(.{
        .provider = .antfly,
        .model = "transcriber-model",
    });
    try std.testing.expectEqualStrings(runtime.inference_api_url.?, transcriber.resolvedUrl().?);

    const extractor = runtime.effectiveExtractorConfig(.{
        .provider = .antfly,
        .model = "extractor-model",
    });
    try std.testing.expectEqualStrings(runtime.inference_api_url.?, extractor.resolvedUrl().?);
}

test "asset producer runtime parses reader multimodal parts" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "", "[{\"type\":\"text\",\"text\":\"read\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]");
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), source.images.len);
    try std.testing.expectEqualStrings("read", source.prompt.?);
}

test "asset producer runtime parses reader string array source" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "[\"data:image/png;base64,aaa\",\"data:image/jpeg;base64,bbb\"]", null);
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), source.images.len);
    try std.testing.expectEqualStrings("data:image/png;base64,aaa", source.images[0]);
    try std.testing.expectEqualStrings("data:image/jpeg;base64,bbb", source.images[1]);
}

test "asset producer runtime parses empty reader array source as empty input" {
    const alloc = std.testing.allocator;
    var source = try parseReaderSource(alloc, "[]", null);
    defer source.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), source.images.len);
}

fn expectOpenAiMultimodalGeneratorRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"gemma4\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"content\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"media\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"type\":\"image_url\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"url\":\"data:image/png;base64,aaa\"") != null);
}

test "asset producer runtime passes rendered media parts to generators" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/chat/completions", .assert_request = expectOpenAiMultimodalGeneratorRequest, .respond = .{
            .body = "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"vision result\"}}]}",
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();

    const cfg_json = try std.fmt.allocPrint(alloc, "{{\"provider\":\"openai\",\"model\":\"gemma4\",\"url\":\"{s}\"}}", .{server.baseUrl()});
    defer alloc.free(cfg_json);

    var result: ?[]u8 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            cfg: []const u8,
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produce(a, .{
                .producer_type = .generator,
                .config_json = cfg,
                .source_text = "describe",
                .source_parts_json = "[{\"type\":\"text\",\"text\":\"describe\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\",\"mime_type\":\"image/png\"}]",
                .content_type = "text/plain",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, producer, cfg_json, &result, &run_err });
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer alloc.free(result.?);
    try std.testing.expectEqualStrings("vision result", result.?);
}

test "asset producer runtime passes rendered bytes to embedded generators without base64" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    const Local = struct {
        calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .generate_messages_with_attachments = generate,
                .model_capabilities = capabilities,
            };
        }

        fn capabilities(
            _: *anyopaque,
            _: Allocator,
            _: []const u8,
            task: inference_work.Task,
        ) !inference_work.InferenceCapabilities {
            if (task != .generate) return error.TestUnexpectedResult;
            return .{
                .task = .generate,
                .input_modalities = .{ .text = true, .image = true },
                .accepted_mime_types = .{ .text_plain = true, .image_png = true },
                .input_granularity = .page,
                .batch = .{
                    .mode = .serial_compatibility,
                    .preferred_items = 8,
                    .max_items = 128,
                    .max_encoded_media_bytes = 64 * 1024 * 1024,
                    .max_decoded_pixels = 50_000_000,
                    .max_media_parts_per_item = 8,
                },
                .output = .generated_text,
                .borrowed_attachments = true,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn generate(
            ptr: *anyopaque,
            a: Allocator,
            model: []const u8,
            messages: []const generating_runtime.ChatMessage,
            attachments: []const inference_work.Attachment,
        ) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("google/gemma-4-mm", model);
            try std.testing.expectEqual(@as(usize, 1), messages.len);
            const parts = switch (messages[0].content orelse return error.TestUnexpectedResult) {
                .parts => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(@as(usize, 2), parts.len);
            try std.testing.expectEqualStrings("Transcribe exactly", parts[0].text);
            try std.testing.expectEqual(@as(usize, 0), parts[1].media.data.len);
            try std.testing.expectEqual(@as(usize, 1), attachments.len);
            try std.testing.expectEqualSlices(u8, &.{ 0x89, 0x50, 0x4e, 0x47 }, attachments[0].bytes);
            try std.testing.expectEqualStrings("image/png", attachments[0].content_type);
            try std.testing.expectEqualStrings(if (self.calls == 1) "page:3" else "page:4", attachments[0].identity.item_id);
            try std.testing.expectEqualStrings("doc-a", attachments[0].identity.source_fingerprint.?);
            try std.testing.expectEqual(@as(?u32, @intCast(self.calls + 2)), attachments[0].identity.page_number);
            return try a.dupe(u8, "borrowed vision result");
        }
    };

    const png = [_]u8{ 0x89, 0x50, 0x4e, 0x47 };
    var local = Local{};
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const requests = [_]asset_producer.Request{
        .{
            .producer_type = .generator,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"google/gemma-4-mm\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"Transcribe exactly\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "doc-a",
            .item_id = "page:3",
            .page_number = 3,
            .media = &.{.{ .bytes = &png, .mime_type = "image/png" }},
        },
        .{
            .producer_type = .generator,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"google/gemma-4-mm\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"Transcribe exactly\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "doc-a",
            .item_id = "page:4",
            .page_number = 4,
            .media = &.{.{ .bytes = &png, .mime_type = "image/png" }},
        },
    };
    const producer = runtime.producer();
    try std.testing.expectEqual(inference_work.BatchMode.serial_compatibility, try producer.batchMode(alloc, &requests));
    const results = try producer.produceBatch(alloc, &requests);
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }
    try std.testing.expectEqual(@as(usize, 2), results.len);
    for (results) |result| try std.testing.expectEqualStrings("borrowed vision result", result);
    try std.testing.expectEqual(@as(usize, 2), local.calls);
}

fn expectOpenAiToolGeneratorRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"tools\":[") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"tool_choice\":{\"type\":\"function\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"name\":\"emit_relations\"") != null);
}

test "asset producer runtime stores generator tool call arguments" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/chat/completions", .assert_request = expectOpenAiToolGeneratorRequest, .respond = .{
            .body =
            \\{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"emit_relations","arguments":"{\"relations\":[{\"type\":\"signed\",\"target\":{\"id\":\"Ada\"}}]}"}}]}}]}
            ,
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();

    const cfg_json = try std.fmt.allocPrint(alloc,
        \\{{"provider":"openai","model":"gemma4","url":"{s}","tool_output":"arguments","tool_name":"emit_relations","tool_choice":{{"type":"function","function":{{"name":"emit_relations"}}}},"tools":[{{"type":"function","function":{{"name":"emit_relations","parameters":{{"type":"object"}}}}}}]}}
    , .{server.baseUrl()});
    defer alloc.free(cfg_json);

    var result: ?[]u8 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            cfg: []const u8,
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produce(a, .{
                .producer_type = .generator,
                .config_json = cfg,
                .source_text = "signed by Ada",
                .content_type = "application/json",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, producer, cfg_json, &result, &run_err });
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer alloc.free(result.?);
    try std.testing.expectEqualStrings("{\"relations\":[{\"type\":\"signed\",\"target\":{\"id\":\"Ada\"}}]}", result.?);
}

test "asset producer runtime stores forced tool arguments from plain json content" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/chat/completions", .assert_request = expectOpenAiToolGeneratorRequest, .respond = .{
            .body =
            \\{"choices":[{"message":{"role":"assistant","content":"{\"relations\":[{\"type\":\"mentioned in\",\"target\":{\"id\":\"Ada\"}}]}"}}]}
            ,
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();

    const cfg_json = try std.fmt.allocPrint(alloc,
        \\{{"provider":"openai","model":"gemma4","url":"{s}","tool_output":"arguments","tool_name":"emit_relations","tool_choice":{{"type":"function","function":{{"name":"emit_relations"}}}},"tools":[{{"type":"function","function":{{"name":"emit_relations","parameters":{{"type":"object"}}}}}}]}}
    , .{server.baseUrl()});
    defer alloc.free(cfg_json);

    var result: ?[]u8 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            cfg: []const u8,
            out: *?[]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produce(a, .{
                .producer_type = .generator,
                .config_json = cfg,
                .source_text = "mentioned Ada",
                .content_type = "application/json",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, producer, cfg_json, &result, &run_err });
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer alloc.free(result.?);
    try std.testing.expectEqualStrings("{\"relations\":[{\"type\":\"mentioned in\",\"target\":{\"id\":\"Ada\"}}]}", result.?);
}

fn expectAntflyGenerateBatchRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expectEqualStrings("Bearer test-token", req.header("Authorization") orelse "");
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"mode\":\"sync\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"custom_id\":\"0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"custom_id\":\"1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"local-generator\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"content\":\"first prompt\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"content\":\"second prompt\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"max_tokens\":24") != null);

    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, req.body, .{});
    defer parsed.deinit();
    const requests = parsed.value.object.get("requests") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), requests.array.items.len);
    for (requests.array.items) |item| {
        const body = item.object.get("body") orelse return error.TestUnexpectedResult;
        try expectJsonI64Field(body, "max_tokens", 24);
        try expectJsonF32Field(body, "temperature", 0.25);
        try expectJsonF32Field(body, "top_p", 0.9);
        try expectJsonI64Field(body, "top_k", 40);
        try expectJsonF32Field(body, "frequency_penalty", 0.1);
        try expectJsonF32Field(body, "presence_penalty", 0.2);
    }
}

fn expectJsonI64Field(value: std.json.Value, field: []const u8, expected: i64) !void {
    const raw = value.object.get(field) orelse return error.TestUnexpectedResult;
    const actual = switch (raw) {
        .integer => |integer| integer,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(expected, actual);
}

fn expectJsonF32Field(value: std.json.Value, field: []const u8, expected: f32) !void {
    const raw = value.object.get(field) orelse return error.TestUnexpectedResult;
    const actual: f32 = switch (raw) {
        .float => |float| @floatCast(float),
        .integer => |integer| @floatFromInt(integer),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectApproxEqAbs(expected, actual, 0.0001);
}

fn testNativeReaderCapabilities(
    _: *anyopaque,
    _: Allocator,
    _: []const u8,
    task: inference_work.Task,
) !inference_work.InferenceCapabilities {
    if (task != .read) return error.TestUnexpectedResult;
    return .{
        .task = .read,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true, .image_jpeg = true, .image_webp = true },
        .input_granularity = .page,
        .batch = .{
            .mode = .native,
            .preferred_items = 8,
            .max_items = local_reader_batch_ceiling,
            .max_encoded_media_bytes = 64 * 1024 * 1024,
            .max_decoded_pixels = 50_000_000,
            .max_media_parts_per_item = 1,
        },
        .output = .read_result,
        .borrowed_attachments = true,
    };
}

test "owned asset producer foreground contract follows the selected route" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    const Local = struct {
        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .read_images = readImages,
                .model_capabilities = testNativeReaderCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn readImages(_: *anyopaque, _: Allocator, _: []const u8, _: readers.Request) ![]readers.Result {
            return error.TestUnexpectedResult;
        }

        fn readImagesWithContext(_: *anyopaque, _: Allocator, _: []const u8, _: readers.Request, context: RequestContext) ![]readers.Result {
            try context.check();
            return error.TestUnexpectedResult;
        }
    };

    var local = Local{};
    const runtime = try Runtime.createOwned(alloc, io_impl.io(), .{ .antfly_provider = local.provider() });
    const producer = runtime.ownedProducer();
    defer producer.deinit(alloc);
    try std.testing.expect(runtime.http.allocator.vtable == std.heap.smp_allocator.vtable);
    try std.testing.expect(!runtime.http.config.cookies_enabled);

    try std.testing.expect(!try producer.foregroundBoundedForRequests(alloc, &.{.{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
        .source_text = "image",
    }}));
    try std.testing.expect(try producer.foregroundBoundedForRequests(alloc, &.{.{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"remote-reader\",\"url\":\"http://127.0.0.1:8082\"}",
        .source_text = "image",
    }}));
    try std.testing.expect(try producer.foregroundBoundedForRequests(alloc, &.{.{
        .producer_type = .copy,
        .config_json = "",
        .source_text = "copy",
    }}));

    var controlled_provider = local.provider();
    controlled_provider.read_images_with_context = Local.readImagesWithContext;
    const controlled_runtime = try Runtime.createOwned(alloc, io_impl.io(), .{ .antfly_provider = controlled_provider });
    const controlled = controlled_runtime.ownedProducer();
    defer controlled.deinit(alloc);
    try std.testing.expect(try controlled.foregroundBoundedForRequests(alloc, &.{.{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
        .source_text = "image",
    }}));
}

test "asset producer runtime preserves generator policies for single and batch dispatch" {
    const alloc = std.testing.allocator;
    var registry = provider_limits.Registry.init(alloc);
    defer registry.deinit();
    var client = httpx.Client.initWithConfig(alloc, std.testing.io, .{});
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .limits = &registry });
    defer runtime.deinit();
    const raw =
        \\{"provider":"antfly","model":"test","api_url":"http://127.0.0.1:1","max_tokens":10,"rate_limit":{"tokens_per_minute":1}}
    ;
    var parsed = try parseGeneratorProducerConfig(alloc, raw);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?i64, 1), parsed.generator.rate_limit.?.tokens_per_minute);
    const request = asset_producer.Request{ .producer_type = .generator, .config_json = raw, .source_text = "hello" };
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, runtime.produceOne(alloc, request));
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, runtime.tryGenerateBatch(alloc, &.{ request, request }));
}

test "asset producer runtime batches compatible antfly generator requests" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/ai/v1/models", .respond = .{
            .body = "{\"generators\":{\"local-generator\":{\"inputs\":[\"text\",\"image\"],\"inference_capabilities\":{\"task\":\"generate\",\"batch\":{\"mode\":\"serial_compatibility\",\"preferred_items\":8,\"max_items\":128,\"max_encoded_bytes\":104857600,\"max_decoded_pixels\":0,\"max_media_parts_per_item\":8,\"per_item_failures\":true}}}}}",
        } },
        .{ .method = .POST, .path = "/generate/batch", .assert_request = expectAntflyGenerateBatchRequest, .respond = .{
            .body =
            \\{"object":"generate.batch","data":[
            \\{"custom_id":"0","index":0,"response":{"id":"gen-0","object":"chat.completion","created":1,"model":"local-generator","choices":[{"index":0,"message":{"role":"assistant","content":"first answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}},
            \\{"custom_id":"1","index":1,"response":{"id":"gen-1","object":"chat.completion","created":1,"model":"local-generator","choices":[{"index":0,"message":{"role":"assistant","content":"second answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}}
            \\],"summary":{"total":2,"succeeded":2,"failed":0},"execution":{"requested_items":2,"native_batches":0,"native_items":0,"serial_items":2,"fallback_items":0}}
            ,
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();

    const cfg_json = try std.fmt.allocPrint(alloc, "{{\"provider\":\"antfly\",\"model\":\"local-generator\",\"url\":\"{s}\",\"api_key\":\"test-token\",\"max_tokens\":24,\"temperature\":0.25,\"top_p\":0.9,\"top_k\":40,\"frequency_penalty\":0.1,\"presence_penalty\":0.2}}", .{server.baseUrl()});
    defer alloc.free(cfg_json);

    var results: ?[][]u8 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            cfg: []const u8,
            out: *?[][]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produceBatch(a, &.{
                .{
                    .producer_type = .generator,
                    .config_json = cfg,
                    .source_text = "first prompt",
                    .content_type = "text/plain",
                },
                .{
                    .producer_type = .generator,
                    .config_json = cfg,
                    .source_text = "second prompt",
                    .content_type = "text/plain",
                },
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, producer, cfg_json, &results, &run_err });
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer {
        for (results.?) |result| alloc.free(result);
        alloc.free(results.?);
    }
    try std.testing.expectEqual(@as(usize, 2), results.?.len);
    try std.testing.expectEqualStrings("first answer", results.?[0]);
    try std.testing.expectEqualStrings("second answer", results.?[1]);
}

test "asset producer runtime preserves remote reader identity and native execution" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/ai/v1/models", .respond = .{
            .body = "{\"readers\":{\"florence\":{\"inputs\":[\"text\",\"image\"],\"inference_capabilities\":{\"task\":\"read\",\"batch\":{\"mode\":\"native\",\"preferred_items\":2,\"max_items\":2,\"max_encoded_bytes\":33554432,\"max_decoded_pixels\":0,\"max_media_parts_per_item\":1,\"per_item_failures\":false}}}}}",
        } },
        .{ .method = .POST, .path = "/read", .respond = .{
            .body =
            \\{"object":"list","data":[
            \\{"text":"page one","object":"read.result","index":0},
            \\{"text":"page two","object":"read.result","index":1}
            \\],"model":"florence","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3},"execution":{"requested_items":2,"native_batches":1,"native_items":2,"serial_items":0,"rejected_items":0,"fallback_items":0}}
            ,
        } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();
    const cfg_json = try std.fmt.allocPrint(
        alloc,
        "{{\"provider\":\"antfly\",\"model\":\"florence\",\"url\":\"{s}\"}}",
        .{server.baseUrl()},
    );
    defer alloc.free(cfg_json);
    var png = [_]u8{0} ** 24;
    @memcpy(png[0..8], "\x89PNG\r\n\x1a\n");
    std.mem.writeInt(u32, png[16..20], 2, .big);
    std.mem.writeInt(u32, png[20..24], 3, .big);
    const media = [_][1]asset_producer.EncodedMedia{
        .{.{ .bytes = &png, .mime_type = "image/png" }},
        .{.{ .bytes = &png, .mime_type = "image/png" }},
    };
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .reader, .config_json = cfg_json, .source_text = "", .inline_media_trusted = true, .item_id = "page-1", .source_fingerprint = "doc-a", .page_number = 1, .media = &media[0] },
        .{ .producer_type = .reader, .config_json = cfg_json, .source_text = "", .inline_media_trusted = true, .item_id = "page-2", .source_fingerprint = "doc-b", .page_number = 2, .media = &media[1] },
    };

    var result: ?asset_producer.ProducedBatch = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            reqs: []const asset_producer.Request,
            out: *?asset_producer.ProducedBatch,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            _ = p.batchMode(a, reqs) catch |err| {
                err_out.* = err;
                return;
            };
            out.* = p.produceBatchReported(a, reqs) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    try group.concurrent(io, Fiber.run, .{ alloc, producer, &requests, &result, &run_err });
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    var batch = result.?;
    defer batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), batch.execution.native_items);
    try std.testing.expectEqual(@as(usize, 1), batch.execution.native_batches);
    try std.testing.expect(batch.items[0].identity.eql(.{ .item_id = "page-1", .source_fingerprint = "doc-a", .page_number = 1 }));
    try std.testing.expect(batch.items[1].identity.eql(.{ .item_id = "page-2", .source_fingerprint = "doc-b", .page_number = 2 }));
    try std.testing.expectEqualStrings("page one", batch.items[0].result.value);
    try std.testing.expectEqualStrings("page two", batch.items[1].result.value);
}

test "asset producer runtime preserves generator item failures" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .GET, .path = "/ai/v1/models", .respond = .{
            .body = "{\"generators\":{\"gemma4\":{\"inputs\":[\"text\",\"image\"],\"inference_capabilities\":{\"task\":\"generate\",\"batch\":{\"mode\":\"serial_compatibility\",\"preferred_items\":8,\"max_items\":128,\"max_encoded_bytes\":104857600,\"max_decoded_pixels\":0,\"max_media_parts_per_item\":8,\"per_item_failures\":true}}}}}",
        } },
        .{ .method = .POST, .path = "/generate/batch", .respond = .{
            .body =
            \\{"object":"generate.batch","data":[
            \\{"custom_id":"0","index":0,"response":{"choices":[{"message":{"content":"ok"}}]}},
            \\{"custom_id":"1","index":1,"error":{"code":"INVALID_REQUEST","message":"unreadable","retryable":false}}
            \\],"summary":{"total":2,"succeeded":1,"failed":1},"execution":{"requested_items":2,"native_batches":0,"native_items":0,"serial_items":2,"fallback_items":0}}
            ,
        } },
    });
    defer server.deinit();
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();
    const cfg_json = try std.fmt.allocPrint(
        alloc,
        "{{\"provider\":\"antfly\",\"model\":\"gemma4\",\"url\":\"{s}\"}}",
        .{server.baseUrl()},
    );
    defer alloc.free(cfg_json);
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .generator, .config_json = cfg_json, .source_text = "one", .item_id = "page-1", .page_number = 1 },
        .{ .producer_type = .generator, .config_json = cfg_json, .source_text = "two", .item_id = "page-2", .page_number = 2 },
    };
    var result: ?asset_producer.ProducedBatch = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            reqs: []const asset_producer.Request,
            out: *?asset_producer.ProducedBatch,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produceBatchReported(a, reqs) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    try group.concurrent(io, Fiber.run, .{ alloc, producer, &requests, &result, &run_err });
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    var batch = result.?;
    defer batch.deinit(alloc);
    try std.testing.expectEqualStrings("ok", batch.items[0].result.value);
    try std.testing.expectEqual(error.GenerateBatchItemRejected, batch.items[1].result.item_error.cause);
    try std.testing.expectEqual(inference_work.ItemFailure.Code.invalid_request, batch.items[1].result.item_error.code);
    try std.testing.expect(!batch.items[1].result.item_error.retryable);
    try std.testing.expect(batch.items[1].identity.eql(.{ .item_id = "page-2", .page_number = 2 }));
}

test "generator item failures preserve remote retry guidance" {
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .generator, .config_json = "{}", .source_text = "one", .item_id = "page-1" },
        .{ .producer_type = .generator, .config_json = "{}", .source_text = "two", .item_id = "page-2" },
    };
    const payload =
        \\{"data":[
        \\{"index":0,"error":{"code":"SERVICE_UNAVAILABLE","retryable":true,"retry_after_ms":1250}},
        \\{"index":1,"error":{"code":"CONTENT_TOO_LARGE","retryable":false}}
        \\],"execution":{"requested_items":2,"native_batches":0,"native_items":0,"serial_items":2,"rejected_items":0,"fallback_items":0}}
    ;
    var batch = try parseAntflyGenerateBatchResponseAlloc(std.testing.allocator, payload, &requests);
    defer batch.deinit(std.testing.allocator);

    const transient = batch.items[0].result.item_error;
    try std.testing.expectEqual(error.GenerateBatchItemRetryable, transient.cause);
    try std.testing.expectEqual(inference_work.ItemFailure.Code.service_unavailable, transient.code);
    try std.testing.expect(transient.retryable);
    try std.testing.expectEqual(@as(?u64, 1250), transient.retry_after_ms);

    const terminal = batch.items[1].result.item_error;
    try std.testing.expectEqual(error.GenerateBatchItemRejected, terminal.cause);
    try std.testing.expectEqual(inference_work.ItemFailure.Code.content_too_large, terminal.code);
    try std.testing.expect(!terminal.retryable);
}

test "asset producer runtime routes antfly reader without url to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        read_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .read_images = readImages,
                .model_capabilities = testNativeReaderCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn readImages(ptr: *anyopaque, a: Allocator, model: []const u8, request: readers.Request) ![]readers.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.read_calls += 1;
            try std.testing.expectEqualStrings("local-reader", model);
            try std.testing.expectEqual(@as(usize, 1), request.images.len);
            try std.testing.expectEqualStrings(test_png_data_uri, request.images[0]);
            try std.testing.expectEqualStrings("extract", request.prompt.?);
            try std.testing.expectEqual(readers.InlineContentTrust.untrusted, request.inline_content_trust);

            const out = try a.alloc(readers.Result, 1);
            out[0] = .{ .text = try a.dupe(u8, "local read text") };
            return out;
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
        .source_text = "",
        .source_parts_json = "[{\"type\":\"text\",\"text\":\"extract\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,iVBORw0KGgoAAAAAAAAAAAAAAAIAAAAD\"}]",
        .content_type = "text/plain",
    });
    defer alloc.free(result);

    try std.testing.expectEqualStrings("local read text", result);
    try std.testing.expectEqual(@as(usize, 1), local.read_calls);
}

test "asset producer runtime batches compatible antfly reader requests" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        read_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .read_images = readImages,
                .model_capabilities = testNativeReaderCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn readImages(ptr: *anyopaque, a: Allocator, model: []const u8, request: readers.Request) ![]readers.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.read_calls += 1;
            try std.testing.expectEqualStrings("local-reader", model);
            try std.testing.expectEqual(@as(usize, 2), request.images.len);
            try std.testing.expectEqualStrings(test_png_data_uri, request.images[0]);
            try std.testing.expectEqualStrings(test_png_data_uri, request.images[1]);
            try std.testing.expect(request.prompt == null);
            try std.testing.expectEqual(readers.InlineContentTrust.trusted_internal, request.inline_content_trust);

            const out = try a.alloc(readers.Result, 2);
            out[0] = .{ .text = try a.dupe(u8, "first") };
            out[1] = .{ .text = try a.dupe(u8, "second") };
            return out;
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const requests = [_]asset_producer.Request{
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = test_png_data_uri,
            .content_type = "text/plain",
            .inline_media_trusted = true,
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = test_png_data_uri,
            .content_type = "text/plain",
            .inline_media_trusted = true,
        },
    };
    try std.testing.expect(try producer.canProduceBatch(alloc, &requests));
    const results = try producer.produceBatch(alloc, &requests);
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("first", results[0]);
    try std.testing.expectEqualStrings("second", results[1]);
    try std.testing.expectEqual(@as(usize, 1), local.read_calls);

    const multi_image_prompt = [_]asset_producer.Request{.{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
        .source_text = "[\"data:image/png;base64,aaa\",\"data:image/png;base64,bbb\"]",
        .content_type = "text/plain",
        .inline_media_trusted = true,
    }};
    try std.testing.expect(!(try producer.canProduceBatch(alloc, &multi_image_prompt)));
}

test "asset producer raw raster selection requires local physical capability and borrows pixels" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();

    const Local = struct {
        expected: [2][*]const u8,
        calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .model_capabilities = capabilities,
                .read_raster_images_reported = readRasters,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn capabilities(_: *anyopaque, _: Allocator, _: []const u8, task: inference_work.Task) !inference_work.InferenceCapabilities {
            var result = try testNativeReaderCapabilities(undefined, undefined, "", task);
            result.borrowed_rasters = true;
            return result;
        }

        fn readRasters(
            ptr: *anyopaque,
            result_alloc: Allocator,
            model: []const u8,
            request: readers.RasterRequest,
        ) !readers.BatchResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("florence2", model);
            try std.testing.expectEqualStrings("<OCR>", request.prompt.?);
            try std.testing.expectEqual(@as(usize, 2), request.images.len);
            for (request.images, 0..) |raster, i| {
                try std.testing.expectEqual(@intFromPtr(self.expected[i]), @intFromPtr(raster.bytes.ptr));
                try std.testing.expectEqual(@as(u32, 2), raster.width);
                try std.testing.expectEqual(@as(u32, 1), raster.height);
                try std.testing.expectEqual(@as(usize, 8), raster.stride_bytes);
            }
            const out = try result_alloc.alloc(readers.Result, 2);
            for (out, request.images, 0..) |*result, raster, i| result.* = .{
                .text = try result_alloc.dupe(u8, if (i == 0) "first" else "second"),
                .item_id = try result_alloc.dupe(u8, raster.item_id),
                .source_fingerprint = if (raster.source_fingerprint) |value| try result_alloc.dupe(u8, value) else null,
                .page_number = raster.page_number,
            };
            return .{
                .items = out,
                .execution = .{ .requested_items = 2, .native_batches = 1, .native_items = 2 },
            };
        }
    };

    var first_pixels = [_]u8{ 1, 2, 3, 255, 4, 5, 6, 255 };
    var second_pixels = [_]u8{ 7, 8, 9, 255, 10, 11, 12, 255 };
    var local = Local{ .expected = .{ first_pixels[0..].ptr, second_pixels[0..].ptr } };
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();
    const requests = [_]asset_producer.Request{
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"florence2\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "doc",
            .item_id = "page:1",
            .page_number = 1,
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"florence2\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "doc",
            .item_id = "page:2",
            .page_number = 2,
        },
    };
    const rasters = [_]readers.RasterImage{
        .{ .bytes = &first_pixels, .width = 2, .height = 1, .stride_bytes = 8, .item_id = "page:1", .source_fingerprint = "doc", .page_number = 1 },
        .{ .bytes = &second_pixels, .width = 2, .height = 1, .stride_bytes = 8, .item_id = "page:2", .source_fingerprint = "doc", .page_number = 2 },
    };
    try std.testing.expect(try producer.borrowedRasterBatchAvailable(alloc, &requests));
    var batch = try producer.produceBorrowedRasterBatchReported(alloc, &requests, &rasters);
    defer batch.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), local.calls);
    try std.testing.expectEqualStrings("first", batch.items[0].result.value);
    try std.testing.expectEqualStrings("second", batch.items[1].result.value);
    first_pixels[0] = 99;
    try std.testing.expectEqualStrings("first", batch.items[0].result.value);

    var remote_requests = requests;
    remote_requests[0].config_json = "{\"provider\":\"antfly\",\"model\":\"florence2\",\"url\":\"https://inference.example/ai/v1\"}";
    remote_requests[1].config_json = remote_requests[0].config_json;
    try std.testing.expect(!try producer.borrowedRasterBatchAvailable(alloc, &remote_requests));
}

test "asset producer runtime batches local encoded media without base64 adaptation" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        read_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .read_images = readImages,
                .read_encoded_images = readEncodedImages,
                .model_capabilities = testNativeReaderCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn readImages(_: *anyopaque, _: Allocator, _: []const u8, _: readers.Request) ![]readers.Result {
            return error.TestUnexpectedResult;
        }

        fn readEncodedImages(ptr: *anyopaque, a: Allocator, model: []const u8, request: readers.EncodedRequest) ![]readers.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("local-reader", model);
            try std.testing.expectEqual(@as(usize, 2), request.images.len);
            for (request.images, 0..) |image, i| {
                const expected = if (i == 0) &test_png_2x3 else &test_png_3x3;
                try std.testing.expectEqualSlices(u8, expected, image.bytes);
                try std.testing.expectEqualStrings("image/png", image.mime_type);
                try std.testing.expectEqualStrings(if (i == 0) "first-source" else "second-source", image.source_fingerprint.?);
            }
            try std.testing.expectEqualStrings("<OCR>", request.prompt.?);
            try std.testing.expect(request.source_fingerprint != null);

            const out = try a.alloc(readers.Result, 2);
            out[0] = .{ .text = try a.dupe(u8, "first") };
            out[1] = .{ .text = try a.dupe(u8, "second") };
            self.read_calls += 1;
            return out;
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const requests = [_]asset_producer.Request{
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "first-source",
            .media = &.{.{ .bytes = &test_png_2x3, .mime_type = "image/png" }},
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "second-source",
            .media = &.{.{ .bytes = &test_png_3x3, .mime_type = "image/png" }},
        },
    };
    try std.testing.expect(try producer.canProduceBatch(alloc, &requests));
    try std.testing.expectEqual(inference_work.BatchMode.native, try producer.batchMode(alloc, &requests));
    const results = try producer.produceBatch(alloc, &requests);
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqualStrings("first", results[0]);
    try std.testing.expectEqualStrings("second", results[1]);
    try std.testing.expectEqual(@as(usize, 1), local.read_calls);
}

fn expectSingleImageOpenAiReaderRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, req.body, "\"type\":\"image_url\""));
}

test "asset producer runtime keeps prompt-level remote readers sequential" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/chat/completions", .assert_request = expectSingleImageOpenAiReaderRequest, .respond = .{
            .body = "{\"choices\":[{\"message\":{\"content\":\"ocr text\"}}]}",
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    defer runtime.deinit();
    const producer = runtime.producer();
    const cfg_json = try std.fmt.allocPrint(
        alloc,
        "{{\"provider\":\"openai\",\"model\":\"vision-reader\",\"base_url\":\"{s}\"}}",
        .{server.baseUrl()},
    );
    defer alloc.free(cfg_json);

    const requests = [_]asset_producer.Request{
        .{ .producer_type = .reader, .config_json = cfg_json, .source_text = "data:image/png;base64,aaa", .content_type = "text/plain" },
        .{ .producer_type = .reader, .config_json = cfg_json, .source_text = "data:image/png;base64,bbb", .content_type = "text/plain" },
    };
    try std.testing.expect(!(try producer.canProduceBatch(alloc, &requests)));

    var results: ?[][]u8 = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(
            a: Allocator,
            p: asset_producer.Producer,
            cfg: []const u8,
            out: *?[][]u8,
            err_out: *?anyerror,
        ) std.Io.Cancelable!void {
            out.* = p.produceBatch(a, &.{
                .{ .producer_type = .reader, .config_json = cfg, .source_text = "data:image/png;base64,aaa", .content_type = "text/plain" },
                .{ .producer_type = .reader, .config_json = cfg, .source_text = "data:image/png;base64,bbb", .content_type = "text/plain" },
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, producer, cfg_json, &results, &run_err });
    try server.handleOne();
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer {
        for (results.?) |result| alloc.free(result);
        alloc.free(results.?);
    }
    try std.testing.expectEqual(@as(usize, 2), results.?.len);
    try std.testing.expectEqualStrings("ocr text", results.?[0]);
    try std.testing.expectEqualStrings("ocr text", results.?[1]);
}

test "asset producer runtime chunks local antfly reader batches to inference cap" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        read_calls: usize = 0,
        batch_lengths: [2]usize = .{ 0, 0 },

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .read_images = readImages,
                .model_capabilities = testNativeReaderCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn readImages(ptr: *anyopaque, a: Allocator, model: []const u8, request: readers.Request) ![]readers.Result {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("local-reader", model);
            try std.testing.expect(request.images.len > 0);
            try std.testing.expect(request.images.len <= localReaderBatchMaxImages());
            try std.testing.expect(request.prompt == null);
            if (self.read_calls >= self.batch_lengths.len) return error.TestUnexpectedResult;
            if (self.read_calls == 0)
                try std.testing.expectEqualStrings("head-source", request.source_fingerprint.?)
            else
                try std.testing.expectEqualStrings("tail-source", request.source_fingerprint.?);
            self.batch_lengths[self.read_calls] = request.images.len;
            self.read_calls += 1;

            const out = try a.alloc(readers.Result, request.images.len);
            var filled: usize = 0;
            errdefer {
                for (out[0..filled]) |*result| readers.deinitResult(a, result);
                a.free(out);
            }
            for (request.images, 0..) |image, i| {
                out[i] = .{ .text = try a.dupe(u8, image) };
                filled += 1;
            }
            return out;
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const expected_batch_images = localReaderBatchMaxImages();
    const request_count = expected_batch_images + 1;
    const urls = try alloc.alloc([]u8, request_count);
    defer alloc.free(urls);
    var urls_filled: usize = 0;
    defer {
        for (urls[0..urls_filled]) |url| alloc.free(url);
    }
    const requests = try alloc.alloc(asset_producer.Request, request_count);
    defer alloc.free(requests);
    for (0..request_count) |i| {
        urls[i] = try alloc.dupe(u8, test_png_data_uri);
        urls_filled += 1;
        requests[i] = .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = urls[i],
            .content_type = "text/plain",
            .source_fingerprint = if (i < expected_batch_images) "head-source" else "tail-source",
        };
    }

    const results = try producer.produceBatch(alloc, requests);
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, request_count), results.len);
    try std.testing.expectEqual(@as(usize, 2), local.read_calls);
    try std.testing.expectEqual(expected_batch_images, local.batch_lengths[0]);
    try std.testing.expectEqual(@as(usize, 1), local.batch_lengths[1]);
    for (results, urls) |result, url| {
        try std.testing.expectEqualStrings(url, result);
    }
}

test "asset producer runtime batches compatible antfly transcriber requests" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        transcribe_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .transcribe_audio = transcribeAudio,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn transcribeAudio(ptr: *anyopaque, a: Allocator, model: []const u8, request: transcribing.Request) !transcribing.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.transcribe_calls += 1;
            try std.testing.expectEqualStrings("local-transcriber", model);
            try std.testing.expectEqualStrings("en-US", request.language.?);
            const text = if (std.mem.endsWith(u8, request.url, "a.wav")) "first transcript" else "second transcript";
            return .{
                .text = try a.dupe(u8, text),
                .language = try a.dupe(u8, "en-US"),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const inline_audio = asset_producer.Request{
        .producer_type = .transcriber,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-transcriber\"}",
        .source_text = "DATA:audio/wav;BASE64,AQ==",
        .content_type = "text/plain",
    };
    const inline_plan = try producer.invocationMemoryForRequests(alloc, &.{inline_audio});
    try std.testing.expectEqual(inference_work.AttachmentTransport.data_uri, inline_plan.attachment_transport);
    var remote_audio = inline_audio;
    remote_audio.source_text = "file:///tmp/audio.wav";
    try std.testing.expectError(
        error.InferenceInvocationMemoryUnavailable,
        producer.invocationMemoryForRequests(alloc, &.{ inline_audio, remote_audio }),
    );

    const requests = [_]asset_producer.Request{
        .{
            .producer_type = .transcriber,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-transcriber\",\"language_code\":\"en-US\"}",
            .source_text = "file:///tmp/a.wav",
            .content_type = "text/plain",
        },
        .{
            .producer_type = .transcriber,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-transcriber\",\"language_code\":\"en-US\"}",
            .source_text = "file:///tmp/b.wav",
            .content_type = "text/plain",
        },
    };
    try std.testing.expect(try producer.canProduceBatch(alloc, &requests));
    try std.testing.expectEqual(
        inference_work.BatchMode.serial_compatibility,
        try producer.batchMode(alloc, &requests),
    );

    const results = try producer.produceBatch(alloc, &requests);
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("first transcript", results[0]);
    try std.testing.expectEqualStrings("second transcript", results[1]);
    try std.testing.expectEqual(@as(usize, 2), local.transcribe_calls);
}

test "asset producer runtime routes antfly transcriber without url to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        transcribe_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .transcribe_audio = transcribeAudio,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn transcribeAudio(ptr: *anyopaque, a: Allocator, model: []const u8, request: transcribing.Request) !transcribing.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.transcribe_calls += 1;
            try std.testing.expectEqualStrings("local-transcriber", model);
            try std.testing.expectEqualStrings("file:///tmp/audio.wav", request.url);
            try std.testing.expectEqualStrings("en-US", request.language.?);
            return .{
                .text = try a.dupe(u8, "local transcript"),
                .language = try a.dupe(u8, "en-US"),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .transcriber,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-transcriber\",\"language_code\":\"en-US\"}",
        .source_text = "file:///tmp/audio.wav",
        .content_type = "text/plain",
    });
    defer alloc.free(result);

    try std.testing.expectEqualStrings("local transcript", result);
    try std.testing.expectEqual(@as(usize, 1), local.transcribe_calls);
}

test "asset producer runtime routes antfly extractor without url to local provider" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        extract_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .extract = extract,
                .model_capabilities = modelCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn modelCapabilities(_: *anyopaque, _: Allocator, _: []const u8, task: inference_work.Task) !inference_work.InferenceCapabilities {
            try std.testing.expectEqual(inference_work.Task.extract, task);
            return .{
                .task = .extract,
                .input_modalities = .{ .text = true },
                .accepted_mime_types = .{ .text_plain = true },
                .input_granularity = .item,
                .batch = .{ .mode = .serial_compatibility, .preferred_items = 8, .max_items = 128 },
                .output = .extraction,
                .prompt_policy = .structured_schema,
            };
        }

        fn extract(ptr: *anyopaque, a: Allocator, model: []const u8, request: extracting.Request) !extracting.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.extract_calls += 1;
            try std.testing.expectEqualStrings("local-extractor", model);
            try std.testing.expectEqual(@as(usize, 1), request.inputs.len);
            try std.testing.expect(std.mem.indexOf(u8, request.inputs[0].content_json, "Ada") != null);
            try std.testing.expect(std.mem.indexOf(u8, request.schema_json, "person") != null);
            return .{
                .allocator = a,
                .json = try a.dupe(u8, "{\"object\":\"extraction\",\"model\":\"local-extractor\",\"data\":[{\"entities\":[{\"label\":\"person\",\"text\":\"Ada\"}],\"relations\":[]}]}"),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .extractor,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-extractor\",\"schema\":{\"entities\":[\"person\"]}}",
        .source_text = "Ada works at Antfly.",
        .content_type = "application/json",
    });
    defer alloc.free(result);

    try std.testing.expect(std.mem.indexOf(u8, result, "\"entities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"Ada\"") != null);
    try std.testing.expectEqual(@as(usize, 1), local.extract_calls);
}

test "asset producer runtime batches compatible antfly extractor requests" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const Local = struct {
        extract_calls: usize = 0,

        fn provider(self: *@This()) managed_embedder.AntflyProvider {
            return .{
                .ptr = self,
                .owns_invocation_admission = true,
                .embed_dense_texts = embedDense,
                .embed_sparse_texts = embedSparse,
                .extract = extract,
                .model_capabilities = modelCapabilities,
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn modelCapabilities(_: *anyopaque, _: Allocator, _: []const u8, task: inference_work.Task) !inference_work.InferenceCapabilities {
            try std.testing.expectEqual(inference_work.Task.extract, task);
            return .{
                .task = .extract,
                .input_modalities = .{ .text = true },
                .accepted_mime_types = .{ .text_plain = true },
                .input_granularity = .item,
                .batch = .{ .mode = .serial_compatibility, .preferred_items = 8, .max_items = 128 },
                .output = .extraction,
                .prompt_policy = .structured_schema,
            };
        }

        fn extract(ptr: *anyopaque, a: Allocator, model: []const u8, request: extracting.Request) !extracting.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.extract_calls += 1;
            try std.testing.expectEqualStrings("local-extractor", model);
            try std.testing.expectEqual(@as(usize, 2), request.inputs.len);
            try std.testing.expect(request.inputs[0].id != null);
            try std.testing.expect(request.inputs[1].id != null);
            try std.testing.expect(std.mem.indexOf(u8, request.inputs[0].content_json, "Ada") != null);
            try std.testing.expect(std.mem.indexOf(u8, request.inputs[1].content_json, "Grace") != null);
            return .{
                .allocator = a,
                .json = try std.fmt.allocPrint(
                    a,
                    "{{\"object\":\"extraction\",\"model\":\"local-extractor\",\"data\":[{{\"id\":{f},\"entities\":[{{\"label\":\"person\",\"text\":\"Grace\"}}],\"relations\":[]}},{{\"id\":{f},\"entities\":[{{\"label\":\"person\",\"text\":\"Ada\"}}],\"relations\":[]}}]}}",
                    .{ std.json.fmt(request.inputs[1].id.?, .{}), std.json.fmt(request.inputs[0].id.?, .{}) },
                ),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
    defer runtime.deinit();
    const producer = runtime.producer();

    const results = try producer.produceBatch(alloc, &.{
        .{
            .producer_type = .extractor,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-extractor\",\"schema\":{\"entities\":[\"person\"]}}",
            .source_text = "Ada works at Antfly.",
            .content_type = "application/json",
        },
        .{
            .producer_type = .extractor,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-extractor\",\"schema\":{\"entities\":[\"person\"]}}",
            .source_text = "Grace works at Antfly.",
            .content_type = "application/json",
        },
    });
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expect(std.mem.indexOf(u8, results[0], "\"Ada\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, results[1], "\"Grace\"") != null);
    try std.testing.expectEqual(@as(usize, 1), local.extract_calls);
}
