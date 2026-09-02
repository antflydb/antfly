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
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const extracting = @import("antfly_extracting");
const extraction_api = @import("antfly_extraction_openapi");
const asset_producer = @import("storage/db/enrichment/asset_producer.zig");
const inference_work = @import("inference/work.zig");
const remote_capabilities = @import("inference/remote_capabilities.zig");

const Allocator = std.mem.Allocator;
const local_reader_batch_ceiling: usize = 64;
const default_local_reader_batch_images: usize = 8;
const max_asset_provider_timeout_ms: u64 = 300_000;
const max_asset_provider_response_bytes: usize = 4 << 20;
const invocation_response_resident_multiplier: usize = 4;
const invocation_nonmedia_resident_multiplier: usize = 8;
const invocation_result_bytes_per_item: usize = 1 << 20;
const invocation_control_bytes_per_item: usize = 4096;
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

pub const Runtime = struct {
    alloc: Allocator,
    http: *httpx.Client,
    capability_cache: remote_capabilities.Cache,
    owned_http: ?*httpx.Client = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    secret_store: ?*common_secrets.FileStore = null,

    pub const Options = struct {
        antfly_provider: ?managed_embedder.AntflyProvider = null,
        secret_store: ?*common_secrets.FileStore = null,
    };

    pub fn init(alloc: Allocator, http: *httpx.Client) Runtime {
        return initWithOptions(alloc, http, .{});
    }

    pub fn initWithOptions(alloc: Allocator, http: *httpx.Client, options: Options) Runtime {
        return .{
            .alloc = alloc,
            .http = http,
            .capability_cache = remote_capabilities.Cache.init(alloc, http.io),
            .antfly_provider = options.antfly_provider,
            .secret_store = options.secret_store,
        };
    }

    pub fn createOwned(alloc: Allocator, io: std.Io, options: Options) !*Runtime {
        const runtime = try alloc.create(Runtime);
        errdefer alloc.destroy(runtime);

        const client = try alloc.create(httpx.Client);
        errdefer alloc.destroy(client);
        var client_config = httpx.ClientConfig{ .keep_alive = false };
        client_config.max_response_size = max_asset_provider_response_bytes;
        client_config.timeouts = httpx.Timeouts.uniform(max_asset_provider_timeout_ms);
        client_config.timeouts.request_ms = max_asset_provider_timeout_ms;
        client.* = httpx.Client.initWithConfig(alloc, io, client_config);
        errdefer client.deinit();

        runtime.* = Runtime.initWithOptions(alloc, client, options);
        runtime.owned_http = client;
        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
        self.capability_cache.deinit();
        if (self.owned_http) |client| {
            client.deinit();
            self.alloc.destroy(client);
            self.owned_http = null;
        }
        self.* = undefined;
    }

    pub fn producer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            // A caller-owned HTTP client may not have a finite request timeout,
            // so this borrowed interface cannot advertise the foreground
            // liveness contract.
            .vtable = &.{ .produce = produce, .produce_batch = produceBatch, .produce_batch_reported = produceBatchReported, .batch_mode = batchMode, .can_produce_batch = canProduceBatch, .capabilities_for_requests = capabilitiesForRequests, .invocation_memory_for_requests = invocationMemoryForRequests },
        };
    }

    pub fn ownedProducer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            .vtable = &.{
                .produce = produce,
                .produce_batch = produceBatch,
                .produce_batch_reported = produceBatchReported,
                .batch_mode = batchMode,
                .can_produce_batch = canProduceBatch,
                .capabilities_for_requests = capabilitiesForRequests,
                .invocation_memory_for_requests = invocationMemoryForRequests,
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
        if (!requestsShareConfig(requests)) return error.InferenceInvocationMemoryUnavailable;

        var has_media = false;
        var nonmedia_bytes: usize = 0;
        for (requests) |request| {
            has_media = has_media or request.media.len > 0;
            nonmedia_bytes = std.math.add(usize, nonmedia_bytes, request.config_json.len) catch
                return error.InferenceEncodedBytesExceeded;
            nonmedia_bytes = std.math.add(usize, nonmedia_bytes, request.source_text.len) catch
                return error.InferenceEncodedBytesExceeded;
            if (request.source_parts_json) |parts| nonmedia_bytes = std.math.add(usize, nonmedia_bytes, parts.len) catch
                return error.InferenceEncodedBytesExceeded;
        }
        if (!has_media) return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = nonmedia_bytes };

        var remote = false;
        const transport: inference_work.AttachmentTransport = switch (requests[0].producer_type) {
            .reader => blk: {
                var parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                remote = !isLocalReaderProvider(parsed.value.provider, parsed.value.resolvedUrl());
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    const binary = local.read_encoded_images != null or local.read_encoded_images_reported != null;
                    if (!binary and local.read_images == null) return error.InferenceInvocationMemoryUnavailable;
                    break :blk if (binary) .borrowed_binary else .data_uri;
                }
                break :blk .data_uri;
            },
            .generator => blk: {
                var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
                defer parsed.deinit(alloc);
                remote = parsed.generator.url.len > 0 or parsed.generator.provider != .antfly;
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    if (local.generate_messages_with_attachments != null) break :blk .borrowed_binary;
                    if (local.generate_messages == null) return error.InferenceInvocationMemoryUnavailable;
                    break :blk .data_uri;
                }
                // Remote Antfly batches stream one base64 body, but OCR may
                // recover from a failed batch through the singleton provider
                // adapter, which retains data URIs. Reserve the larger legal
                // execution path so fallback cannot escape admission.
                break :blk .data_uri;
            },
            .extractor => blk: {
                var parsed = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
                defer parsed.deinit(alloc);
                remote = !isLocalExtractionProvider(parsed.provider, parsed.resolvedUrl());
                if (!remote) {
                    const local = self.antfly_provider orelse return error.InferenceInvocationMemoryUnavailable;
                    if (local.extract == null) return error.InferenceInvocationMemoryUnavailable;
                }
                break :blk extractorAttachmentTransport(parsed);
            },
            // Audio and future family adapters must publish an exact route
            // before they can participate in bounded media planning.
            .transcriber, .copy, .document_extraction => return error.InferenceInvocationMemoryUnavailable,
        };

        var fixed = std.math.mul(usize, nonmedia_bytes, invocation_nonmedia_resident_multiplier) catch
            return error.InferenceEncodedBytesExceeded;
        const control = std.math.mul(usize, requests.len, invocation_control_bytes_per_item) catch
            return error.InferenceEncodedBytesExceeded;
        fixed = std.math.add(usize, fixed, control) catch return error.InferenceEncodedBytesExceeded;
        const results = std.math.mul(usize, requests.len, invocation_result_bytes_per_item) catch
            return error.InferenceEncodedBytesExceeded;
        fixed = std.math.add(usize, fixed, results) catch return error.InferenceEncodedBytesExceeded;
        if (remote) {
            const response_peak = std.math.mul(
                usize,
                self.http.maxResponseSize(),
                invocation_response_resident_multiplier,
            ) catch return error.InferenceEncodedBytesExceeded;
            fixed = std.math.add(usize, fixed, response_peak) catch return error.InferenceEncodedBytesExceeded;
        }
        return .{ .attachment_transport = transport, .fixed_bytes = fixed };
    }

    fn requestForegroundBounded(self: *Runtime, alloc: Allocator, request: asset_producer.Request) !bool {
        return switch (request.producer_type) {
            // These routes never enter an external callback. Unsupported
            // document extraction also fails synchronously in produceOne.
            .copy, .document_extraction => true,
            .generator => blk: {
                var parsed = try parseGeneratorProducerConfig(alloc, request.config_json);
                defer parsed.deinit(alloc);
                const local = self.antfly_provider orelse break :blk true;
                break :blk !(parsed.generator.provider == .antfly and
                    parsed.generator.url.len == 0 and
                    (local.generate_messages != null or local.generate_text != null));
            },
            .reader => blk: {
                var parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const local = self.antfly_provider orelse break :blk true;
                break :blk !(isLocalReaderProvider(parsed.value.provider, parsed.value.resolvedUrl()) and
                    (local.read_images != null or local.read_encoded_images != null));
            },
            .transcriber => blk: {
                var parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const local = self.antfly_provider orelse break :blk true;
                break :blk !(isLocalTranscriberProvider(parsed.value.provider, parsed.value.resolvedUrl()) and
                    local.transcribe_audio != null);
            },
            .extractor => blk: {
                var parsed = try extracting.parseConfigFromSlice(alloc, request.config_json);
                defer parsed.deinit(alloc);
                const local = self.antfly_provider orelse break :blk true;
                break :blk !(isLocalExtractionProvider(parsed.provider, parsed.resolvedUrl()) and
                    local.extract != null);
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

    fn canProduceBatch(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        return try batchMode(ptr, alloc, requests) != .none;
    }

    fn capabilitiesForRequests(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !?inference_work.InferenceCapabilities {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0 or !requestsShareConfig(requests)) return null;
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
                if (parsed.generator.provider == .antfly and parsed.generator.url.len == 0) {
                    const local = self.antfly_provider orelse break :blk null;
                    const resolve = local.model_capabilities orelse break :blk null;
                    break :blk try resolve(local.ptr, alloc, parsed.generator.model, .generate);
                }
                if (parsed.generator.provider == .antfly and parsed.generator.url.len > 0) {
                    var secret = try common_secrets.SecretValue.initConfigOrEnv(
                        alloc,
                        parsed.generator.api_key,
                        "ANTFLY_INFERENCE_API_KEY",
                    );
                    defer secret.deinit(alloc);
                    const token = secret.resolveOwned(alloc, self.secret_store) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => break :blk null,
                    };
                    defer if (token) |value| alloc.free(value);
                    var auth_value: ?[]u8 = null;
                    defer if (auth_value) |value| alloc.free(value);
                    var header_storage: [1][2][]const u8 = undefined;
                    const headers: []const [2][]const u8 = if (token) |value| auth: {
                        auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
                        header_storage[0] = .{ "Authorization", auth_value.? };
                        break :auth &header_storage;
                    } else &.{};
                    break :blk self.capability_cache.getOrDiscover(
                        self.http,
                        parsed.generator.url,
                        parsed.generator.model,
                        .generate,
                        headers,
                    ) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => null,
                    };
                }
                break :blk null;
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
                // The embedded callback is one invocation per item today.
                if (parsed.generator.url.len == 0) break :blk .serial_compatibility;
                const capabilities = (try capabilitiesForRequests(ptr, alloc, requests)) orelse
                    break :blk .serial_compatibility;
                break :blk capabilities.batch.mode;
            },
            // The transcriber interface accepts one audio input per call. Its
            // produceBatch implementation is therefore a compatibility loop,
            // not an atomic provider batch, and must be isolated by callers.
            .transcriber => .none,
            .extractor => blk: {
                if (!try self.canExtractBatch(alloc, requests)) break :blk .none;
                const capabilities = (try capabilitiesForRequests(ptr, alloc, requests)) orelse break :blk .none;
                break :blk capabilities.batch.mode;
            },
        };
    }

    fn requestsShareConfig(requests: []const asset_producer.Request) bool {
        for (requests[1..]) |request| {
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return false;
        }
        return true;
    }

    fn canReadBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareConfig(requests)) return false;
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
                if (request.media.len == 0) return false;
            } else if (source.images.len == 0) return false;
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
        cfg: readers.Config,
    ) !?inference_work.InferenceCapabilities {
        if (!isLocalReaderProvider(cfg.provider, cfg.resolvedUrl())) {
            if (cfg.provider != .antfly) return null;
            const endpoint = cfg.resolvedUrl() orelse return null;
            var auth_value: ?[]u8 = null;
            defer if (auth_value) |value| alloc.free(value);
            var header_storage: [1][2][]const u8 = undefined;
            const headers: []const [2][]const u8 = if (cfg.bearer_token orelse cfg.api_key) |token| blk: {
                auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
                header_storage[0] = .{ "Authorization", auth_value.? };
                break :blk &header_storage;
            } else &.{};
            return self.capability_cache.getOrDiscover(
                self.http,
                endpoint,
                cfg.model orelse "",
                .read,
                headers,
            ) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => null,
            };
        }
        const local = self.antfly_provider orelse return null;
        const resolve = local.model_capabilities orelse return null;
        const capabilities = try resolve(local.ptr, alloc, cfg.model orelse "", .read);
        try capabilities.validate();
        if (capabilities.task != .read) return error.InvalidInferenceCapabilities;
        return capabilities;
    }

    fn canGenerateBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareConfig(requests)) return false;
        var all_have_media = true;
        for (requests) |request| {
            if (request.media.len > 0 and !request.inline_media_trusted) return false;
            all_have_media = all_have_media and request.media.len > 0;
        }
        var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
        defer parsed.deinit(alloc);
        const cfg = parsed.generator;
        const local_serial = all_have_media and cfg.provider == .antfly and cfg.url.len == 0 and
            self.antfly_provider != null and
            self.antfly_provider.?.generate_messages_with_attachments != null;
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

    fn canExtractBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareConfig(requests)) return false;
        const capabilities = (try capabilitiesForRequests(self, alloc, requests)) orelse return false;
        if (capabilities.task != .extract or capabilities.result_cardinality != .one_per_item or
            capabilities.batch.mode == .none) return false;
        var cfg = extracting.parseConfigFromSlice(alloc, requests[0].config_json) catch return false;
        defer cfg.deinit(alloc);
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
        if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) {
            const local = self.antfly_provider orelse return null;
            if (local.extract == null) return null;
            const resolve = local.model_capabilities orelse return null;
            const capabilities = try resolve(local.ptr, alloc, cfg.model, .extract);
            try capabilities.validate();
            if (capabilities.task != .extract) return error.InvalidInferenceCapabilities;
            return capabilities;
        }
        if (cfg.provider != .antfly) return null;
        const endpoint = cfg.resolvedUrl() orelse return null;
        var auth_value: ?[]u8 = null;
        defer if (auth_value) |value| alloc.free(value);
        var header_storage: [1][2][]const u8 = undefined;
        const headers: []const [2][]const u8 = if (cfg.bearer_token orelse cfg.api_key) |token| blk: {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
            header_storage[0] = .{ "Authorization", auth_value.? };
            break :blk &header_storage;
        } else &.{};
        return self.capability_cache.getOrDiscover(
            self.http,
            endpoint,
            cfg.model,
            .extract,
            headers,
        ) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => null,
        };
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
            out[i] = try self.produceOne(alloc, request);
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
        const attachment_transport = extractorAttachmentTransport(cfg);
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
        };
        var response = if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.BatchIncompatible;
            const extract_fn = local.extract orelse return error.BatchIncompatible;
            break :blk try extract_fn(local.ptr, alloc, cfg.model, extract_request);
        } else try extracting.extractWithConfig(alloc, self.http, cfg, extract_request);
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
        const cfg = parsed_cfg.generator;
        if (cfg.provider != .antfly) return error.BatchIncompatible;
        if (cfg.project_id != null or cfg.location != null or cfg.credentials_path != null) return error.BatchIncompatible;
        if (cfg.tools_json != null or cfg.tool_choice_json != null or parsed_cfg.tool_output != .content) return error.BatchIncompatible;
        if (cfg.url.len == 0) {
            const local = self.antfly_provider orelse return error.BatchIncompatible;
            if (local.generate_messages_with_attachments == null) return error.BatchIncompatible;
            const outputs = try self.produceBatchSequential(alloc, requests);
            return try asset_producer.producedBatchFromOutputs(
                alloc,
                requests,
                outputs,
                inference_work.ExecutionReport.serial(requests.len),
            );
        }

        const capabilities = (try capabilitiesForRequests(self, alloc, requests)) orelse
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
        var header_storage: [1][2][]const u8 = undefined;
        const headers: []const [2][]const u8 = if (token) |value| auth: {
            auth_value = try std.fmt.allocPrint(alloc, "Bearer {s}", .{value});
            header_storage[0] = .{ "Authorization", auth_value.? };
            break :auth &header_storage;
        } else &.{};

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
            const attachment_transport: inference_work.AttachmentTransport = .base64_payload;
            const end = try generatorBatchEnd(alloc, capabilities, attachment_transport, requests, start);
            const chunk = requests[start..end];
            try validateGeneratorInvocation(alloc, capabilities, attachment_transport, chunk);
            const body = try antflyGenerateBatchRequestJsonAlloc(alloc, cfg, chunk);
            defer alloc.free(body);
            var resp = try self.http.post(batch_url, .{ .json = body, .headers = headers, .timeout_ms = 300_000 });
            defer resp.deinit();
            if (!resp.ok()) return mapAntflyGenerateBatchStatus(resp.status.code);
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
        if (!isLocalTranscriberProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl())) return error.BatchIncompatible;
        const local = self.antfly_provider orelse return error.BatchIncompatible;
        const transcribe_audio = local.transcribe_audio orelse return error.BatchIncompatible;

        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            var result = try transcribe_audio(local.ptr, alloc, cfg_parsed.value.model orelse "", .{
                .url = request.source_text,
                .language = cfg_parsed.value.language_code,
            });
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
            if (image_counts[i] == 0) return error.BatchIncompatible;
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
                encodedReaderBatchEnd(
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
                })
            else blk: {
                break :blk try self.readImagesWithConfigReported(alloc, cfg_parsed.value, .{
                    .images = flat_images.items[image_offset..image_end],
                    .prompt = shared_prompt,
                    .max_tokens = cfg_parsed.value.max_tokens,
                    .inline_content_trust = if (requests[0].inline_media_trusted) .trusted_internal else .untrusted,
                    .source_fingerprint = chunk_source_fingerprint,
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
        return try asset_producer.producedBatchFromOutputs(alloc, requests, out, reader_execution);
    }

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var parsed_cfg = try parseGeneratorProducerConfig(alloc, request.config_json);
        defer parsed_cfg.deinit(alloc);
        const cfg = parsed_cfg.generator;
        if (request.media.len > 0 and !request.inline_media_trusted) return error.UntrustedInlineMedia;
        for (request.media) |media| try validateEncodedMedia(media);
        const local_attachments = cfg.provider == .antfly and cfg.url.len == 0 and
            self.antfly_provider != null and
            self.antfly_provider.?.generate_messages_with_attachments != null and
            request.media.len > 0;
        var parts: ?[]generating_runtime.ContentPart = null;
        defer if (parts) |items| freeGeneratorContentParts(alloc, items);
        const content: generating_runtime.ChatMessageContent = if (request.source_parts_json != null or request.media.len > 0) blk: {
            const raw_parts = request.source_parts_json orelse "[]";
            parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts, request.media, !local_attachments);
            break :blk .{ .parts = parts.? };
        } else .{ .text = request.source_text };
        const messages = [_]generating_runtime.ChatMessage{.{ .role = .user, .content = content }};
        if (local_attachments) {
            const local = self.antfly_provider.?;
            const resolve_capabilities = local.model_capabilities orelse return error.InvalidInferenceCapabilities;
            const capabilities = try resolve_capabilities(local.ptr, alloc, cfg.model, .generate);
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
            const result = try self.antfly_provider.?.generate_messages_with_attachments.?(
                self.antfly_provider.?.ptr,
                alloc,
                cfg.model,
                &messages,
                attachments,
            );
            return result;
        }
        const link = generating_runtime.ChainLink{ .generator = cfg };
        var result = try generating_runtime.executeChainWithOptions(alloc, self.http, &.{link}, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
        }, &messages);
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
            });
        } else try self.readImagesWithConfig(alloc, cfg_parsed.value, .{
            .images = source.images,
            .prompt = source.prompt orelse cfg_parsed.value.prompt,
            .max_tokens = cfg_parsed.value.max_tokens,
            .inline_content_trust = if (request.inline_media_trusted) .trusted_internal else .untrusted,
            .source_fingerprint = request.source_fingerprint,
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
        const local_reader = isLocalReaderProvider(cfg.provider, cfg.resolvedUrl());
        if (try self.readerCapabilities(alloc, cfg)) |capabilities| {
            const inline_shape = try readerUriInvocationShape(capabilities, request);
            try capabilities.validateInvocation(.read, .{
                .item_count = request.images.len,
                .modalities = .{ .image = true },
                .encoded_media_bytes = inline_shape.encoded_media_bytes,
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
            const read_images = local.read_images orelse return error.UnsupportedReaderProvider;
            const items = try read_images(local.ptr, alloc, cfg.model orelse "", request);
            return .{ .items = items, .execution = .{ .requested_items = items.len, .serial_items = items.len } };
        }

        var registry = readers.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg);

        var runtime = readers.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        const provider = try runtime.get("asset");
        return try provider.readReported(alloc, request);
    }

    const ReaderUriInvocationShape = struct {
        encoded_media_bytes: usize = 0,
    };

    /// Inspect inline payloads before selecting a local callback or remote
    /// adapter. Network URLs remain provider-owned until download, where the
    /// inference server applies its own byte/MIME limits.
    fn readerUriInvocationShape(
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
        }
        return shape;
    }

    fn readEncodedImagesWithConfig(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.EncodedRequest) ![]readers.Result {
        return (try self.readEncodedImagesWithConfigReported(alloc, cfg, request)).items;
    }

    fn readEncodedImagesWithConfigReported(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.EncodedRequest) !readers.BatchResult {
        try readers.validateEncodedRequest(request);
        const local_reader = isLocalReaderProvider(cfg.provider, cfg.resolvedUrl());
        const capabilities = try self.readerCapabilities(alloc, cfg);
        if (capabilities) |resolved| {
            var encoded_bytes: usize = 0;
            const transport: inference_work.AttachmentTransport = if (local_reader)
                .borrowed_binary
            else
                .data_uri;
            for (request.images) |image| {
                try resolved.validateMimeType(image.mime_type);
                const resident = try transport.wireSize(image.bytes.len, image.mime_type.len);
                encoded_bytes = std.math.add(usize, encoded_bytes, resident) catch
                    return error.InferenceEncodedBytesExceeded;
            }
            try resolved.validateInvocation(.read, .{
                .item_count = request.images.len,
                .modalities = .{ .image = true },
                .encoded_media_bytes = encoded_bytes,
                .max_media_parts_per_item = if (request.images.len > 0) 1 else 0,
            });
        } else if (local_reader) {
            return error.InvalidInferenceCapabilities;
        }
        if (local_reader) {
            const local = self.antfly_provider orelse return error.UnsupportedReaderProvider;
            if (local.read_encoded_images_reported) |read_reported|
                return try read_reported(local.ptr, alloc, cfg.model orelse "", request);
            if (local.read_encoded_images) |read_encoded_images| {
                const items = try read_encoded_images(local.ptr, alloc, cfg.model orelse "", request);
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
        var adapted = try self.readImagesWithConfigReported(alloc, cfg, .{
            .images = urls,
            .prompt = request.prompt,
            .max_tokens = request.max_tokens,
            .inline_content_trust = .trusted_internal,
            .source_fingerprint = request.source_fingerprint,
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

    fn transcribe(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        if (isLocalTranscriberProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl())) {
            const local = self.antfly_provider orelse return error.UnsupportedTranscriberProvider;
            const transcribe_audio = local.transcribe_audio orelse return error.UnsupportedTranscriberProvider;
            var result = try transcribe_audio(local.ptr, alloc, cfg_parsed.value.model orelse "", .{
                .url = request.source_text,
                .language = cfg_parsed.value.language_code,
            });
            defer transcribing.deinitResponse(alloc, &result);

            if (isJsonContentType(request.content_type)) {
                return try std.json.Stringify.valueAlloc(alloc, result, .{});
            }
            return try alloc.dupe(u8, result.text orelse "");
        }

        var registry = transcribing.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = transcribing.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        const provider = try runtime.get("asset");
        var result = try provider.transcribe(alloc, .{ .url = request.source_text });
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

        if (try self.extractorCapabilities(alloc, cfg)) |capabilities| {
            try validateExtractorInvocation(
                alloc,
                capabilities,
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
        };

        var response = if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.UnsupportedExtractionProvider;
            const extract_fn = local.extract orelse return error.UnsupportedExtractionProvider;
            break :blk try extract_fn(local.ptr, alloc, cfg.model, extract_request);
        } else try extracting.extractWithConfig(alloc, self.http, cfg, extract_request);
        defer response.deinit();

        if (isJsonContentType(request.content_type) or request.content_type.len == 0) {
            return try extracting.firstResultJsonAlloc(alloc, response.json);
        }
        return try alloc.dupe(u8, response.json);
    }
};

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
    const provider_value = value.object.get("provider") orelse return error.InvalidGeneratorConfig;
    if (provider_value != .string) return error.InvalidGeneratorConfig;
    const provider = generatorProviderFromString(provider_value.string) orelse return error.UnsupportedGeneratorProvider;

    const model = jsonStringField(value, "model") orelse "";
    const url = jsonStringField(value, "url") orelse jsonStringField(value, "api_url") orelse "";
    var cfg = generating_runtime.GeneratorConfig{
        .provider = provider,
        .model = if (model.len > 0) try alloc.dupe(u8, model) else "",
        .url = if (url.len > 0) try alloc.dupe(u8, url) else "",
        .api_key = if (jsonStringField(value, "api_key")) |text| try alloc.dupe(u8, text) else null,
        .project_id = if (jsonStringField(value, "project_id")) |text| try alloc.dupe(u8, text) else null,
        .location = if (jsonStringField(value, "location")) |text| try alloc.dupe(u8, text) else null,
        .credentials_path = if (jsonStringField(value, "credentials_path")) |text| try alloc.dupe(u8, text) else null,
        .tools_json = if (value.object.get("tools")) |tools| try std.json.Stringify.valueAlloc(alloc, tools, .{}) else null,
        .tool_choice_json = if (value.object.get("tool_choice")) |tool_choice| try std.json.Stringify.valueAlloc(alloc, tool_choice, .{}) else null,
        .max_tokens = jsonIntegerField(value, "max_tokens") orelse generating_runtime.default_max_tokens,
        .temperature = jsonFloatField(value, "temperature"),
        .top_p = jsonFloatField(value, "top_p"),
        .top_k = jsonIntegerField(value, "top_k"),
        .frequency_penalty = jsonFloatField(value, "frequency_penalty"),
        .presence_penalty = jsonFloatField(value, "presence_penalty"),
    };
    errdefer cfg.deinit(alloc);
    try cfg.validate();
    return cfg;
}

fn generatorProviderFromString(value: []const u8) ?generating_runtime.Provider {
    if (std.mem.eql(u8, value, "gemini")) return .gemini;
    if (std.mem.eql(u8, value, "vertex")) return .vertex;
    if (std.mem.eql(u8, value, "openai")) return .openai;
    if (std.mem.eql(u8, value, "ollama")) return .ollama;
    if (std.mem.eql(u8, value, "antfly")) return .antfly;
    if (std.mem.eql(u8, value, "mock")) return .mock;
    return null;
}

fn jsonStringField(value: std.json.Value, field: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return if (found == .string) found.string else null;
}

fn jsonIntegerField(value: std.json.Value, field: []const u8) ?i64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .integer => |integer| integer,
        else => null,
    };
}

fn jsonFloatField(value: std.json.Value, field: []const u8) ?f32 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .float => |float| @floatCast(float),
        .integer => |integer| @floatFromInt(integer),
        else => null,
    };
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

fn isLocalExtractionProvider(provider: extracting.Provider, url: ?[]const u8) bool {
    return provider == .antfly and url == null;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn readerResultMatchesImageIdentity(result: readers.Result, image: readers.EncodedImage) bool {
    return std.mem.eql(u8, result.item_id, image.item_id) and
        optionalStringsEqual(result.source_fingerprint, image.source_fingerprint) and
        result.page_number == image.page_number;
}

/// Mirrors the native Florence chunk policy at the caller boundary. Keeping
/// the exact environment contract here prevents an outer 64-image invocation
/// from being silently repartitioned into differently attributed inner
/// chunks. The server retains the same hard ceiling independently.
fn localReaderBatchMaxImages() usize {
    const configured = platform.env.getenvUsize("ANTFLY_INFERENCE_READ_BATCH_SIZE") orelse
        return default_local_reader_batch_images;
    return std.math.clamp(configured, 1, local_reader_batch_ceiling);
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
) usize {
    const item_end = @min(start +| capabilities.max_items, images.len);
    const max_encoded_media_bytes = capabilities.max_encoded_media_bytes orelse return item_end;
    var end = start;
    var bytes: usize = 0;
    while (end < item_end) : (end += 1) {
        const resident = transport.wireSize(
            images[end].bytes.len,
            images[end].mime_type.len,
        ) catch break;
        const next = std.math.add(usize, bytes, resident) catch break;
        if (next > max_encoded_media_bytes and end > start) break;
        bytes = next;
    }
    return @max(start + 1, end);
}

const GeneratorItemShape = struct {
    modalities: inference_work.Modalities = .{},
    encoded_media_bytes: usize = 0,
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
    if (std.ascii.startsWithIgnoreCase(mime_type, "image/")) return .{ .image = true };
    if (std.ascii.startsWithIgnoreCase(mime_type, "audio/")) return .{ .audio = true };
    if (std.ascii.eqlIgnoreCase(mime_type, "text/plain")) return .{ .text = true };
    if (std.ascii.eqlIgnoreCase(mime_type, "application/pdf")) return .{ .document = true };
    return error.UnsupportedInferenceMimeType;
}

fn validateEncodedMedia(media: asset_producer.EncodedMedia) !void {
    if (media.bytes.len == 0) return error.InvalidInferenceMedia;
    if (media.mime_type.len == 0) return error.UnsupportedInferenceMimeType;
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

fn extractorAttachmentTransport(cfg: extracting.Config) inference_work.AttachmentTransport {
    return if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl()))
        .borrowed_binary
    else
        .base64_payload;
}

fn generatorRequestShape(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    request: asset_producer.Request,
) !GeneratorItemShape {
    var shape = GeneratorItemShape{};
    if (request.source_parts_json) |raw_parts| {
        const parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts, &.{}, false);
        defer freeGeneratorContentParts(alloc, parts);
        for (parts) |part| switch (part) {
            .text => {
                shape.modalities.text = true;
                try capabilities.validateMimeType("text/plain");
            },
            .image_url => |image| {
                shape.modalities.image = true;
                shape.media_parts += 1;
                if (dataUriMimeType(image.url)) |mime_type| try capabilities.validateMimeType(mime_type);
                if (try dataUriResidentSize(image.url)) |media_bytes| {
                    shape.encoded_media_bytes = std.math.add(usize, shape.encoded_media_bytes, media_bytes) catch
                        return error.InferenceEncodedBytesExceeded;
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
            },
        };
    } else {
        shape.modalities.text = true;
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
        const item = try generatorRequestShape(alloc, capabilities, attachment_transport, request);
        mergeInferenceModalities(&invocation.modalities, item.modalities);
        invocation.encoded_media_bytes = std.math.add(usize, invocation.encoded_media_bytes, item.encoded_media_bytes) catch
            return error.InferenceEncodedBytesExceeded;
        invocation.max_media_parts_per_item = @max(invocation.max_media_parts_per_item, item.media_parts);
    }
    try capabilities.validateInvocation(.generate, invocation);
}

const ExtractorItemShape = struct {
    modalities: inference_work.Modalities = .{},
    encoded_media_bytes: usize = 0,
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
}

fn extractorRequestShape(
    alloc: Allocator,
    capabilities: inference_work.InferenceCapabilities,
    attachment_transport: inference_work.AttachmentTransport,
    request: asset_producer.Request,
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
                try addExtractorMediaShape(
                    capabilities,
                    request,
                    &shape,
                    dataUriMimeType(url),
                    try dataUriResidentSize(url),
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
                    try addExtractorMediaShape(
                        capabilities,
                        request,
                        &shape,
                        declared_mime orelse inferred_mime,
                        try dataUriResidentSize(url_value.string),
                        inferred_mime != null,
                    );
                } else if (part.object.get("data")) |data_value| {
                    if (data_value != .string) return error.InvalidExtractionContent;
                    const inferred_mime = dataUriMimeType(data_value.string);
                    try addExtractorMediaShape(
                        capabilities,
                        request,
                        &shape,
                        declared_mime orelse inferred_mime,
                        try inlineBase64ResidentSize(data_value.string),
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
        try addExtractorMediaShape(
            capabilities,
            request,
            &shape,
            media.mime_type,
            try attachment_transport.wireSize(media.bytes.len, media.mime_type.len),
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
        var item = try extractorRequestShape(alloc, capabilities, attachment_transport, request);
        defer item.deinit(alloc);
        const item_uses_media = item.media_parts > 0;
        if (uses_media) |expected| {
            if (expected != item_uses_media) return error.BatchIncompatible;
        } else uses_media = item_uses_media;
        mergeInferenceModalities(&invocation.modalities, item.modalities);
        invocation.max_media_parts_per_item = @max(invocation.max_media_parts_per_item, item.media_parts);
        invocation.encoded_media_bytes = std.math.add(usize, invocation.encoded_media_bytes, item.encoded_media_bytes) catch
            return error.InferenceEncodedBytesExceeded;
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
        var item = try extractorRequestShape(alloc, capabilities, attachment_transport, request);
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
    const data_uri = "data:image/png;base64,AQID";
    const capabilities = inference_work.InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .image = true },
        .accepted_mime_types = .{ .text_plain = true, .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = data_uri.len + 1, .max_media_parts_per_item = 1 },
        .output = .generated_text,
    };
    const requests = [_]asset_producer.Request{
        .{ .producer_type = .generator, .config_json = "{}", .source_text = "", .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,AQID\"}}]" },
        .{ .producer_type = .generator, .config_json = "{}", .source_text = "", .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"image_url\",\"image_url\":{\"url\":\"data:image/png;base64,BAUG\"}}]" },
    };
    try std.testing.expectEqual(@as(usize, 1), try generatorBatchEnd(std.testing.allocator, capabilities, .base64_payload, &requests, 0));
    try validateGeneratorInvocation(std.testing.allocator, capabilities, .base64_payload, requests[0..1]);
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        validateGeneratorInvocation(std.testing.allocator, capabilities, .base64_payload, &requests),
    );
}

test "asset producer runtime media accounting follows attachment transport" {
    const bytes = [_]u8{ 1, 2, 3 };
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
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 2, .max_encoded_media_bytes = 4, .max_media_parts_per_item = 1 },
        .output = .generated_text,
        .borrowed_attachments = false,
    };
    const encoded = try generatorRequestShape(std.testing.allocator, capabilities, .base64_payload, request);
    try std.testing.expectEqual(@as(usize, 4), encoded.encoded_media_bytes);

    const borrowed = try generatorRequestShape(std.testing.allocator, capabilities, .borrowed_binary, request);
    try std.testing.expectEqual(@as(usize, 3), borrowed.encoded_media_bytes);

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

    var extractor_borrowed = try extractorRequestShape(std.testing.allocator, extractor_capabilities, .borrowed_binary, extractor_request);
    defer extractor_borrowed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), extractor_borrowed.encoded_media_bytes);
    var extractor_encoded = try extractorRequestShape(std.testing.allocator, extractor_capabilities, .base64_payload, extractor_request);
    defer extractor_encoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), extractor_encoded.encoded_media_bytes);
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
        "image/png",
        dataUriMimeType("data:image/png;charset=binary;base64,AQID").?,
    );
}

test "asset producer runtime reader URI admission measures data payloads before execution" {
    const capabilities = inference_work.InferenceCapabilities{
        .task = .read,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{ .mode = .native, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = 2, .max_media_parts_per_item = 1 },
        .output = .read_result,
    };
    const request = readers.Request{
        .images = &.{"data:image/png;base64,AQID"},
        .inline_content_trust = .trusted_internal,
    };
    const shape = try Runtime.readerUriInvocationShape(capabilities, request);
    try std.testing.expectEqual(request.images[0].len, shape.encoded_media_bytes);
    try std.testing.expectError(error.InferenceEncodedBytesExceeded, capabilities.validateInvocation(.read, .{
        .item_count = 1,
        .modalities = .{ .image = true },
        .encoded_media_bytes = shape.encoded_media_bytes,
        .max_media_parts_per_item = 1,
    }));
    try std.testing.expectError(
        error.InvalidDataURI,
        Runtime.readerUriInvocationShape(capabilities, .{
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
    const max_encoded_media_bytes = capabilities.batch.max_encoded_media_bytes orelse return item_end;
    var end = start;
    var bytes: usize = 0;
    while (end < item_end) : (end += 1) {
        const item = try generatorRequestShape(alloc, capabilities, attachment_transport, requests[end]);
        if (item.encoded_media_bytes > max_encoded_media_bytes)
            return error.InferenceEncodedBytesExceeded;
        const next = std.math.add(usize, bytes, item.encoded_media_bytes) catch break;
        if (next > max_encoded_media_bytes and end > start) break;
        bytes = next;
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
    const max_encoded_media_bytes = capabilities.batch.max_encoded_media_bytes orelse return item_end;
    var end = start;
    var bytes: usize = 0;
    while (end < item_end) : (end += 1) {
        var item = try extractorRequestShape(alloc, capabilities, attachment_transport, requests[end]);
        defer item.deinit(alloc);
        if (item.encoded_media_bytes > max_encoded_media_bytes) return error.InferenceEncodedBytesExceeded;
        const next = std.math.add(usize, bytes, item.encoded_media_bytes) catch return error.InferenceEncodedBytesExceeded;
        if (next > max_encoded_media_bytes) break;
        bytes = next;
    }
    return @max(start + 1, end);
}

test "asset producer runtime extractor windows obey resolved item and encoded-byte ceilings" {
    const bytes = [_]u8{ 1, 2, 3 };
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
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 2, .max_items = 2, .max_encoded_media_bytes = 5, .max_media_parts_per_item = 1 },
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
    const first_uri = "data:image/png;base64,AQID";
    const second_uri = "data:image/png;base64,BAUG";
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
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "", .content_type = "application/json", .inline_media_trusted = true, .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,AQID\"}]" },
        .{ .producer_type = .extractor, .config_json = "{}", .source_text = "", .content_type = "application/json", .inline_media_trusted = true, .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,BAUG\"}]" },
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
                .batch = .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 1, .max_encoded_media_bytes = 16, .max_media_parts_per_item = 1 },
                .output = .extraction,
                .prompt_policy = .structured_schema,
            };
            var shape = try extractorRequestShape(alloc, capabilities, .base64_payload, .{
                .producer_type = .extractor,
                .config_json = "{}",
                .source_text = "",
                .inline_media_trusted = true,
                .source_parts_json = "[{\"type\":\"text\",\"text\":\"ocr\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,AQID\"}]",
            });
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
    const bytes = [_]u8{ 1, 2, 3 };
    const images = [_]readers.EncodedImage{
        .{ .bytes = &bytes, .mime_type = "image/png" },
        .{ .bytes = &bytes, .mime_type = "image/png" },
        .{ .bytes = &bytes, .mime_type = "image/png" },
    };
    try std.testing.expectEqual(@as(usize, 1), encodedReaderBatchEnd(&images, 0, .{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 2,
        .max_encoded_media_bytes = 5,
    }, .borrowed_binary));
    try std.testing.expectEqual(@as(usize, 2), encodedReaderBatchEnd(&images, 0, .{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 2,
        .max_encoded_media_bytes = 6,
    }, .borrowed_binary));
    const data_uri_bytes = try inference_work.AttachmentTransport.data_uri.wireSize(
        bytes.len,
        "image/png".len,
    );
    try std.testing.expectEqual(@as(usize, 1), encodedReaderBatchEnd(&images, 0, .{
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

fn encodeReaderResults(alloc: Allocator, content_type: []const u8, results: []const readers.Result) ![]u8 {
    if (isJsonContentType(content_type)) {
        return try std.json.Stringify.valueAlloc(alloc, results, .{});
    }

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (results, 0..) |result, i| {
        if (i > 0) try out.append(alloc, '\n');
        try out.appendSlice(alloc, result.text);
    }
    return try out.toOwnedSlice(alloc);
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

fn antflyGenerateBatchRequestJsonAlloc(
    alloc: Allocator,
    cfg: generating_runtime.GeneratorConfig,
    requests: []const asset_producer.Request,
) ![]u8 {
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
    var exact_size: usize = outer_prefix.len + "]}".len;
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
                try Helper.addSize(&exact_size, ",\"data\":\"".len + "\"}".len);
                try Helper.addSize(&exact_size, std.base64.standard.Encoder.calcSize(media.bytes.len));
                emitted += 1;
            }
        }
        try Helper.addSize(&exact_size, item_suffix.len + options.items.len + "}}".len);
    }

    var output: std.Io.Writer.Allocating = try .initCapacity(alloc, exact_size);
    defer output.deinit();
    try output.writer.writeAll(outer_prefix);
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
                try Helper.writeBase64String(&output.writer, media.bytes);
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
    return body;
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
            const body = try antflyGenerateBatchRequestJsonAlloc(alloc, .{
                .provider = .antfly,
                .model = "gemma\"4",
                .url = "http://inference.invalid",
                .max_tokens = 32,
                .temperature = 0.25,
            }, &requests);
            defer alloc.free(body);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
            defer parsed.deinit();
            const batch = parsed.value.object.get("requests").?.array.items;
            try std.testing.expectEqual(@as(usize, 2), batch.len);
            const content = batch[0].object.get("body").?.object.get("messages").?.array.items[0].object.get("content").?.array.items;
            try std.testing.expectEqual(@as(usize, 2), content.len);
            try std.testing.expectEqualStrings("AQID", content[1].object.get("data").?.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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
    };

    var local = Local{};
    const runtime = try Runtime.createOwned(alloc, io_impl.io(), .{ .antfly_provider = local.provider() });
    const producer = runtime.ownedProducer();
    defer producer.deinit(alloc);

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
    const png = [_]u8{ 1, 2, 3 };
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
            try std.testing.expectEqualStrings("data:image/png;base64,YWFh", request.images[0]);
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
        .source_parts_json = "[{\"type\":\"text\",\"text\":\"extract\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,YWFh\"}]",
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
            try std.testing.expectEqualStrings("data:image/png;base64,YWFh", request.images[0]);
            try std.testing.expectEqualStrings("data:image/png;base64,YmJi", request.images[1]);
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
            .source_text = "data:image/png;base64,YWFh",
            .content_type = "text/plain",
            .inline_media_trusted = true,
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "data:image/png;base64,YmJi",
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
                const expected_suffix: u8 = @intCast(i + 1);
                try std.testing.expectEqualSlices(u8, &.{ 0x89, 0x50, 0x4e, 0x47, expected_suffix }, image.bytes);
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

    const first_png = [_]u8{ 0x89, 0x50, 0x4e, 0x47, 1 };
    const second_png = [_]u8{ 0x89, 0x50, 0x4e, 0x47, 2 };
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
            .media = &.{.{ .bytes = &first_png, .mime_type = "image/png" }},
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "",
            .source_parts_json = "[{\"type\":\"text\",\"text\":\"<OCR>\"}]",
            .content_type = "text/plain",
            .inline_media_trusted = true,
            .source_fingerprint = "second-source",
            .media = &.{.{ .bytes = &second_png, .mime_type = "image/png" }},
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
        urls[i] = try alloc.dupe(u8, "data:image/png;base64,AQ==");
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
    try std.testing.expect(!(try producer.canProduceBatch(alloc, &requests)));

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
