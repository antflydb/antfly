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
const httpx = @import("httpx");
const generating_runtime = @import("generating/mod.zig");
const managed_embedder = @import("inference/managed_embedder.zig");
const common_secrets = @import("common/secrets.zig");
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const extracting = @import("antfly_extracting");
const asset_producer = @import("storage/db/enrichment/asset_producer.zig");
const RequestContext = @import("inference/request_context.zig").RequestContext;

const provider_limits = @import("common/provider_limits.zig");
const Allocator = std.mem.Allocator;
const local_reader_batch_max_images: usize = 64;
const max_asset_provider_timeout_ms: u64 = 300_000;

fn requestHttpClient(alloc: Allocator, context: RequestContext) !httpx.Client {
    const remaining_ms = @min(
        max_asset_provider_timeout_ms,
        (try context.remainingTimeoutMs()) orelse max_asset_provider_timeout_ms,
    );
    var config = httpx.ClientConfig{ .keep_alive = false };
    config.timeouts = httpx.Timeouts.uniform(remaining_ms);
    config.timeouts.request_ms = remaining_ms;
    config.request_cancellation = if (context.cancellation) |token|
        httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn)
    else
        null;
    return httpx.Client.initWithConfig(alloc, context.io, config);
}

pub const Runtime = struct {
    alloc: Allocator,
    http: *httpx.Client,
    owned_http: ?*httpx.Client = null,
    limits: *provider_limits.Registry = &provider_limits.process_registry,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    inference_api_url: ?[]const u8 = null,
    owns_inference_api_url: bool = false,
    secret_store: ?*common_secrets.FileStore = null,

    pub const Options = struct {
        limits: *provider_limits.Registry = &provider_limits.process_registry,
        antfly_provider: ?managed_embedder.AntflyProvider = null,
        /// Process-isolated Antfly inference endpoint. When no local provider
        /// is installed, Antfly asset configs without their own URL inherit
        /// this endpoint instead of falling back to an unrelated localhost
        /// default.
        inference_api_url: ?[]const u8 = null,
        secret_store: ?*common_secrets.FileStore = null,
    };

    pub fn init(alloc: Allocator, http: *httpx.Client) Runtime {
        return initWithOptions(alloc, http, .{});
    }

    pub fn initWithOptions(alloc: Allocator, http: *httpx.Client, options: Options) Runtime {
        return .{
            .alloc = alloc,
            .http = http,
            .limits = options.limits,
            .antfly_provider = options.antfly_provider,
            .inference_api_url = options.inference_api_url,
            .secret_store = options.secret_store,
        };
    }

    pub fn createOwned(alloc: Allocator, io: std.Io, options: Options) !*Runtime {
        const runtime = try alloc.create(Runtime);
        errdefer alloc.destroy(runtime);

        const client = try alloc.create(httpx.Client);
        errdefer alloc.destroy(client);
        var client_config = httpx.ClientConfig{ .keep_alive = false };
        client_config.timeouts = httpx.Timeouts.uniform(max_asset_provider_timeout_ms);
        client_config.timeouts.request_ms = max_asset_provider_timeout_ms;
        client.* = httpx.Client.initWithConfig(alloc, io, client_config);
        errdefer client.deinit();

        const inference_api_url = if (options.inference_api_url) |raw|
            try normalizeAntflyInferenceBaseUrl(alloc, raw)
        else
            null;
        errdefer if (inference_api_url) |owned| alloc.free(owned);

        var owned_options = options;
        owned_options.inference_api_url = inference_api_url;
        runtime.* = Runtime.initWithOptions(alloc, client, owned_options);
        runtime.owned_http = client;
        runtime.owns_inference_api_url = inference_api_url != null;
        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
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
                .can_produce_batch = canProduceBatch,
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
                .can_produce_batch = canProduceBatch,
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

    fn requestForegroundBounded(self: *Runtime, alloc: Allocator, request: asset_producer.Request) !bool {
        return switch (request.producer_type) {
            // These routes never enter an external callback. Unsupported
            // document extraction also fails synchronously in produceOne.
            .copy, .document_extraction => true,
            .generator => blk: {
                var parsed = try parseGeneratorProducerConfig(alloc, request.config_json);
                defer parsed.deinit(alloc);
                const local = self.antfly_provider orelse break :blk true;
                const routes_local = parsed.generator.provider == .antfly and parsed.generator.url.len == 0;
                break :blk !routes_local or
                    local.generate_messages_with_context != null or
                    local.generate_text_with_context != null;
            },
            .reader => blk: {
                var parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const local = self.antfly_provider orelse break :blk true;
                break :blk !isLocalReaderProvider(parsed.value.provider, parsed.value.resolvedUrl()) or
                    local.read_images_with_context != null;
            },
            .transcriber => blk: {
                var parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();
                const local = self.antfly_provider orelse break :blk true;
                break :blk !isLocalTranscriberProvider(parsed.value.provider, parsed.value.resolvedUrl()) or
                    local.transcribe_audio_with_context != null;
            },
            .extractor => blk: {
                var parsed = try extracting.parseConfigFromSlice(alloc, request.config_json);
                defer parsed.deinit(alloc);
                const local = self.antfly_provider orelse break :blk true;
                break :blk !isLocalExtractionProvider(parsed.provider, parsed.resolvedUrl()) or
                    local.extract_with_context != null;
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
        return try self.produceOne(alloc, request, null);
    }

    fn produceWithContext(ptr: *anyopaque, alloc: Allocator, request: asset_producer.Request, context: RequestContext) ![]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        var client = try requestHttpClient(alloc, context);
        defer client.deinit();
        var scoped = self.*;
        scoped.http = &client;
        scoped.owned_http = null;
        scoped.owns_inference_api_url = false;
        return try scoped.produceOne(alloc, request, context);
    }

    fn canProduceBatch(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        if (requests.len == 0) return true;
        const first = requests[0];
        for (requests) |request| {
            if (request.producer_type != first.producer_type) return false;
        }
        return switch (first.producer_type) {
            .copy => true,
            .document_extraction => false,
            .reader => try self.canReadBatch(alloc, requests),
            .generator => try self.canGenerateBatch(alloc, requests),
            // The transcriber interface accepts one audio input per call. Its
            // produceBatch implementation is therefore a compatibility loop,
            // not an atomic provider batch, and must be isolated by callers.
            .transcriber => false,
            .extractor => try self.canExtractBatch(alloc, requests),
        };
    }

    fn requestsShareConfig(requests: []const asset_producer.Request) bool {
        for (requests[1..]) |request| {
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return false;
        }
        return true;
    }

    fn canReadBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        _ = self;
        if (!requestsShareConfig(requests)) return false;
        for (requests[1..]) |request| {
            if (request.inline_media_trusted != requests[0].inline_media_trusted) return false;
        }
        var cfg = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg.deinit();
        if (!readerSupportsPerImageBatch(cfg.value.provider)) return false;

        var shared_prompt: ?[]const u8 = cfg.value.prompt;
        for (requests, 0..) |request, i| {
            var source = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
            defer source.deinit(alloc);
            if (source.images.len == 0) return false;
            const effective_prompt = source.prompt orelse cfg.value.prompt;
            if (i == 0) {
                shared_prompt = effective_prompt;
            } else if (!optionalStringsEqual(shared_prompt, effective_prompt)) {
                return false;
            }
        }
        return true;
    }

    fn canGenerateBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareConfig(requests)) return false;
        for (requests) |request| {
            if (request.source_parts_json) |parts| if (parts.len > 0) return false;
        }
        var parsed = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
        defer parsed.deinit(alloc);
        const cfg = self.effectiveGeneratorConfig(parsed.generator);
        return cfg.provider == .antfly and
            cfg.url.len > 0 and
            cfg.api_key == null and
            cfg.project_id == null and
            cfg.location == null and
            cfg.credentials_path == null and
            cfg.tools_json == null and
            cfg.tool_choice_json == null and
            parsed.tool_output == .content;
    }

    fn canExtractBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) !bool {
        if (!requestsShareConfig(requests)) return false;
        var cfg = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
        defer cfg.deinit(alloc);
        const effective_cfg = self.effectiveExtractorConfig(cfg);
        if (!isLocalExtractionProvider(effective_cfg.provider, effective_cfg.resolvedUrl())) return true;
        const local = self.antfly_provider orelse return false;
        return local.extract != null or local.extract_with_context != null;
    }

    fn produceOne(self: *Runtime, alloc: Allocator, request: asset_producer.Request, context: ?RequestContext) ![]u8 {
        if (context) |active| try active.check();
        return switch (request.producer_type) {
            .copy => try alloc.dupe(u8, request.source_text),
            .document_extraction => error.UnsupportedAssetProducer,
            .generator => try self.generate(alloc, request, context),
            .reader => try self.read(alloc, request, context),
            .transcriber => try self.transcribe(alloc, request, context),
            .extractor => try self.extract(alloc, request, context),
        };
    }

    fn produceBatch(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        return try self.produceBatchImpl(alloc, requests, null);
    }

    fn produceBatchWithContext(ptr: *anyopaque, alloc: Allocator, requests: []const asset_producer.Request, context: RequestContext) ![][]u8 {
        const self: *Runtime = @ptrCast(@alignCast(ptr));
        var client = try requestHttpClient(alloc, context);
        defer client.deinit();
        var scoped = self.*;
        scoped.http = &client;
        scoped.owned_http = null;
        scoped.owns_inference_api_url = false;
        return try scoped.produceBatchImpl(alloc, requests, context);
    }

    fn produceBatchImpl(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request, context: ?RequestContext) ![][]u8 {
        if (context) |active| try active.check();
        if (requests.len == 0) return try alloc.alloc([]u8, 0);

        const first_type = requests[0].producer_type;
        for (requests) |request| {
            if (request.producer_type != first_type) return try self.produceBatchSequential(alloc, requests, context);
        }

        const batch_result = switch (first_type) {
            .copy => self.produceCopyBatch(alloc, requests),
            .reader => self.tryReadBatch(alloc, requests, context),
            .generator => self.tryGenerateBatch(alloc, requests, context),
            .extractor => self.tryExtractBatch(alloc, requests, context),
            .transcriber => self.tryTranscribeBatch(alloc, requests, context),
            .document_extraction => error.BatchIncompatible,
        };
        if (batch_result) |items| {
            return items;
        } else |err| switch (err) {
            error.BatchIncompatible => {},
            else => return err,
        }
        return try self.produceBatchSequential(alloc, requests, context);
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

    fn produceBatchSequential(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request, context: ?RequestContext) ![][]u8 {
        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            if (context) |active| try active.check();
            out[i] = try self.produceOne(alloc, request, context);
        }
        return out;
    }

    fn tryExtractBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request, context: ?RequestContext) ![][]u8 {
        for (requests) |request| {
            if (request.producer_type != .extractor) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
        }

        var cfg = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
        defer cfg.deinit(alloc);
        const effective_cfg = self.effectiveExtractorConfig(cfg);

        const inputs = try alloc.alloc(extracting.Input, requests.len);
        var inputs_filled: usize = 0;
        defer {
            for (inputs[0..inputs_filled]) |input| alloc.free(input.content_json);
            alloc.free(inputs);
        }

        for (requests, 0..) |request, i| {
            inputs[i] = .{
                .content_json = try extractionContentJsonAlloc(alloc, request.source_text, request.source_parts_json),
            };
            inputs_filled += 1;
        }

        const extract_request = extracting.Request{
            .inputs = inputs,
            .schema_json = effective_cfg.schema_json,
            .options_json = effective_cfg.options_json,
        };
        var response = if (isLocalExtractionProvider(effective_cfg.provider, effective_cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.BatchIncompatible;
            if (context) |active| {
                const extract_fn = local.extract_with_context orelse return error.BatchIncompatible;
                break :blk try extract_fn(local.ptr, alloc, effective_cfg.model, extract_request, active);
            }
            const extract_fn = local.extract orelse return error.BatchIncompatible;
            break :blk try extract_fn(local.ptr, alloc, effective_cfg.model, extract_request);
        } else try extracting.extractWithConfig(alloc, self.http, effective_cfg, extract_request);
        defer response.deinit();

        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            out[i] = if (isJsonContentType(request.content_type) or request.content_type.len == 0)
                try extractionResultJsonAtAlloc(alloc, response.json, i)
            else
                try alloc.dupe(u8, response.json);
        }
        return out;
    }

    fn tryGenerateBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request, context: ?RequestContext) ![][]u8 {
        for (requests) |request| {
            if (request.producer_type != .generator) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
            if (request.source_parts_json) |raw_parts| {
                if (raw_parts.len > 0) return error.BatchIncompatible;
            }
        }

        var parsed_cfg = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
        defer parsed_cfg.deinit(alloc);
        const cfg = self.effectiveGeneratorConfig(parsed_cfg.generator);
        if (cfg.provider != .antfly or cfg.url.len == 0) return error.BatchIncompatible;
        if (cfg.api_key != null or cfg.project_id != null or cfg.location != null or cfg.credentials_path != null) return error.BatchIncompatible;
        if (cfg.tools_json != null or cfg.tool_choice_json != null or parsed_cfg.tool_output != .content) return error.BatchIncompatible;

        const texts = try alloc.alloc([]const u8, requests.len);
        defer alloc.free(texts);
        for (requests, 0..) |request, i| texts[i] = request.source_text;
        var resp = try generating_runtime.generateAntflyTextBatchResponse(alloc, self.http, cfg, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
            .limits = self.limits,
            .request_context = context,
        }, texts);
        defer resp.deinit();
        if (!resp.ok()) return mapAntflyGenerateBatchStatus(resp.status.code);
        const payload = resp.body orelse return error.EmptyGenerateBatchResponse;
        return try parseAntflyGenerateBatchResponseAlloc(alloc, payload, requests.len);
    }

    fn mapAntflyGenerateBatchStatus(status: u16) anyerror {
        return switch (status) {
            408, 409, 425, 429 => error.GenerateBatchTransientFailure,
            else => if (status >= 500 and status <= 599) error.GenerateBatchTransientFailure else error.GenerateBatchRequestFailed,
        };
    }

    fn tryTranscribeBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request, context: ?RequestContext) ![][]u8 {
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

        const out = try alloc.alloc([]u8, requests.len);
        errdefer {
            for (out) |item| {
                if (item.len > 0) alloc.free(item);
            }
            alloc.free(out);
        }
        for (out) |*item| item.* = "";
        for (requests, 0..) |request, i| {
            if (context) |active| try active.check();
            const transcribe_request = transcribing.Request{
                .url = request.source_text,
                .language = cfg_parsed.value.language_code,
            };
            var result = if (context) |active| blk: {
                const transcribe_audio = local.transcribe_audio_with_context orelse return error.BatchIncompatible;
                break :blk try transcribe_audio(local.ptr, alloc, cfg_parsed.value.model orelse "", transcribe_request, active);
            } else blk: {
                const transcribe_audio = local.transcribe_audio orelse return error.BatchIncompatible;
                break :blk try transcribe_audio(local.ptr, alloc, cfg_parsed.value.model orelse "", transcribe_request);
            };
            defer transcribing.deinitResponse(alloc, &result);

            out[i] = if (isJsonContentType(request.content_type))
                try std.json.Stringify.valueAlloc(alloc, result, .{})
            else
                try alloc.dupe(u8, result.text orelse "");
        }
        return out;
    }

    fn tryReadBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request, context: ?RequestContext) ![][]u8 {
        for (requests) |request| {
            if (request.producer_type != .reader) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
            // Never collapse external and internally generated media into one
            // request: trust is intentionally carried at the request boundary.
            if (request.inline_media_trusted != requests[0].inline_media_trusted) return error.BatchIncompatible;
        }

        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();
        const effective_cfg = self.effectiveReaderConfig(cfg_parsed.value);
        // The Antfly reader contract returns one independently addressable
        // result per input image. OpenAI and Vertex accept multiple images in
        // one prompt, but produce one response for the prompt as a whole, so
        // flattening requests across those providers loses the request/result
        // boundary. Let the generic batch path execute them sequentially.
        if (!readerSupportsPerImageBatch(effective_cfg.provider)) return error.BatchIncompatible;
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

        var shared_prompt: ?[]const u8 = effective_cfg.prompt;
        for (requests, 0..) |request, i| {
            sources[i] = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
            sources_filled += 1;
            const effective_prompt = sources[i].prompt orelse effective_cfg.prompt;
            if (!optionalStringsEqual(shared_prompt, effective_prompt)) {
                if (i == 0) shared_prompt = effective_prompt else return error.BatchIncompatible;
            }
            image_counts[i] = sources[i].images.len;
            try flat_images.appendSlice(alloc, sources[i].images);
        }
        if (flat_images.items.len == 0) return error.BatchIncompatible;

        const results = try alloc.alloc(readers.Result, flat_images.items.len);
        var results_filled: usize = 0;
        var results_errdefer_active = true;
        errdefer if (results_errdefer_active) {
            for (results[0..results_filled]) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        };
        var image_offset: usize = 0;
        while (image_offset < flat_images.items.len) {
            const image_end = @min(image_offset + local_reader_batch_max_images, flat_images.items.len);
            const chunk_images = flat_images.items[image_offset..image_end];
            if (context) |active| try active.check();
            const chunk_results = try self.readImagesWithConfig(alloc, effective_cfg, .{
                .images = chunk_images,
                .prompt = shared_prompt,
                .max_tokens = effective_cfg.max_tokens,
                .inline_content_trust = if (requests[0].inline_media_trusted) .trusted_internal else .untrusted,
                .source_fingerprint = requests[0].source_fingerprint,
            }, context);
            if (chunk_results.len != chunk_images.len) {
                for (chunk_results) |*result| readers.deinitResult(alloc, result);
                alloc.free(chunk_results);
                return error.InvalidReaderResponse;
            }
            for (chunk_results, 0..) |result, j| {
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
        return out;
    }

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request, context: ?RequestContext) ![]u8 {
        var parsed_cfg = try parseGeneratorProducerConfig(alloc, request.config_json);
        defer parsed_cfg.deinit(alloc);
        const cfg = parsed_cfg.generator;
        var parts: ?[]generating_runtime.ContentPart = null;
        defer if (parts) |items| freeGeneratorContentParts(alloc, items);
        const content: generating_runtime.ChatMessageContent = if (request.source_parts_json) |raw_parts| blk: {
            if (raw_parts.len == 0) break :blk .{ .text = request.source_text };
            parts = try parseGeneratorContentParts(alloc, request.source_text, raw_parts);
            break :blk .{ .parts = parts.? };
        } else .{ .text = request.source_text };
        const effective_cfg = self.effectiveGeneratorConfig(cfg);
        const link = generating_runtime.ChainLink{ .generator = effective_cfg };
        var result = try generating_runtime.executeChainWithOptions(alloc, self.http, &.{link}, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
            .request_context = context,
            .limits = self.limits,
        }, &.{
            .{ .role = .user, .content = content },
        });
        defer result.deinit();
        if (parsed_cfg.tool_output == .arguments) {
            return try toolCallArgumentsOutputAlloc(alloc, result.tool_calls, parsed_cfg.tool_name);
        }
        return try alloc.dupe(u8, result.content);
    }

    fn read(self: *Runtime, alloc: Allocator, request: asset_producer.Request, context: ?RequestContext) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        var source = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
        defer source.deinit(alloc);

        const effective_cfg = self.effectiveReaderConfig(cfg_parsed.value);
        const results = try self.readImagesWithConfig(alloc, effective_cfg, .{
            .images = source.images,
            .prompt = source.prompt orelse effective_cfg.prompt,
            .max_tokens = effective_cfg.max_tokens,
            .inline_content_trust = if (request.inline_media_trusted) .trusted_internal else .untrusted,
            .source_fingerprint = request.source_fingerprint,
        }, context);
        defer {
            for (results) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        }
        return try encodeReaderResults(alloc, request.content_type, results);
    }

    fn readImagesWithConfig(self: *Runtime, alloc: Allocator, cfg: readers.Config, request: readers.Request, context: ?RequestContext) ![]readers.Result {
        if (isLocalReaderProvider(cfg.provider, cfg.resolvedUrl())) {
            const local = self.antfly_provider orelse return error.UnsupportedReaderProvider;
            if (context) |active| {
                const read_images = local.read_images_with_context orelse return error.UncancellableInferenceProvider;
                return try read_images(local.ptr, alloc, cfg.model orelse "", request, active);
            }
            const read_images = local.read_images orelse return error.UnsupportedReaderProvider;
            return try read_images(local.ptr, alloc, cfg.model orelse "", request);
        }

        var registry = readers.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg);

        var runtime = readers.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        const provider = try runtime.get("asset");
        return try provider.read(alloc, request);
    }

    fn transcribe(self: *Runtime, alloc: Allocator, request: asset_producer.Request, context: ?RequestContext) ![]u8 {
        var cfg_parsed = try std.json.parseFromSlice(transcribing.Config, alloc, request.config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();

        const effective_cfg = self.effectiveTranscriberConfig(cfg_parsed.value);
        if (isLocalTranscriberProvider(effective_cfg.provider, effective_cfg.resolvedUrl())) {
            const local = self.antfly_provider orelse return error.UnsupportedTranscriberProvider;
            const transcribe_request = transcribing.Request{
                .url = request.source_text,
                .language = effective_cfg.language_code,
            };
            var result = if (context) |active| blk: {
                const transcribe_audio = local.transcribe_audio_with_context orelse return error.UncancellableInferenceProvider;
                break :blk try transcribe_audio(local.ptr, alloc, effective_cfg.model orelse "", transcribe_request, active);
            } else blk: {
                const transcribe_audio = local.transcribe_audio orelse return error.UnsupportedTranscriberProvider;
                break :blk try transcribe_audio(local.ptr, alloc, effective_cfg.model orelse "", transcribe_request);
            };
            defer transcribing.deinitResponse(alloc, &result);

            if (isJsonContentType(request.content_type)) {
                return try std.json.Stringify.valueAlloc(alloc, result, .{});
            }
            return try alloc.dupe(u8, result.text orelse "");
        }

        var registry = transcribing.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", effective_cfg);

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

    fn extract(self: *Runtime, alloc: Allocator, request: asset_producer.Request, context: ?RequestContext) ![]u8 {
        var cfg = try extracting.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);
        const effective_cfg = self.effectiveExtractorConfig(cfg);

        const content_json = try extractionContentJsonAlloc(alloc, request.source_text, request.source_parts_json);
        defer alloc.free(content_json);
        const input = extracting.Input{ .content_json = content_json };
        const extract_request = extracting.Request{
            .inputs = &.{input},
            .schema_json = effective_cfg.schema_json,
            .options_json = effective_cfg.options_json,
        };

        var response = if (isLocalExtractionProvider(effective_cfg.provider, effective_cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.UnsupportedExtractionProvider;
            if (context) |active| {
                const extract_fn = local.extract_with_context orelse return error.UncancellableInferenceProvider;
                break :blk try extract_fn(local.ptr, alloc, effective_cfg.model, extract_request, active);
            }
            const extract_fn = local.extract orelse return error.UnsupportedExtractionProvider;
            break :blk try extract_fn(local.ptr, alloc, effective_cfg.model, extract_request);
        } else try extracting.extractWithConfig(alloc, self.http, effective_cfg, extract_request);
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

fn readerSupportsPerImageBatch(provider: readers.Provider) bool {
    return provider == .antfly;
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

            if (images.items.len > 0) {
                return .{
                    .images = try images.toOwnedSlice(alloc),
                    .prompt = if (prompt.items.len > 0) try prompt.toOwnedSlice(alloc) else null,
                };
            }
            prompt.deinit(alloc);
            images.deinit(alloc);
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

fn extractionResultJsonAtAlloc(alloc: Allocator, response_json: []const u8, index: usize) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response_json, .{});
    defer parsed.deinit();
    if (parsed.value == .object) {
        if (parsed.value.object.get("data")) |data| {
            if (data == .array) {
                if (index >= data.array.items.len) return error.InvalidExtractorResponse;
                return try std.json.Stringify.valueAlloc(alloc, data.array.items[index], .{});
            }
        }
    }
    if (index == 0) return try alloc.dupe(u8, response_json);
    return error.InvalidExtractorResponse;
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
fn parseAntflyGenerateBatchResponseAlloc(alloc: Allocator, payload: []const u8, count: usize) ![][]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGenerateBatchResponse;
    const data = parsed.value.object.get("data") orelse return error.InvalidGenerateBatchResponse;
    if (data != .array) return error.InvalidGenerateBatchResponse;

    const out = try alloc.alloc([]u8, count);
    errdefer {
        for (out) |item| {
            if (item.len > 0) alloc.free(item);
        }
        alloc.free(out);
    }
    for (out) |*item| item.* = "";
    var seen = try alloc.alloc(bool, count);
    defer alloc.free(seen);
    @memset(seen, false);

    for (data.array.items) |item| {
        if (item != .object) return error.InvalidGenerateBatchResponse;
        const raw_index = item.object.get("index") orelse return error.InvalidGenerateBatchResponse;
        if (raw_index != .integer or raw_index.integer < 0) return error.InvalidGenerateBatchResponse;
        const index: usize = @intCast(raw_index.integer);
        if (index >= count or seen[index]) return error.InvalidGenerateBatchResponse;
        seen[index] = true;

        if (item.object.get("error")) |err_value| {
            if (err_value != .null) return error.GenerateBatchItemFailed;
        }
        const response = item.object.get("response") orelse return error.InvalidGenerateBatchResponse;
        if (response == .null) return error.InvalidGenerateBatchResponse;
        out[index] = try generateResponseContentAlloc(alloc, response);
    }

    for (seen) |was_seen| {
        if (!was_seen) return error.InvalidGenerateBatchResponse;
    }
    return out;
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

fn parseGeneratorContentParts(alloc: Allocator, source_text: []const u8, raw_parts: []const u8) ![]generating_runtime.ContentPart {
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
    const raw =
        \\{"provider":"antfly","model":"test","api_url":"http://127.0.0.1:1","max_tokens":10,"rate_limit":{"tokens_per_minute":1}}
    ;
    var parsed = try parseGeneratorProducerConfig(alloc, raw);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?i64, 1), parsed.generator.rate_limit.?.tokens_per_minute);
    const request = asset_producer.Request{ .producer_type = .generator, .config_json = raw, .source_text = "hello" };
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, runtime.produceOne(alloc, request, null));
    try std.testing.expectError(error.ProviderTokenBudgetExceeded, runtime.tryGenerateBatch(alloc, &.{ request, request }, null));
}

test "asset producer runtime batches compatible antfly generator requests" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/generate/batch", .assert_request = expectAntflyGenerateBatchRequest, .respond = .{
            .body =
            \\{"object":"generate.batch","data":[
            \\{"custom_id":"0","index":0,"response":{"id":"gen-0","object":"chat.completion","created":1,"model":"local-generator","choices":[{"index":0,"message":{"role":"assistant","content":"first answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}},
            \\{"custom_id":"1","index":1,"response":{"id":"gen-1","object":"chat.completion","created":1,"model":"local-generator","choices":[{"index":0,"message":{"role":"assistant","content":"second answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}}
            \\],"summary":{"total":2,"succeeded":2,"failed":0}}
            ,
        } },
    });
    defer server.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.init(alloc, &client);
    const producer = runtime.producer();

    const cfg_json = try std.fmt.allocPrint(alloc, "{{\"provider\":\"antfly\",\"model\":\"local-generator\",\"url\":\"{s}\",\"max_tokens\":24,\"temperature\":0.25,\"top_p\":0.9,\"top_k\":40,\"frequency_penalty\":0.1,\"presence_penalty\":0.2}}", .{server.baseUrl()});
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
            try std.testing.expectEqualStrings("data:image/png;base64,aaa", request.images[0]);
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
    const producer = runtime.producer();

    const result = try producer.produce(alloc, .{
        .producer_type = .reader,
        .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
        .source_text = "",
        .source_parts_json = "[{\"type\":\"text\",\"text\":\"extract\"},{\"type\":\"media\",\"url\":\"data:image/png;base64,aaa\"}]",
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
            try std.testing.expectEqualStrings("data:image/png;base64,aaa", request.images[0]);
            try std.testing.expectEqualStrings("data:image/png;base64,bbb", request.images[1]);
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
    const producer = runtime.producer();

    const results = try producer.produceBatch(alloc, &.{
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "data:image/png;base64,aaa",
            .content_type = "text/plain",
            .inline_media_trusted = true,
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "data:image/png;base64,bbb",
            .content_type = "text/plain",
            .inline_media_trusted = true,
        },
    });
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, 2), results.len);
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
            try std.testing.expect(request.images.len <= local_reader_batch_max_images);
            try std.testing.expect(request.prompt == null);
            if (self.read_calls >= self.batch_lengths.len) return error.TestUnexpectedResult;
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
    const producer = runtime.producer();

    const request_count = local_reader_batch_max_images + 1;
    var urls: [request_count][]u8 = undefined;
    var urls_filled: usize = 0;
    defer {
        for (urls[0..urls_filled]) |url| alloc.free(url);
    }
    var requests: [request_count]asset_producer.Request = undefined;
    for (0..request_count) |i| {
        urls[i] = try std.fmt.allocPrint(alloc, "data:image/png;base64,{d}", .{i});
        urls_filled += 1;
        requests[i] = .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = urls[i],
            .content_type = "text/plain",
        };
    }

    const results = try producer.produceBatch(alloc, &requests);
    defer {
        for (results) |result| alloc.free(result);
        alloc.free(results);
    }

    try std.testing.expectEqual(@as(usize, request_count), results.len);
    try std.testing.expectEqual(@as(usize, 2), local.read_calls);
    try std.testing.expectEqual(@as(usize, local_reader_batch_max_images), local.batch_lengths[0]);
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
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
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
            };
        }

        fn embedDense(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![][]f32 {
            return error.TestUnexpectedResult;
        }

        fn embedSparse(_: *anyopaque, _: Allocator, _: []const u8, _: []const []const u8) ![]@import("storage/db/enrichment/embedder.zig").SparseEmbedding {
            return error.TestUnexpectedResult;
        }

        fn extract(ptr: *anyopaque, a: Allocator, model: []const u8, request: extracting.Request) !extracting.Response {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.extract_calls += 1;
            try std.testing.expectEqualStrings("local-extractor", model);
            try std.testing.expectEqual(@as(usize, 2), request.inputs.len);
            try std.testing.expect(std.mem.indexOf(u8, request.inputs[0].content_json, "Ada") != null);
            try std.testing.expect(std.mem.indexOf(u8, request.inputs[1].content_json, "Grace") != null);
            return .{
                .allocator = a,
                .json = try a.dupe(u8, "{\"object\":\"extraction\",\"model\":\"local-extractor\",\"data\":[{\"entities\":[{\"label\":\"person\",\"text\":\"Ada\"}],\"relations\":[]},{\"entities\":[{\"label\":\"person\",\"text\":\"Grace\"}],\"relations\":[]}]}"),
            };
        }
    };

    var local = Local{};
    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();
    var runtime = Runtime.initWithOptions(alloc, &client, .{ .antfly_provider = local.provider() });
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
