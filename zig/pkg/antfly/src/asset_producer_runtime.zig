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

const Allocator = std.mem.Allocator;
const local_reader_batch_max_images: usize = 64;

pub const Runtime = struct {
    alloc: Allocator,
    http: *httpx.Client,
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
            .antfly_provider = options.antfly_provider,
            .secret_store = options.secret_store,
        };
    }

    pub fn createOwned(alloc: Allocator, io: std.Io, options: Options) !*Runtime {
        const runtime = try alloc.create(Runtime);
        errdefer alloc.destroy(runtime);

        const client = try alloc.create(httpx.Client);
        errdefer alloc.destroy(client);
        client.* = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        errdefer client.deinit();

        runtime.* = Runtime.initWithOptions(alloc, client, options);
        runtime.owned_http = client;
        return runtime;
    }

    pub fn deinit(self: *Runtime) void {
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
            .vtable = &.{ .produce = produce, .produce_batch = produceBatch },
        };
    }

    pub fn ownedProducer(self: *Runtime) asset_producer.Producer {
        return .{
            .ptr = self,
            .vtable = &.{ .produce = produce, .produce_batch = produceBatch, .deinit = deinitProducer },
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
        for (requests) |request| {
            if (request.producer_type != .extractor) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
        }

        var cfg = try extracting.parseConfigFromSlice(alloc, requests[0].config_json);
        defer cfg.deinit(alloc);

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
            .schema_json = cfg.schema_json,
            .options_json = cfg.options_json,
        };
        var response = if (isLocalExtractionProvider(cfg.provider, cfg.resolvedUrl())) blk: {
            const local = self.antfly_provider orelse return error.BatchIncompatible;
            const extract_fn = local.extract orelse return error.BatchIncompatible;
            break :blk try extract_fn(local.ptr, alloc, cfg.model, extract_request);
        } else try extracting.extractWithConfig(alloc, self.http, cfg, extract_request);
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

    fn tryGenerateBatch(self: *Runtime, alloc: Allocator, requests: []const asset_producer.Request) ![][]u8 {
        for (requests) |request| {
            if (request.producer_type != .generator) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
            if (request.source_parts_json) |raw_parts| {
                if (raw_parts.len > 0) return error.BatchIncompatible;
            }
        }

        var parsed_cfg = try parseGeneratorProducerConfig(alloc, requests[0].config_json);
        defer parsed_cfg.deinit(alloc);
        const cfg = parsed_cfg.generator;
        if (cfg.provider != .antfly or cfg.url.len == 0) return error.BatchIncompatible;
        if (cfg.api_key != null or cfg.project_id != null or cfg.location != null or cfg.credentials_path != null) return error.BatchIncompatible;
        if (cfg.tools_json != null or cfg.tool_choice_json != null or parsed_cfg.tool_output != .content) return error.BatchIncompatible;

        const batch_url = try antflyGenerateBatchUrlAlloc(alloc, cfg.url);
        defer alloc.free(batch_url);
        const body = try antflyGenerateBatchRequestJsonAlloc(alloc, cfg, requests);
        defer alloc.free(body);

        var resp = try self.http.post(batch_url, .{ .json = body, .timeout_ms = 300_000 });
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
        for (requests) |request| {
            if (request.producer_type != .reader) return error.BatchIncompatible;
            if (!std.mem.eql(u8, request.config_json, requests[0].config_json)) return error.BatchIncompatible;
        }

        var cfg_parsed = try std.json.parseFromSlice(readers.Config, alloc, requests[0].config_json, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer cfg_parsed.deinit();
        if (!isLocalReaderProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl())) return error.BatchIncompatible;
        const local = self.antfly_provider orelse return error.BatchIncompatible;
        const read_images = local.read_images orelse return error.BatchIncompatible;

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

        var shared_prompt: ?[]const u8 = null;
        for (requests, 0..) |request, i| {
            sources[i] = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
            sources_filled += 1;
            if (!optionalStringsEqual(shared_prompt, sources[i].prompt)) {
                if (i == 0) {
                    shared_prompt = sources[i].prompt;
                } else {
                    return error.BatchIncompatible;
                }
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
            const chunk_results = try read_images(local.ptr, alloc, cfg_parsed.value.model orelse "", .{
                .images = chunk_images,
                .prompt = shared_prompt,
                .max_tokens = cfg_parsed.value.max_tokens,
            });
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

    fn generate(self: *Runtime, alloc: Allocator, request: asset_producer.Request) ![]u8 {
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
        const link = generating_runtime.ChainLink{ .generator = cfg };
        var result = try generating_runtime.executeChainWithOptions(alloc, self.http, &.{link}, .{
            .antfly_provider = self.antfly_provider,
            .secret_store = self.secret_store,
        }, &.{
            .{ .role = .user, .content = content },
        });
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

        var source = try parseReaderSource(alloc, request.source_text, request.source_parts_json);
        defer source.deinit(alloc);

        if (isLocalReaderProvider(cfg_parsed.value.provider, cfg_parsed.value.resolvedUrl())) {
            const local = self.antfly_provider orelse return error.UnsupportedReaderProvider;
            const read_images = local.read_images orelse return error.UnsupportedReaderProvider;
            const results = try read_images(local.ptr, alloc, cfg_parsed.value.model orelse "", .{
                .images = source.images,
                .prompt = source.prompt,
                .max_tokens = cfg_parsed.value.max_tokens,
            });
            defer {
                for (results) |*result| readers.deinitResult(alloc, result);
                alloc.free(results);
            }
            return try encodeReaderResults(alloc, request.content_type, results);
        }

        var registry = readers.Registry.init(alloc);
        defer registry.deinit();
        try registry.registerConfig("asset", cfg_parsed.value);

        var runtime = readers.Runtime.init(alloc);
        defer runtime.deinit();
        try runtime.loadFromRegistry(self.http, &registry);

        const provider = try runtime.get("asset");
        const results = try provider.read(alloc, .{
            .images = source.images,
            .prompt = source.prompt,
            .max_tokens = cfg_parsed.value.max_tokens,
        });
        defer {
            for (results) |*result| readers.deinitResult(alloc, result);
            alloc.free(results);
        }

        return try encodeReaderResults(alloc, request.content_type, results);
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
        var cfg = try extracting.parseConfigFromSlice(alloc, request.config_json);
        defer cfg.deinit(alloc);

        const content_json = try extractionContentJsonAlloc(alloc, request.source_text, request.source_parts_json);
        defer alloc.free(content_json);
        const input = extracting.Input{ .content_json = content_json };
        const extract_request = extracting.Request{
            .inputs = &.{input},
            .schema_json = cfg.schema_json,
            .options_json = cfg.options_json,
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
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, "{\"mode\":\"sync\",\"requests\":[");
    for (requests, 0..) |request, i| {
        if (i > 0) try out.append(alloc, ',');
        const item = try std.fmt.allocPrint(
            alloc,
            "{{\"custom_id\":\"{d}\",\"body\":{{\"model\":{f},\"messages\":[{{\"role\":\"user\",\"content\":{f}}}],\"mode\":\"eager\"",
            .{
                i,
                std.json.fmt(cfg.model, .{}),
                std.json.fmt(request.source_text, .{}),
            },
        );
        defer alloc.free(item);
        try out.appendSlice(alloc, item);
        try appendBatchI64Field(alloc, &out, "max_tokens", cfg.max_tokens);
        if (cfg.temperature) |temperature| try appendBatchFloatField(alloc, &out, "temperature", temperature);
        if (cfg.top_p) |top_p| try appendBatchFloatField(alloc, &out, "top_p", top_p);
        if (cfg.top_k) |top_k| try appendBatchI64Field(alloc, &out, "top_k", top_k);
        if (cfg.frequency_penalty) |frequency_penalty| try appendBatchFloatField(alloc, &out, "frequency_penalty", frequency_penalty);
        if (cfg.presence_penalty) |presence_penalty| try appendBatchFloatField(alloc, &out, "presence_penalty", presence_penalty);
        try out.appendSlice(alloc, "}}");
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
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
        },
        .{
            .producer_type = .reader,
            .config_json = "{\"provider\":\"antfly\",\"model\":\"local-reader\"}",
            .source_text = "data:image/png;base64,bbb",
            .content_type = "text/plain",
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

    const results = try producer.produceBatch(alloc, &.{
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
    });
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
