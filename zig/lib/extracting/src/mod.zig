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
const httpx = @import("httpx");

const Allocator = std.mem.Allocator;

pub const Provider = enum {
    antfly,
    pioneer,
    openai,
    mock,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
            .pioneer => "pioneer",
            .openai => "openai",
            .mock => "mock",
        });
    }

    pub fn jsonParse(_: Allocator, source: anytype, _: std.json.ParseOptions) !@This() {
        const raw = switch (try source.next()) {
            .string => |value| value,
            else => return error.UnexpectedToken,
        };
        if (std.mem.eql(u8, raw, "antfly")) return .antfly;
        if (std.mem.eql(u8, raw, "pioneer")) return .pioneer;
        if (std.mem.eql(u8, raw, "openai")) return .openai;
        if (std.mem.eql(u8, raw, "mock")) return .mock;
        return error.UnexpectedToken;
    }
};

pub const Config = struct {
    provider: Provider,
    model: []const u8 = "",
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    bearer_token: ?[]const u8 = null,
    capability_token: ?[]const u8 = null,
    capability_revision: ?[]const u8 = null,
    /// Runtime-resolved Antfly transport capability. This is deliberately not
    /// parsed from user configuration: only a successful capability lease may
    /// enable the task-neutral binary attachment envelope.
    framed_attachments: bool = false,
    schema_json: []const u8 = "",
    options_json: []const u8 = "",

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.model.len > 0) alloc.free(@constCast(self.model));
        if (self.url.len > 0) alloc.free(@constCast(self.url));
        if (self.api_key) |api_key| alloc.free(@constCast(api_key));
        if (self.bearer_token) |bearer_token| alloc.free(@constCast(bearer_token));
        if (self.capability_token) |capability_token| alloc.free(@constCast(capability_token));
        if (self.capability_revision) |revision| alloc.free(@constCast(revision));
        if (self.schema_json.len > 0) alloc.free(@constCast(self.schema_json));
        if (self.options_json.len > 0) alloc.free(@constCast(self.options_json));
        self.* = undefined;
    }

    pub fn validate(self: Config) !void {
        if (self.provider != .mock and self.model.len == 0) return error.InvalidExtractionConfig;
        if ((self.provider == .pioneer or self.provider == .openai) and self.url.len == 0) return error.InvalidExtractionConfig;
    }

    pub fn resolvedUrl(self: Config) ?[]const u8 {
        if (self.url.len == 0) return null;
        return self.url;
    }
};

pub const Input = struct {
    id: ?[]const u8 = null,
    content_json: []const u8,
    tokens_json: ?[]const u8 = null,
    metadata_json: ?[]const u8 = null,
};

pub const Request = struct {
    inputs: []const Input,
    schema_json: []const u8 = "",
    options_json: []const u8 = "",
    /// Borrowed binary media associated with one logical input. Embedded
    /// providers preserve these bytes across the native boundary; HTTP
    /// providers encode them only while constructing the final wire request.
    attachments: []const Attachment = &.{},
    /// Route-owned hard response ceiling for bounded orchestration.
    max_response_bytes: ?usize = null,
};

pub const Attachment = struct {
    input_index: usize,
    bytes: []const u8,
    mime_type: []const u8,
};

pub const Response = struct {
    allocator: Allocator,
    json: []u8,

    pub fn deinit(self: *Response) void {
        self.allocator.free(self.json);
        self.* = undefined;
    }
};

pub const Extractor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        extract: *const fn (ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn extract(self: Extractor, alloc: Allocator, req: Request) !Response {
        return try self.vtable.extract(self.ptr, alloc, req);
    }

    pub fn deinit(self: Extractor) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const Registry = struct {
    allocator: Allocator,
    configs: std.StringArrayHashMapUnmanaged(Config) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Registry {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.configs.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.configs.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn registerConfig(self: *Registry, name: []const u8, cfg: Config) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const owned = try cloneConfig(self.allocator, cfg);
        errdefer {
            var tmp = owned;
            tmp.deinit(self.allocator);
        }
        const gop = try self.configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            return error.DuplicateExtractionProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn getConfig(self: *const Registry, name: ?[]const u8) !Config {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultExtractionProvider;
        return self.configs.get(resolved) orelse return error.UnknownExtractionProvider;
    }
};

pub const Runtime = struct {
    allocator: Allocator,
    extractors: std.StringArrayHashMapUnmanaged(Extractor) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Runtime {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Runtime) void {
        var it = self.extractors.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.extractors.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn loadFromRegistry(self: *Runtime, http: *httpx.Client, registry: *const Registry) !void {
        var it = registry.configs.iterator();
        while (it.next()) |entry| {
            const extractor = try initExtractor(self.allocator, http, entry.value_ptr.*);
            errdefer extractor.deinit();
            try self.registerOwnedExtractor(entry.key_ptr.*, extractor);
        }
        if (registry.default_provider) |name| {
            const idx = self.extractors.getIndex(name) orelse return error.UnknownExtractionProvider;
            self.default_provider = self.extractors.keys()[idx];
        }
    }

    pub fn registerOwnedExtractor(self: *Runtime, name: []const u8, extractor: Extractor) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const gop = try self.extractors.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            extractor.deinit();
            return error.DuplicateExtractionProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = extractor;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn get(self: *const Runtime, name: ?[]const u8) !Extractor {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultExtractionProvider;
        return self.extractors.get(resolved) orelse return error.UnknownExtractionProvider;
    }
};

pub fn parseConfigFromSlice(alloc: Allocator, raw: []const u8) !Config {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtractionConfig;

    const provider_raw = stringField(parsed.value, "provider") orelse return error.InvalidExtractionConfig;
    const provider = try parseProvider(provider_raw);
    const model = if (stringField(parsed.value, "model")) |value| try alloc.dupe(u8, value) else "";
    errdefer if (model.len > 0) alloc.free(model);

    const url_raw = stringField(parsed.value, "url") orelse stringField(parsed.value, "api_url");
    const url = if (url_raw) |value| try alloc.dupe(u8, value) else "";
    errdefer if (url.len > 0) alloc.free(url);

    const api_key = if (stringField(parsed.value, "api_key")) |value| try alloc.dupe(u8, value) else null;
    errdefer if (api_key) |value| alloc.free(value);

    const bearer_token = if (stringField(parsed.value, "bearer_token")) |value| try alloc.dupe(u8, value) else null;
    errdefer if (bearer_token) |value| alloc.free(value);

    const schema_json = if (parsed.value.object.get("schema")) |schema|
        try std.json.Stringify.valueAlloc(alloc, schema, .{})
    else
        try alloc.dupe(u8, "{}");
    errdefer alloc.free(schema_json);

    const options_json = if (parsed.value.object.get("options")) |options|
        try std.json.Stringify.valueAlloc(alloc, options, .{})
    else
        try alloc.dupe(u8, "{}");
    errdefer alloc.free(options_json);

    var cfg = Config{
        .provider = provider,
        .model = model,
        .url = url,
        .api_key = api_key,
        .bearer_token = bearer_token,
        .capability_token = null,
        .capability_revision = null,
        .framed_attachments = false,
        .schema_json = schema_json,
        .options_json = options_json,
    };
    try cfg.validate();
    return cfg;
}

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    return .{
        .provider = cfg.provider,
        .model = if (cfg.model.len > 0) try alloc.dupe(u8, cfg.model) else "",
        .url = if (cfg.url.len > 0) try alloc.dupe(u8, cfg.url) else "",
        .api_key = if (cfg.api_key) |value| try alloc.dupe(u8, value) else null,
        .bearer_token = if (cfg.bearer_token) |value| try alloc.dupe(u8, value) else null,
        .capability_token = if (cfg.capability_token) |value| try alloc.dupe(u8, value) else null,
        .capability_revision = if (cfg.capability_revision) |value| try alloc.dupe(u8, value) else null,
        .framed_attachments = cfg.framed_attachments,
        .schema_json = try alloc.dupe(u8, cfg.schema_json),
        .options_json = try alloc.dupe(u8, cfg.options_json),
    };
}

pub const RemoteOptions = struct {
    source_table: []const u8 = "",
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,
};

pub fn initExtractor(alloc: Allocator, http: *httpx.Client, cfg: Config) !Extractor {
    return initExtractorWithOptions(alloc, http, cfg, .{});
}

fn initExtractorWithOptions(alloc: Allocator, http: *httpx.Client, cfg: Config, options: RemoteOptions) !Extractor {
    return switch (cfg.provider) {
        .antfly, .pioneer, .openai => try HttpExtractorState.init(alloc, http, cfg, options),
        .mock => error.UnsupportedExtractionProvider,
    };
}

pub fn extractWithConfig(alloc: Allocator, http: *httpx.Client, cfg: Config, req: Request) !Response {
    const extractor = try initExtractor(alloc, http, cfg);
    defer extractor.deinit();
    return try extractor.extract(alloc, req);
}

pub fn extractWithConfigAndOptions(
    alloc: Allocator,
    http: *httpx.Client,
    cfg: Config,
    req: Request,
    options: RemoteOptions,
) !Response {
    const extractor = try initExtractorWithOptions(alloc, http, cfg, options);
    defer extractor.deinit();
    return try extractor.extract(alloc, req);
}

pub fn firstResultJsonAlloc(alloc: Allocator, response_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, response_json, .{});
    defer parsed.deinit();
    if (parsed.value == .object) {
        if (parsed.value.object.get("data")) |data| {
            if (data == .array and data.array.items.len > 0) {
                return try std.json.Stringify.valueAlloc(alloc, data.array.items[0], .{});
            }
        }
    }
    return try alloc.dupe(u8, response_json);
}

const HttpExtractorState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    cfg: Config,
    source_table: ?[]u8 = null,
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config, options: RemoteOptions) !Extractor {
        const state = try alloc.create(HttpExtractorState);
        errdefer alloc.destroy(state);
        // Source routing is an Antfly-internal protocol. Never forward it to
        // third-party extractor endpoints that happen to share this adapter.
        const source_table = if (cfg.provider == .antfly and options.source_table.len > 0)
            try alloc.dupe(u8, options.source_table)
        else
            null;
        errdefer if (source_table) |value| alloc.free(value);
        state.* = .{
            .alloc = alloc,
            .http = http,
            .cfg = try cloneConfig(alloc, cfg),
            .source_table = source_table,
            .timeout_ms = options.timeout_ms,
            .cancellation = options.cancellation,
        };
        return .{ .ptr = state, .vtable = &.{ .extract = extract, .deinit = deinit } };
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *HttpExtractorState = @ptrCast(@alignCast(ptr));
        self.cfg.deinit(self.alloc);
        if (self.source_table) |source_table| self.alloc.free(source_table);
        self.alloc.destroy(self);
    }

    fn extract(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *HttpExtractorState = @ptrCast(@alignCast(ptr));
        const metadata = try requestJsonAlloc(alloc, self.cfg, req);
        defer alloc.free(metadata);
        const use_framed_transport = self.cfg.provider == .antfly and
            self.cfg.framed_attachments and req.attachments.len > 0;
        var framed_body: ?httpx.attachment_envelope.EncodedSegments = null;
        defer if (framed_body) |*body| body.deinit();
        if (use_framed_transport) {
            const attachments = try alloc.alloc(httpx.attachment_envelope.Attachment, req.attachments.len);
            defer alloc.free(attachments);
            var attachment_index: usize = 0;
            for (req.inputs, 0..) |_, input_index| {
                for (req.attachments) |attachment| {
                    if (attachment.input_index != input_index) continue;
                    attachments[attachment_index] = .{
                        .mime_type = attachment.mime_type,
                        .data = attachment.bytes,
                    };
                    attachment_index += 1;
                }
            }
            std.debug.assert(attachment_index == attachments.len);
            framed_body = try httpx.attachment_envelope.encodeSegmentsAlloc(alloc, metadata, attachments);
        }

        const base = self.cfg.resolvedUrl() orelse switch (self.cfg.provider) {
            .antfly => "http://127.0.0.1:8080",
            else => return error.InvalidExtractionConfig,
        };
        const path = switch (self.cfg.provider) {
            .pioneer => "/inference",
            else => "/extract",
        };
        const url = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base, path });
        defer alloc.free(url);

        var headers = std.ArrayList([2][]const u8).empty;
        defer headers.deinit(alloc);
        var auth_header: ?[]u8 = null;
        defer if (auth_header) |value| alloc.free(value);
        if (self.cfg.bearer_token orelse self.cfg.api_key) |token| {
            auth_header = try std.fmt.allocPrint(alloc, "Bearer {s}", .{token});
            try headers.append(alloc, .{ "Authorization", auth_header.? });
        }
        if (self.source_table) |source_table|
            try headers.append(alloc, .{ "X-Antfly-Source-Table", source_table });
        if (self.cfg.capability_token) |token|
            try headers.append(alloc, .{ "X-Antfly-Capability-Token", token });
        if (self.cfg.capability_revision) |revision|
            try headers.append(alloc, .{ "X-Antfly-Capability-Revision", revision });

        if (use_framed_transport)
            try headers.append(alloc, .{ "Content-Type", httpx.attachment_envelope.content_type });
        var resp = try self.http.post(url, .{
            .json = if (use_framed_transport) null else metadata,
            .borrowed_body_segments = if (framed_body) |body| body.segments else null,
            .headers = headers.items,
            .timeout_ms = self.timeout_ms,
            .max_response_size = req.max_response_bytes,
            .cancellation = self.cancellation,
        });
        defer resp.deinit();
        if (!resp.ok()) return if (responseCapabilityStale(resp))
            error.InferenceCapabilitiesStale
        else
            error.ExtractionRequestFailed;
        const payload = resp.body orelse return error.EmptyExtractionResponse;
        const canonical = try canonicalResponseJsonAlloc(alloc, payload);
        return .{ .allocator = alloc, .json = canonical };
    }
};

fn responseCapabilityStale(response: httpx.Response) bool {
    if (response.status.code != 409) return false;
    const value = response.headers.get("X-Antfly-Capability-Stale") orelse return false;
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, value, " \t"), "true");
}

fn requestJsonAlloc(alloc: Allocator, cfg: Config, req: Request) ![]u8 {
    try validateAttachments(req);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"model\":");
    try appendJsonString(alloc, &out, cfg.model);
    try out.appendSlice(alloc, ",\"inputs\":[");
    var attachment_cursor: usize = 0;
    for (req.inputs, 0..) |input, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        var first = true;
        if (input.id) |id| {
            try out.appendSlice(alloc, "\"id\":");
            try appendJsonString(alloc, &out, id);
            first = false;
        }
        if (!first) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"content\":");
        const content_json = try inputContentJsonAlloc(
            alloc,
            req,
            i,
            input.content_json,
            cfg.provider == .antfly and cfg.framed_attachments,
            &attachment_cursor,
        );
        defer alloc.free(content_json);
        try out.appendSlice(alloc, content_json);
        if (input.tokens_json) |tokens_json| {
            try out.appendSlice(alloc, ",\"tokens\":");
            try out.appendSlice(alloc, tokens_json);
        }
        if (input.metadata_json) |metadata_json| {
            try out.appendSlice(alloc, ",\"metadata\":");
            try out.appendSlice(alloc, metadata_json);
        }
        try out.append(alloc, '}');
    }
    const schema_json = if (req.schema_json.len > 0) req.schema_json else cfg.schema_json;
    const options_json = if (req.options_json.len > 0) req.options_json else cfg.options_json;
    try out.appendSlice(alloc, "],\"schema\":");
    try out.appendSlice(alloc, if (schema_json.len > 0) schema_json else "{}");
    if (options_json.len > 0 and !std.mem.eql(u8, options_json, "{}")) {
        try out.appendSlice(alloc, ",\"options\":");
        try out.appendSlice(alloc, options_json);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn inputContentJsonAlloc(
    alloc: Allocator,
    req: Request,
    input_index: usize,
    original: []const u8,
    framed_attachments: bool,
    attachment_cursor: *usize,
) ![]u8 {
    var attachment_count: usize = 0;
    for (req.attachments) |attachment| {
        if (attachment.input_index == input_index) attachment_count += 1;
    }
    if (attachment_count == 0) return try alloc.dupe(u8, original);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, original, .{});
    defer parsed.deinit();
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    var emitted = false;
    if (parsed.value == .array) {
        for (parsed.value.array.items) |part| {
            if (emitted) try out.append(alloc, ',');
            const encoded = try std.json.Stringify.valueAlloc(alloc, part, .{});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
            emitted = true;
        }
    } else if (parsed.value == .string and parsed.value.string.len > 0) {
        try out.appendSlice(alloc, "{\"type\":\"text\",\"text\":");
        try appendJsonString(alloc, &out, parsed.value.string);
        try out.append(alloc, '}');
        emitted = true;
    } else if (parsed.value != .string) return error.InvalidExtractionContent;
    for (req.attachments) |attachment| {
        if (attachment.input_index != input_index) continue;
        if (emitted) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"type\":\"media\",\"mime_type\":");
        try appendJsonString(alloc, &out, attachment.mime_type);
        try out.appendSlice(alloc, ",\"data\":");
        if (framed_attachments) {
            const reference = try std.fmt.allocPrint(alloc, "attachment:{d}", .{attachment_cursor.*});
            defer alloc.free(reference);
            try appendJsonString(alloc, &out, reference);
            attachment_cursor.* += 1;
        } else {
            const encoded_len = std.base64.standard.Encoder.calcSize(attachment.bytes.len);
            const encoded = try alloc.alloc(u8, encoded_len);
            defer alloc.free(encoded);
            _ = std.base64.standard.Encoder.encode(encoded, attachment.bytes);
            try appendJsonString(alloc, &out, encoded);
        }
        try out.append(alloc, '}');
        emitted = true;
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn validateAttachments(req: Request) !void {
    for (req.attachments) |attachment| {
        if (attachment.input_index >= req.inputs.len or attachment.mime_type.len == 0 or attachment.bytes.len == 0)
            return error.InvalidExtractionAttachment;
    }
}

fn canonicalResponseJsonAlloc(alloc: Allocator, payload: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, payload, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidExtractionResponse;

    if (parsed.value.object.get("object")) |object_value| {
        if (object_value == .string and std.mem.eql(u8, object_value.string, "extraction")) {
            return try alloc.dupe(u8, payload);
        }
    }
    return error.InvalidExtractionResponse;
}

fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn parseProvider(raw: []const u8) !Provider {
    if (std.mem.eql(u8, raw, "antfly")) return .antfly;
    if (std.mem.eql(u8, raw, "pioneer")) return .pioneer;
    if (std.mem.eql(u8, raw, "openai")) return .openai;
    if (std.mem.eql(u8, raw, "mock")) return .mock;
    return error.InvalidExtractionConfig;
}

fn stringField(value: std.json.Value, name: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const field = value.object.get(name) orelse return null;
    if (field != .string) return null;
    return field.string;
}

test "extracting config parses schema and options" {
    const alloc = std.testing.allocator;
    var cfg = try parseConfigFromSlice(alloc,
        \\{"provider":"antfly","model":"gliner","schema":{"entities":["person"]},"options":{"threshold":0.5}}
    );
    defer cfg.deinit(alloc);
    try std.testing.expectEqual(Provider.antfly, cfg.provider);
    try std.testing.expect(std.mem.indexOf(u8, cfg.schema_json, "\"entities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, cfg.options_json, "\"threshold\"") != null);
}

test "extracting request json uses content parts" {
    const alloc = std.testing.allocator;
    var cfg = try parseConfigFromSlice(alloc,
        \\{"provider":"antfly","model":"gliner","schema":{"entities":["person"]}}
    );
    defer cfg.deinit(alloc);
    const req = Request{ .inputs = &.{.{ .content_json = "[{\"type\":\"text\",\"text\":\"hello\"}]" }} };
    const body = try requestJsonAlloc(alloc, cfg, req);
    defer alloc.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"inputs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"type\":\"text\"") != null);
}

test "extracting rejects non-canonical extract response" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidExtractionResponse, canonicalResponseJsonAlloc(alloc,
        \\{"object":"list","model":"gliner","data":[{"results":{"person":[{"name":{"value":"Ada"}}]}}]}
    ));
}

test "extracting accepts canonical extract response" {
    const alloc = std.testing.allocator;
    const canonical = try canonicalResponseJsonAlloc(alloc,
        \\{"object":"extraction","model":"gliner","data":[{"entities":[{"label":"person","text":"Ada"}]}]}
    );
    defer alloc.free(canonical);
    try std.testing.expect(std.mem.indexOf(u8, canonical, "\"object\":\"extraction\"") != null);
}

test "extracting registry duplicate provider error does not double free config" {
    const alloc = std.testing.allocator;
    var registry = Registry.init(alloc);
    defer registry.deinit();

    try registry.registerConfig("ner", .{ .provider = .antfly, .model = "local-extractor" });
    try std.testing.expectError(error.DuplicateExtractionProviderName, registry.registerConfig("ner", .{
        .provider = .pioneer,
        .model = "gliner2",
        .url = "https://api.example.test",
        .api_key = "test-key",
        .schema_json = "{\"entities\":[\"person\"]}",
    }));
}

test "extracting first result returns asset value" {
    const alloc = std.testing.allocator;
    const value = try firstResultJsonAlloc(alloc,
        \\{"object":"extraction","model":"m","data":[{"entities":[{"text":"Ada","label":"person"}]}]}
    );
    defer alloc.free(value);
    try std.testing.expect(std.mem.indexOf(u8, value, "\"entities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, value, "\"object\"") == null);
}

test "extracting HTTP boundary encodes borrowed media only in final content" {
    const bytes = [_]u8{ 1, 2, 3 };
    const inputs = [_]Input{.{ .content_json = "\"ocr prompt\"" }};
    const attachments = [_]Attachment{.{ .input_index = 0, .bytes = &bytes, .mime_type = "image/png" }};
    var attachment_cursor: usize = 0;
    const content = try inputContentJsonAlloc(std.testing.allocator, .{
        .inputs = &inputs,
        .attachments = &attachments,
    }, 0, inputs[0].content_json, false, &attachment_cursor);
    defer std.testing.allocator.free(content);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"type\":\"text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"mime_type\":\"image/png\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"data\":\"AQID\"") != null);
}

test "extracting framed boundary uses canonical attachment references" {
    const bytes = [_]u8{ 1, 2, 3 };
    const inputs = [_]Input{.{ .content_json = "\"ocr prompt\"" }};
    const attachments = [_]Attachment{.{ .input_index = 0, .bytes = &bytes, .mime_type = "image/png" }};
    const body = try requestJsonAlloc(std.testing.allocator, .{
        .provider = .antfly,
        .model = "extractor",
        .framed_attachments = true,
    }, .{
        .inputs = &inputs,
        .attachments = &attachments,
    });
    defer std.testing.allocator.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"data\":\"attachment:0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "AQID") == null);
}

test "extracting HTTP boundary rejects attachments without a logical input" {
    const bytes = [_]u8{1};
    const attachments = [_]Attachment{.{ .input_index = 0, .bytes = &bytes, .mime_type = "image/png" }};
    try std.testing.expectError(error.InvalidExtractionAttachment, requestJsonAlloc(std.testing.allocator, .{
        .provider = .antfly,
        .model = "extractor",
    }, .{
        .inputs = &.{},
        .attachments = &attachments,
    }));
}

fn expectExtractRequest(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(.POST, req.method);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"model\":\"gliner\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"inputs\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"entities\"") != null);
    try std.testing.expectEqualStrings("Bearer secret", req.header("Authorization") orelse return error.MissingHeader);
}

test "extracting antfly provider posts canonical extract request" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/extract", .assert_request = expectExtractRequest, .respond = .{
            .body = "{\"object\":\"extraction\",\"model\":\"gliner\",\"data\":[{\"entities\":[{\"label\":\"person\",\"text\":\"Ada\"}]}]}",
        } },
    });
    defer server.deinit();

    const raw_cfg = try std.fmt.allocPrint(
        alloc,
        "{{\"provider\":\"antfly\",\"model\":\"gliner\",\"url\":\"{s}\",\"api_key\":\"secret\",\"schema\":{{\"entities\":[\"person\"]}}}}",
        .{server.baseUrl()},
    );
    defer alloc.free(raw_cfg);
    var cfg = try parseConfigFromSlice(alloc, raw_cfg);
    defer cfg.deinit(alloc);

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    var result: ?Response = null;
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, http: *httpx.Client, config: Config, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
            out.* = extractWithConfig(a, http, config, .{
                .inputs = &.{.{ .content_json = "\"Ada\"" }},
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    try group.concurrent(io, Fiber.run, .{ alloc, &client, cfg, &result, &run_err });
    try server.handleOne();
    try group.await(io);
    if (run_err) |err| return err;
    defer result.?.deinit();
    try std.testing.expect(std.mem.indexOf(u8, result.?.json, "\"object\":\"extraction\"") != null);
}
