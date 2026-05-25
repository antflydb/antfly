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
const audio = @import("antfly_audio_openapi");
const httpx = @import("httpx");
const termite_api = @import("termite_api");
const scraping = @import("antfly_scraping");

const Allocator = std.mem.Allocator;

pub const Request = audio.STTRequest;
pub const Response = audio.STTResponse;
pub const Segment = audio.TranscriptSegment;
pub const WordTimestamp = audio.WordTimestamp;
pub const Speaker = audio.Speaker;

pub const Provider = enum {
    antfly,
    termite,
    openai,
    vertex,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
            .termite => "termite",
            .openai => "openai",
            .vertex => "vertex",
        });
    }

    pub fn jsonParse(_: Allocator, source: anytype, _: std.json.ParseOptions) !@This() {
        const raw = switch (try source.next()) {
            .string => |value| value,
            else => return error.UnexpectedToken,
        };
        if (std.mem.eql(u8, raw, "antfly")) return .antfly;
        if (std.mem.eql(u8, raw, "termite")) return .termite;
        if (std.mem.eql(u8, raw, "openai")) return .openai;
        if (std.mem.eql(u8, raw, "vertex")) return .vertex;
        return error.UnexpectedToken;
    }
};

pub const Config = struct {
    provider: Provider,
    model: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
    bearer_token: ?[]const u8 = null,
    base_url: ?[]const u8 = null,
    url: ?[]const u8 = null,
    api_url: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    credentials_path: ?[]const u8 = null,
    language_code: ?[]const u8 = null,
    enable_automatic_punctuation: ?bool = null,
    use_enhanced: ?bool = null,

    pub fn resolvedUrl(self: Config) ?[]const u8 {
        return self.url orelse self.api_url;
    }
};

const remote_fetch_security = scraping.ContentSecurityConfig{
    .max_download_size_bytes = 32 << 20,
};

threadlocal var active_runtime: ?*const Runtime = null;

pub const Transcriber = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        transcribe: *const fn (ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn transcribe(self: Transcriber, alloc: Allocator, req: Request) !Response {
        return try self.vtable.transcribe(self.ptr, alloc, req);
    }

    pub fn deinit(self: Transcriber) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const Runtime = struct {
    allocator: Allocator,
    transcribers: std.StringArrayHashMapUnmanaged(Transcriber) = .{},
    default_provider: ?[]const u8 = null,

    pub fn init(alloc: Allocator) Runtime {
        return .{ .allocator = alloc };
    }

    pub fn deinit(self: *Runtime) void {
        var it = self.transcribers.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
        self.transcribers.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn registerOwnedTranscriber(self: *Runtime, name: []const u8, transcriber: Transcriber) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const gop = try self.transcribers.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            transcriber.deinit();
            return error.DuplicateTranscribingProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = transcriber;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn loadFromRegistry(self: *Runtime, http: *httpx.Client, registry: *const Registry) !void {
        var it = registry.configs.iterator();
        while (it.next()) |entry| {
            const transcriber = try initTranscriber(self.allocator, http, entry.value_ptr.*);
            errdefer transcriber.deinit();
            try self.registerOwnedTranscriber(entry.key_ptr.*, transcriber);
        }
        if (registry.default_provider) |name| {
            const idx = self.transcribers.getIndex(name) orelse return error.UnknownTranscribingProvider;
            self.default_provider = self.transcribers.keys()[idx];
        }
    }

    pub fn get(self: *const Runtime, name: ?[]const u8) !Transcriber {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultTranscribingProvider;
        return self.transcribers.get(resolved) orelse return error.UnknownTranscribingProvider;
    }
};

pub fn setActiveRuntime(runtime: ?*const Runtime) void {
    active_runtime = runtime;
}

pub fn getActiveRuntime() ?*const Runtime {
    return active_runtime;
}

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
            deinitConfig(self.allocator, entry.value_ptr);
        }
        self.configs.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn parseFromValue(alloc: Allocator, value: std.json.Value) !Registry {
        if (value != .object) return error.InvalidTranscribingConfig;

        var registry = Registry.init(alloc);
        errdefer registry.deinit();

        var it = value.object.iterator();
        while (it.next()) |entry| {
            var parsed = try std.json.parseFromValue(Config, alloc, entry.value_ptr.*, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            try registry.registerConfig(entry.key_ptr.*, parsed.value);
        }
        return registry;
    }

    pub fn registerConfig(self: *Registry, name: []const u8, cfg: Config) !void {
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        const owned = try cloneConfig(self.allocator, cfg);
        errdefer deinitConfigValue(self.allocator, owned);

        const gop = try self.configs.getOrPut(self.allocator, key);
        if (gop.found_existing) {
            self.allocator.free(key);
            deinitConfigValue(self.allocator, owned);
            return error.DuplicateTranscribingProviderName;
        }
        gop.key_ptr.* = key;
        gop.value_ptr.* = owned;
        if (self.default_provider == null) self.default_provider = gop.key_ptr.*;
    }

    pub fn defaultProviderName(self: *const Registry) ?[]const u8 {
        return self.default_provider;
    }

    pub fn getConfig(self: *const Registry, name: ?[]const u8) !Config {
        const resolved = name orelse self.default_provider orelse return error.NoDefaultTranscribingProvider;
        return self.configs.get(resolved) orelse return error.UnknownTranscribingProvider;
    }
};

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    return .{
        .model = try dupOpt(alloc, cfg.model),
        .api_key = try dupOpt(alloc, cfg.api_key),
        .bearer_token = try dupOpt(alloc, cfg.bearer_token),
        .base_url = try dupOpt(alloc, cfg.base_url),
        .url = try dupOpt(alloc, cfg.url),
        .api_url = try dupOpt(alloc, cfg.api_url),
        .project_id = try dupOpt(alloc, cfg.project_id),
        .location = try dupOpt(alloc, cfg.location),
        .credentials_path = try dupOpt(alloc, cfg.credentials_path),
        .language_code = try dupOpt(alloc, cfg.language_code),
        .enable_automatic_punctuation = cfg.enable_automatic_punctuation,
        .use_enhanced = cfg.use_enhanced,
        .provider = cfg.provider,
    };
}

pub fn deinitConfig(alloc: Allocator, cfg: *Config) void {
    freeOpt(alloc, cfg.model);
    freeOpt(alloc, cfg.api_key);
    freeOpt(alloc, cfg.bearer_token);
    freeOpt(alloc, cfg.base_url);
    freeOpt(alloc, cfg.url);
    freeOpt(alloc, cfg.api_url);
    freeOpt(alloc, cfg.project_id);
    freeOpt(alloc, cfg.location);
    freeOpt(alloc, cfg.credentials_path);
    freeOpt(alloc, cfg.language_code);
    cfg.* = undefined;
}

pub fn deinitResponse(alloc: Allocator, response: *Response) void {
    freeOpt(alloc, response.text);
    freeOpt(alloc, response.language);
    if (response.segments) |segments| {
        for (segments) |segment| {
            var owned = segment;
            deinitSegment(alloc, &owned);
        }
        alloc.free(@constCast(segments));
    }
    if (response.speakers) |speakers| {
        for (speakers) |speaker| {
            var owned = speaker;
            deinitSpeaker(alloc, &owned);
        }
        alloc.free(@constCast(speakers));
    }
    response.* = undefined;
}

fn deinitConfigValue(alloc: Allocator, cfg: Config) void {
    var owned = cfg;
    deinitConfig(alloc, &owned);
}

fn deinitSegment(alloc: Allocator, segment: *Segment) void {
    freeOpt(alloc, segment.text);
    freeOpt(alloc, segment.speaker);
    if (segment.words) |words| {
        for (words) |word| {
            var owned = word;
            deinitWordTimestamp(alloc, &owned);
        }
        alloc.free(@constCast(words));
    }
    segment.* = undefined;
}

fn deinitWordTimestamp(alloc: Allocator, word: *WordTimestamp) void {
    freeOpt(alloc, word.word);
    word.* = undefined;
}

fn deinitSpeaker(alloc: Allocator, speaker: *Speaker) void {
    freeOpt(alloc, speaker.id);
    freeOpt(alloc, speaker.label);
    speaker.* = undefined;
}

fn initTranscriber(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
    return switch (cfg.provider) {
        .antfly, .termite => try AntflyTranscriberState.init(alloc, http, cfg),
        .openai => try OpenAiTranscriberState.init(alloc, http, cfg),
        .vertex => try VertexTranscriberState.init(alloc, http, cfg),
    };
}

const AntflyTranscriberState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    api_url: []const u8,
    model: ?[]const u8 = null,
    language_code: ?[]const u8 = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
        const state = try alloc.create(AntflyTranscriberState);
        errdefer alloc.destroy(state);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .api_url = try alloc.dupe(u8, cfg.resolvedUrl() orelse "http://127.0.0.1:8080"),
            .model = try dupOpt(alloc, cfg.model),
            .language_code = try dupOpt(alloc, cfg.language_code),
        };

        return .{
            .ptr = state,
            .vtable = &.{
                .transcribe = transcribe,
                .deinit = deinit,
            },
        };
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *AntflyTranscriberState = @ptrCast(@alignCast(ptr));
        self.alloc.free(self.api_url);
        freeOpt(self.alloc, self.model);
        freeOpt(self.alloc, self.language_code);
        self.alloc.destroy(self);
    }

    fn transcribe(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *AntflyTranscriberState = @ptrCast(@alignCast(ptr));
        const audio_bytes = try resolveAudioInputAlloc(alloc, req.url);
        defer alloc.free(audio_bytes);

        const encoded_len = std.base64.standard.Encoder.calcSize(audio_bytes.len);
        const encoded = try alloc.alloc(u8, encoded_len);
        defer alloc.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, audio_bytes);

        const url = try std.fmt.allocPrint(alloc, "{s}/transcribe", .{self.api_url});
        defer alloc.free(url);

        const body = try httpx.json.Json.stringify(alloc, termite_api.types.TranscribeRequest{
            .model = self.model,
            .audio = encoded,
            .language = req.language orelse self.language_code,
        });
        defer alloc.free(body);

        var resp = try self.http.post(url, .{ .json = body });
        defer resp.deinit();
        if (!resp.ok()) return error.TranscribeRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(termite_api.types.TranscribeResponse, alloc, payload, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        const first = if (parsed.value.data.len > 0) parsed.value.data[0] else return error.EmptyResponse;
        return .{
            .text = try alloc.dupe(u8, first.text),
            .language = try dupOpt(alloc, first.language),
        };
    }
};

const OpenAiTranscriberState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,
    model: []const u8,
    language_code: ?[]const u8 = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
        const state = try alloc.create(OpenAiTranscriberState);
        errdefer alloc.destroy(state);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .base_url = try alloc.dupe(u8, cfg.base_url orelse "https://api.openai.com/v1"),
            .model = try alloc.dupe(u8, cfg.model orelse "whisper-1"),
            .language_code = try dupOpt(alloc, cfg.language_code),
        };
        if (cfg.api_key) |api_key| try state.setApiKey(api_key);

        return .{
            .ptr = state,
            .vtable = &.{
                .transcribe = transcribe,
                .deinit = deinit,
            },
        };
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *OpenAiTranscriberState = @ptrCast(@alignCast(ptr));
        self.alloc.free(self.base_url);
        self.alloc.free(self.model);
        freeOpt(self.alloc, self.language_code);
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.alloc.destroy(self);
    }

    fn setApiKey(self: *OpenAiTranscriberState, api_key: []const u8) !void {
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.auth_header = .{
            "Authorization",
            try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{api_key}),
        };
    }

    fn transcribe(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *OpenAiTranscriberState = @ptrCast(@alignCast(ptr));
        const audio_bytes = try resolveAudioInputAlloc(alloc, req.url);
        defer alloc.free(audio_bytes);

        const content_type = "application/octet-stream";
        const multipart = try buildOpenAiMultipartAlloc(alloc, self.model, req.language orelse self.language_code, audio_bytes, content_type);
        defer alloc.free(multipart.body);
        defer alloc.free(multipart.content_type);

        const url = try std.fmt.allocPrint(alloc, "{s}/audio/transcriptions", .{self.base_url});
        defer alloc.free(url);

        var headers = std.ArrayList([2][]const u8).empty;
        defer headers.deinit(alloc);
        try headers.append(alloc, .{ "Content-Type", multipart.content_type });
        if (self.auth_header) |header| try headers.append(alloc, header);

        var resp = try self.http.post(url, .{
            .body = multipart.body,
            .headers = headers.items,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.TranscribeRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(audio.STTResponse, alloc, payload, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        return try cloneResponse(alloc, parsed.value);
    }
};

const VertexTranscriberState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,
    project_id: []const u8,
    location: []const u8,
    model: []const u8,
    language_code: []const u8,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
        const state = try alloc.create(VertexTranscriberState);
        errdefer alloc.destroy(state);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .base_url = try alloc.dupe(u8, cfg.base_url orelse "https://speech.googleapis.com/v2"),
            .project_id = try alloc.dupe(u8, cfg.project_id orelse return error.InvalidTranscribingConfig),
            .location = try alloc.dupe(u8, cfg.location orelse "global"),
            .model = try alloc.dupe(u8, cfg.model orelse "latest_long"),
            .language_code = try alloc.dupe(u8, cfg.language_code orelse "en-US"),
        };
        errdefer state.deinitState();

        if (cfg.bearer_token orelse cfg.api_key) |token| {
            try state.setBearer(token);
        } else if (cfg.credentials_path != null) {
            return error.UnsupportedVertexServiceAccountCredentials;
        } else {
            return error.MissingVertexCredentials;
        }

        return .{
            .ptr = state,
            .vtable = &.{
                .transcribe = transcribe,
                .deinit = deinit,
            },
        };
    }

    fn deinitState(self: *VertexTranscriberState) void {
        self.alloc.free(self.base_url);
        self.alloc.free(self.project_id);
        self.alloc.free(self.location);
        self.alloc.free(self.model);
        self.alloc.free(self.language_code);
        if (self.auth_header) |header| self.alloc.free(header[1]);
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *VertexTranscriberState = @ptrCast(@alignCast(ptr));
        self.deinitState();
        self.alloc.destroy(self);
    }

    fn setBearer(self: *VertexTranscriberState, token: []const u8) !void {
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.auth_header = .{
            "Authorization",
            try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{token}),
        };
    }

    fn transcribe(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *VertexTranscriberState = @ptrCast(@alignCast(ptr));
        const audio_bytes = try resolveAudioInputAlloc(alloc, req.url);
        defer alloc.free(audio_bytes);

        const encoded_len = std.base64.standard.Encoder.calcSize(audio_bytes.len);
        const encoded = try alloc.alloc(u8, encoded_len);
        defer alloc.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, audio_bytes);

        const language = req.language orelse self.language_code;
        const RecognitionConfig = struct {
            explicitDecodingConfig: struct {} = .{},
            model: []const u8,
            languageCodes: []const []const u8,
        };
        const RequestBody = struct {
            config: RecognitionConfig,
            content: []const u8,
        };
        const languages = [_][]const u8{language};
        const body = try httpx.json.Json.stringify(alloc, RequestBody{
            .config = .{
                .model = self.model,
                .languageCodes = &languages,
            },
            .content = encoded,
        });
        defer alloc.free(body);

        const url = try std.fmt.allocPrint(
            alloc,
            "{s}/projects/{s}/locations/{s}/recognizers/_:recognize",
            .{ self.base_url, self.project_id, self.location },
        );
        defer alloc.free(url);

        var headers = std.ArrayList([2][]const u8).empty;
        defer headers.deinit(alloc);
        if (self.auth_header) |header| try headers.append(alloc, header);

        var resp = try self.http.post(url, .{
            .json = body,
            .headers = headers.items,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.TranscribeRequestFailed;

        const ResponseBody = struct {
            results: []const struct {
                alternatives: []const struct {
                    transcript: ?[]const u8 = null,
                } = &.{},
                languageCode: ?[]const u8 = null,
            } = &.{},
        };
        const payload = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(ResponseBody, alloc, payload, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.results.len == 0 or parsed.value.results[0].alternatives.len == 0) return error.EmptyResponse;

        return .{
            .text = try dupOpt(alloc, parsed.value.results[0].alternatives[0].transcript),
            .language = try dupOpt(alloc, parsed.value.results[0].languageCode),
        };
    }
};

const MultipartBody = struct {
    content_type: []u8,
    body: []u8,
};

fn buildOpenAiMultipartAlloc(
    alloc: Allocator,
    model: []const u8,
    language: ?[]const u8,
    audio_bytes: []const u8,
    audio_content_type: []const u8,
) !MultipartBody {
    const boundary = "antfly-zig-audio-boundary";
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(alloc);

    try appendMultipartField(&buf, alloc, boundary, "model", model);
    if (language) |lang| try appendMultipartField(&buf, alloc, boundary, "language", lang);
    try appendMultipartField(&buf, alloc, boundary, "response_format", "json");
    try appendMultipartFile(&buf, alloc, boundary, "file", "audio", audio_content_type, audio_bytes);
    try buf.print(alloc, "--{s}--\r\n", .{boundary});

    return .{
        .content_type = try std.fmt.allocPrint(alloc, "multipart/form-data; boundary={s}", .{boundary}),
        .body = try buf.toOwnedSlice(alloc),
    };
}

fn appendMultipartField(
    buf: *std.ArrayList(u8),
    alloc: Allocator,
    boundary: []const u8,
    name: []const u8,
    value: []const u8,
) !void {
    try buf.print(
        alloc,
        "--{s}\r\nContent-Disposition: form-data; name=\"{s}\"\r\n\r\n{s}\r\n",
        .{ boundary, name, value },
    );
}

fn appendMultipartFile(
    buf: *std.ArrayList(u8),
    alloc: Allocator,
    boundary: []const u8,
    name: []const u8,
    filename: []const u8,
    content_type: []const u8,
    content: []const u8,
) !void {
    try buf.print(
        alloc,
        "--{s}\r\nContent-Disposition: form-data; name=\"{s}\"; filename=\"{s}\"\r\nContent-Type: {s}\r\n\r\n",
        .{ boundary, name, filename, content_type },
    );
    try buf.appendSlice(alloc, content);
    try buf.appendSlice(alloc, "\r\n");
}

fn resolveAudioInputAlloc(alloc: Allocator, url: []const u8) ![]u8 {
    if (std.mem.startsWith(u8, url, "data:")) {
        return try decodeDataUriAlloc(alloc, url);
    }

    const fetched = try scraping.downloadContentOutcomeAlloc(alloc, url, &remote_fetch_security, null);
    switch (fetched) {
        .http_error => |err_resp| {
            _ = err_resp;
            return error.RemoteAudioFetchFailed;
        },
        .ok => |response| {
            defer {
                var owned = response;
                owned.deinit(alloc);
            }
            return try alloc.dupe(u8, response.data);
        },
    }
}

fn decodeDataUriAlloc(alloc: Allocator, uri: []const u8) ![]u8 {
    const comma = std.mem.indexOfScalar(u8, uri, ',') orelse return error.InvalidDataUri;
    const meta = uri[5..comma];
    const data = uri[comma + 1 ..];
    if (std.mem.endsWith(u8, meta, ";base64")) {
        const size = try std.base64.standard.Decoder.calcSizeForSlice(data);
        const out = try alloc.alloc(u8, size);
        errdefer alloc.free(out);
        try std.base64.standard.Decoder.decode(out, data);
        return out;
    }
    return try alloc.dupe(u8, data);
}

fn cloneResponse(alloc: Allocator, response: Response) !Response {
    return .{
        .text = try dupOpt(alloc, response.text),
        .language = try dupOpt(alloc, response.language),
        .duration_ms = response.duration_ms,
        .segments = try cloneSegments(alloc, response.segments),
        .speakers = try cloneSpeakers(alloc, response.speakers),
    };
}

fn cloneSegments(alloc: Allocator, segments: ?[]const Segment) !?[]Segment {
    const src = segments orelse return null;
    const out = try alloc.alloc(Segment, src.len);
    errdefer alloc.free(out);
    for (src, 0..) |segment, i| {
        out[i] = .{
            .text = try dupOpt(alloc, segment.text),
            .start_ms = segment.start_ms,
            .end_ms = segment.end_ms,
            .speaker = try dupOpt(alloc, segment.speaker),
            .words = try cloneWords(alloc, segment.words),
        };
    }
    return out;
}

fn cloneWords(alloc: Allocator, words: ?[]const WordTimestamp) !?[]WordTimestamp {
    const src = words orelse return null;
    const out = try alloc.alloc(WordTimestamp, src.len);
    errdefer alloc.free(out);
    for (src, 0..) |word, i| {
        out[i] = .{
            .word = try dupOpt(alloc, word.word),
            .start_ms = word.start_ms,
            .end_ms = word.end_ms,
        };
    }
    return out;
}

fn cloneSpeakers(alloc: Allocator, speakers: ?[]const Speaker) !?[]Speaker {
    const src = speakers orelse return null;
    const out = try alloc.alloc(Speaker, src.len);
    errdefer alloc.free(out);
    for (src, 0..) |speaker, i| {
        out[i] = .{
            .id = try dupOpt(alloc, speaker.id),
            .label = try dupOpt(alloc, speaker.label),
        };
    }
    return out;
}

fn dupOpt(alloc: Allocator, value: ?[]const u8) !?[]const u8 {
    return if (value) |v| try alloc.dupe(u8, v) else null;
}

fn freeOpt(alloc: Allocator, value: ?[]const u8) void {
    if (value) |v| alloc.free(v);
}

test "transcribing registry preserves named providers and default" {
    const alloc = std.testing.allocator;
    const raw =
        \\{
        \\  "whisper-local": { "provider": "termite", "api_url": "http://127.0.0.1:8080", "model": "openai/whisper-base" },
        \\  "whisper-remote": { "provider": "openai", "model": "whisper-1" }
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var registry = try Registry.parseFromValue(alloc, parsed.value);
    defer registry.deinit();

    try std.testing.expectEqualStrings("whisper-local", registry.defaultProviderName().?);
    const default_cfg = try registry.getConfig(null);
    try std.testing.expectEqual(Provider.termite, default_cfg.provider);
    try std.testing.expectEqualStrings("openai/whisper-base", default_cfg.model.?);

    const explicit_cfg = try registry.getConfig("whisper-remote");
    try std.testing.expectEqual(Provider.openai, explicit_cfg.provider);
    try std.testing.expectEqualStrings("whisper-1", explicit_cfg.model.?);
}

test "transcribing runtime loads termite provider and transcribes data uri input" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/transcribe", .respond = .{
            .body = "{\"object\":\"list\",\"data\":[{\"object\":\"transcription\",\"index\":0,\"text\":\"hello from termite\",\"language\":\"en\"}],\"model\":\"openai/whisper-base\",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":3,\"total_tokens\":3}}",
        } },
    });
    defer server.deinit();

    const api_url = try std.fmt.allocPrint(alloc, "{s}", .{server.baseUrl()});
    defer alloc.free(api_url);
    const raw =
        \\{
        \\  "whisper-local": { "provider": "termite", "api_url": "
    ;
    const suffix =
        \\", "model": "openai/whisper-base" }
        \\}
    ;
    const cfg_json = try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{ raw, api_url, suffix });
    defer alloc.free(cfg_json);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, cfg_json, .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var registry = try Registry.parseFromValue(alloc, parsed.value);
    defer registry.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.loadFromRegistry(&client, &registry);

    var response: ?Response = null;
    defer if (response) |*value| deinitResponse(alloc, value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, transcribing_runtime: *Runtime, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
            const transcriber = transcribing_runtime.get(null) catch |err| {
                err_out.* = err;
                return;
            };
            out.* = transcriber.transcribe(a, .{
                .url = "data:audio/wav;base64,ZmFrZQ==",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, &runtime, &response, &run_err }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("hello from termite", response.?.text.?);
    try std.testing.expectEqualStrings("en", response.?.language.?);
}

test "transcribing runtime loads openai provider and transcribes data uri input" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/audio/transcriptions", .respond = .{
            .body = "{\"text\":\"hello from openai\",\"language\":\"en\"}",
        } },
    });
    defer server.deinit();

    const base_url = try std.fmt.allocPrint(alloc, "{s}", .{server.baseUrl()});
    defer alloc.free(base_url);
    const cfg_json = try std.fmt.allocPrint(
        alloc,
        \\{{"whisper-remote":{{"provider":"openai","base_url":"{s}","api_key":"secret","model":"whisper-1"}}}}
    ,
        .{base_url},
    );
    defer alloc.free(cfg_json);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, cfg_json, .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var registry = try Registry.parseFromValue(alloc, parsed.value);
    defer registry.deinit();

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    var runtime = Runtime.init(alloc);
    defer runtime.deinit();
    try runtime.loadFromRegistry(&client, &registry);

    var response: ?Response = null;
    defer if (response) |*value| deinitResponse(alloc, value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, transcribing_runtime: *Runtime, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
            const transcriber = transcribing_runtime.get(null) catch |err| {
                err_out.* = err;
                return;
            };
            out.* = transcriber.transcribe(a, .{
                .url = "data:audio/mpeg;base64,ZmFrZQ==",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, &runtime, &response, &run_err }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("hello from openai", response.?.text.?);
    try std.testing.expectEqualStrings("en", response.?.language.?);
}
