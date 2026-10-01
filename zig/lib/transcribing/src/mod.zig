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
const inference_api = @import("inference_api");
const google_auth = @import("antfly_google").auth;
const scraping = @import("antfly_scraping");

const Allocator = std.mem.Allocator;
const vertex_auth_scope = "https://www.googleapis.com/auth/cloud-platform";

pub const Request = audio.STTRequest;
pub const Response = audio.STTResponse;
pub const Segment = audio.TranscriptSegment;
pub const WordTimestamp = audio.WordTimestamp;
pub const Speaker = audio.Speaker;

pub const Provider = enum {
    antfly,
    openai,
    vertex,

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(switch (self) {
            .antfly => "antfly",
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
    capability_token: ?[]const u8 = null,
    capability_revision: ?[]const u8 = null,
    /// Runtime-resolved Antfly capability; ignored by third-party providers.
    framed_attachments: bool = false,
    base_url: ?[]const u8 = null,
    url: ?[]const u8 = null,
    api_url: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    credentials_path: ?[]const u8 = null,
    language_code: ?[]const u8 = null,
    enable_automatic_punctuation: ?bool = null,
    use_enhanced: ?bool = null,
    /// Route-owned hard response ceiling for bounded orchestration.
    max_response_bytes: ?usize = null,
    /// Largest source recording fetched from a URL, in bytes. Defaults to
    /// `default_max_download_bytes`; the inference service applies its own
    /// media admission on top.
    max_download_bytes: ?usize = null,
    /// Ask the provider for timestamped segments (on by default where the
    /// provider supports them).
    timestamps: ?bool = null,
    /// Ask the provider for speaker labels where it supports them.
    diarization: ?bool = null,

    pub fn resolvedUrl(self: Config) ?[]const u8 {
        return self.url orelse self.api_url;
    }
};

/// A one hour recording is about 30 MB as 64 kbps AAC (a phone voice memo)
/// and about 58 MB as 128 kbps MP3 (a podcast), so the default fetch ceiling
/// covers those with room to spare; `Config.max_download_bytes` raises it.
pub const default_max_download_bytes: usize = 128 << 20;

/// The fetch ceiling for a provider, falling back to the default.
///
/// Zero is treated as unset. The schema's minimum is one byte and a ceiling
/// of zero rejects every recording, so it is always a client that filled in
/// a field it meant to leave out rather than an operator asking for it.
fn remoteFetchSecurity(max_download_bytes: ?usize) scraping.ContentSecurityConfig {
    const configured = if (max_download_bytes) |value| (if (value == 0) null else value) else null;
    return .{ .max_download_size_bytes = configured orelse default_max_download_bytes };
}

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

/// Every `?[]const u8` field of `Config` is owned text that a stored copy
/// must duplicate; every other field is a plain value. Walking the fields
/// keeps the two in step, so adding an option cannot silently drop it from
/// a registered provider (which is how `max_download_bytes` was lost).
fn configOwnsText(comptime T: type) bool {
    return T == ?[]const u8;
}

pub fn cloneConfig(alloc: Allocator, cfg: Config) !Config {
    var owned = cfg;
    inline for (@typeInfo(Config).@"struct".fields) |field| {
        if (comptime configOwnsText(field.type)) @field(owned, field.name) = null;
    }
    errdefer deinitConfig(alloc, &owned);
    inline for (@typeInfo(Config).@"struct".fields) |field| {
        if (comptime configOwnsText(field.type)) {
            @field(owned, field.name) = try dupOpt(alloc, @field(cfg, field.name));
        }
    }
    return owned;
}

pub fn deinitConfig(alloc: Allocator, cfg: *Config) void {
    inline for (@typeInfo(Config).@"struct".fields) |field| {
        if (comptime configOwnsText(field.type)) freeOpt(alloc, @field(cfg, field.name));
    }
    cfg.* = undefined;
}

/// The transcript as one line per phrase prefixed with its speaker label
/// (`SPEAKER_00: ...`) when diarization labelled any segment; otherwise the
/// plain text. Caller frees the result.
pub fn speakerAttributedTextAlloc(alloc: Allocator, response: *const Response) ![]u8 {
    const segments = response.segments orelse return try alloc.dupe(u8, response.text orelse "");
    var any_speaker = false;
    for (segments) |segment| {
        if (segment.speaker != null) any_speaker = true;
    }
    if (!any_speaker) return try alloc.dupe(u8, response.text orelse "");

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    var previous_speaker: ?[]const u8 = null;
    var line_open = false;
    for (segments) |segment| {
        const text = std.mem.trim(u8, segment.text orelse "", " \t\r\n");
        if (text.len == 0) continue;
        const same_speaker = if (segment.speaker) |speaker|
            (if (previous_speaker) |previous| std.mem.eql(u8, speaker, previous) else false)
        else
            previous_speaker == null and line_open;
        if (same_speaker) {
            try out.append(alloc, ' ');
        } else {
            if (line_open) try out.append(alloc, '\n');
            if (segment.speaker) |speaker| {
                try out.appendSlice(alloc, speaker);
                try out.appendSlice(alloc, ": ");
            }
            previous_speaker = segment.speaker;
            line_open = true;
        }
        try out.appendSlice(alloc, text);
    }
    return try out.toOwnedSlice(alloc);
}

test "speaker attributed text joins consecutive phrases of one speaker" {
    const segments = [_]Segment{
        .{ .text = "Hello there.", .speaker = "SPEAKER_00" },
        .{ .text = "How are you?", .speaker = "SPEAKER_00" },
        .{ .text = "Fine, thanks.", .speaker = "SPEAKER_01" },
        .{ .text = "(unlabelled)", .speaker = null },
        .{ .text = "Good.", .speaker = "SPEAKER_00" },
    };
    const response = Response{ .text = "ignored", .segments = &segments };
    const text = try speakerAttributedTextAlloc(std.testing.allocator, &response);
    defer std.testing.allocator.free(text);
    try std.testing.expectEqualStrings(
        "SPEAKER_00: Hello there. How are you?\nSPEAKER_01: Fine, thanks.\n(unlabelled)\nSPEAKER_00: Good.",
        text,
    );

    const plain = Response{ .text = "plain", .segments = &[_]Segment{.{ .text = "plain" }} };
    const plain_text = try speakerAttributedTextAlloc(std.testing.allocator, &plain);
    defer std.testing.allocator.free(plain_text);
    try std.testing.expectEqualStrings("plain", plain_text);
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

/// Frees one segment's owned text, speaker label and word list. Exposed so
/// callers that build segments themselves can unwind a partial batch with
/// the same rules the response teardown uses.
pub fn deinitSegment(alloc: Allocator, segment: *Segment) void {
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

pub const RemoteOptions = struct {
    source_table: []const u8 = "",
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,
};

fn initTranscriber(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
    return initTranscriberWithOptions(alloc, http, cfg, .{});
}

fn initTranscriberWithOptions(alloc: Allocator, http: *httpx.Client, cfg: Config, options: RemoteOptions) !Transcriber {
    return switch (cfg.provider) {
        .antfly => try AntflyTranscriberState.init(alloc, http, cfg, options),
        .openai => try OpenAiTranscriberState.init(alloc, http, cfg),
        .vertex => try VertexTranscriberState.init(alloc, http, cfg),
    };
}

pub fn transcribeWithConfig(
    alloc: Allocator,
    http: *httpx.Client,
    cfg: Config,
    request: Request,
    options: RemoteOptions,
) !Response {
    const transcriber = try initTranscriberWithOptions(alloc, http, cfg, options);
    defer transcriber.deinit();
    return try transcriber.transcribe(alloc, request);
}

const AntflyTranscriberState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    api_url: []const u8,
    auth_header: ?[2][]const u8 = null,
    capability_token: ?[]const u8 = null,
    capability_revision: ?[]const u8 = null,
    source_table: ?[]u8 = null,
    model: []const u8,
    language_code: ?[]const u8 = null,
    max_response_bytes: ?usize = null,
    max_download_bytes: ?usize = null,
    timeout_ms: ?u64 = null,
    cancellation: ?httpx.CancellationToken = null,
    framed_attachments: bool = false,
    /// Provider-level default for speaker labels; a request may override it.
    diarization: ?bool = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config, options: RemoteOptions) !Transcriber {
        const configured_model = cfg.model orelse return error.InvalidTranscribingConfig;
        const model_name = std.mem.trim(u8, configured_model, " \t\r\n");
        if (model_name.len == 0) return error.InvalidTranscribingConfig;
        const state = try alloc.create(AntflyTranscriberState);
        errdefer alloc.destroy(state);

        const api_url = try alloc.dupe(u8, cfg.resolvedUrl() orelse "http://127.0.0.1:8080");
        errdefer alloc.free(api_url);
        const model = try alloc.dupe(u8, model_name);
        errdefer alloc.free(model);
        const capability_token = try dupOpt(alloc, cfg.capability_token);
        errdefer freeOpt(alloc, capability_token);
        const capability_revision = try dupOpt(alloc, cfg.capability_revision);
        errdefer freeOpt(alloc, capability_revision);
        const source_table = if (options.source_table.len > 0)
            try alloc.dupe(u8, options.source_table)
        else
            null;
        errdefer freeOpt(alloc, source_table);
        const language_code = try dupOpt(alloc, cfg.language_code);
        errdefer freeOpt(alloc, language_code);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .api_url = api_url,
            .model = model,
            .capability_token = capability_token,
            .capability_revision = capability_revision,
            .source_table = source_table,
            .language_code = language_code,
            .max_response_bytes = cfg.max_response_bytes,
            .max_download_bytes = cfg.max_download_bytes,
            .timeout_ms = options.timeout_ms,
            .cancellation = options.cancellation,
            .framed_attachments = cfg.framed_attachments,
            .diarization = cfg.diarization,
        };
        if (cfg.bearer_token orelse cfg.api_key) |token| {
            try state.setBearer(token);
        }

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
        self.alloc.free(self.model);
        freeOpt(self.alloc, self.language_code);
        freeOpt(self.alloc, self.capability_token);
        freeOpt(self.alloc, self.capability_revision);
        freeOpt(self.alloc, self.source_table);
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.alloc.destroy(self);
    }

    fn setBearer(self: *AntflyTranscriberState, token: []const u8) !void {
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.auth_header = .{
            "Authorization",
            try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{token}),
        };
    }

    fn transcribe(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *AntflyTranscriberState = @ptrCast(@alignCast(ptr));
        var audio_content = try resolveAudioContentAlloc(alloc, req.url, self.max_download_bytes);
        defer audio_content.deinit(alloc);
        var encoded: ?[]u8 = null;
        defer if (encoded) |value| alloc.free(value);
        const audio_field = if (self.framed_attachments)
            "attachment:0"
        else blk: {
            const encoded_len = std.base64.standard.Encoder.calcSize(audio_content.data.len);
            encoded = try alloc.alloc(u8, encoded_len);
            _ = std.base64.standard.Encoder.encode(encoded.?, audio_content.data);
            break :blk encoded.?;
        };

        const url = try std.fmt.allocPrint(alloc, "{s}/transcribe", .{self.api_url});
        defer alloc.free(url);

        const body = try httpx.json.Json.stringify(alloc, inference_api.types.TranscribeRequest{
            .model = self.model,
            .audio = audio_field,
            .language = req.language orelse self.language_code,
            // Local and remote execution must honour the same options: the
            // inference node cannot label speakers it was never asked for.
            .diarization = req.diarization orelse self.diarization,
        });
        defer alloc.free(body);

        var header_buf: [5][2][]const u8 = undefined;
        var header_count: usize = 0;
        if (self.auth_header) |header| {
            header_buf[header_count] = header;
            header_count += 1;
        }
        if (self.source_table) |source_table| {
            header_buf[header_count] = .{ "X-Antfly-Source-Table", source_table };
            header_count += 1;
        }
        if (self.capability_token) |token| {
            header_buf[header_count] = .{ "X-Antfly-Capability-Token", token };
            header_count += 1;
        }
        if (self.capability_revision) |revision| {
            header_buf[header_count] = .{ "X-Antfly-Capability-Revision", revision };
            header_count += 1;
        }
        var framed_body: ?httpx.attachment_envelope.EncodedSegments = null;
        defer if (framed_body) |*value| value.deinit();
        if (self.framed_attachments) {
            framed_body = try httpx.attachment_envelope.encodeSegmentsAlloc(alloc, body, &.{.{
                .mime_type = audio_content.content_type,
                .data = audio_content.data,
            }});
            header_buf[header_count] = .{ "Content-Type", httpx.attachment_envelope.content_type };
            header_count += 1;
        }
        const headers = header_buf[0..header_count];
        var resp = try self.http.post(url, .{
            .json = if (self.framed_attachments) null else body,
            .borrowed_body_segments = if (framed_body) |value| value.segments else null,
            .headers = headers,
            .timeout_ms = self.timeout_ms,
            .max_response_size = self.max_response_bytes,
            .cancellation = self.cancellation,
        });
        defer resp.deinit();
        if (!resp.ok()) {
            const stale = resp.headers.get("X-Antfly-Capability-Stale");
            if (resp.status.code == 409 and stale != null and
                std.ascii.eqlIgnoreCase(std.mem.trim(u8, stale.?, " \t"), "true"))
                return error.InferenceCapabilitiesStale;
            return error.TranscribeRequestFailed;
        }

        const payload = resp.body orelse return error.EmptyResponse;
        var parsed = try std.json.parseFromSlice(inference_api.types.TranscribeResponse, alloc, payload, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        const first = if (parsed.value.data.len > 0) parsed.value.data[0] else return error.EmptyResponse;
        var response = Response{
            .text = try alloc.dupe(u8, first.text),
            .duration_ms = first.duration_ms,
        };
        errdefer deinitResponse(alloc, &response);
        response.language = try dupOpt(alloc, first.language);
        if (first.segments) |api_segments| response.segments = try antflySegmentsAlloc(alloc, api_segments);
        if (first.speakers) |api_speakers| response.speakers = try antflySpeakersAlloc(alloc, api_speakers);
        return response;
    }
};

/// The inference service's timestamped phrases as shared transcript segments.
fn antflySegmentsAlloc(alloc: Allocator, api_segments: []const inference_api.types.DictationSegment) ![]Segment {
    const segments = try alloc.alloc(Segment, api_segments.len);
    var filled: usize = 0;
    errdefer {
        for (segments[0..filled]) |segment| {
            var owned = segment;
            deinitSegment(alloc, &owned);
        }
        alloc.free(segments);
    }
    for (api_segments, 0..) |api_segment, i| {
        const words = try alloc.alloc(WordTimestamp, api_segment.words.len);
        var words_filled: usize = 0;
        errdefer {
            for (words[0..words_filled]) |word| {
                var owned = word;
                deinitWordTimestamp(alloc, &owned);
            }
            alloc.free(words);
        }
        for (api_segment.words, 0..) |api_word, j| {
            words[j] = .{
                .word = try alloc.dupe(u8, api_word.word),
                .start_ms = api_word.start_ms,
                .end_ms = api_word.end_ms,
            };
            words_filled += 1;
        }
        segments[i] = .{
            .text = try alloc.dupe(u8, api_segment.text),
            .start_ms = api_segment.start_ms,
            .end_ms = api_segment.end_ms,
            .words = words,
            .speaker = try dupOpt(alloc, api_segment.speaker),
        };
        filled += 1;
    }
    return segments;
}

/// The speakers a diarized inference response reports, in its order.
fn antflySpeakersAlloc(alloc: Allocator, api_speakers: []const []const u8) ![]Speaker {
    const speakers = try alloc.alloc(Speaker, api_speakers.len);
    var filled: usize = 0;
    errdefer {
        for (speakers[0..filled]) |speaker| {
            var owned = speaker;
            deinitSpeaker(alloc, &owned);
        }
        alloc.free(speakers);
    }
    for (api_speakers, 0..) |label, i| {
        const id = try alloc.dupe(u8, label);
        errdefer alloc.free(id);
        speakers[i] = .{ .id = id, .label = try alloc.dupe(u8, label) };
        filled += 1;
    }
    return speakers;
}

const OpenAiTranscriberState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,
    model: []const u8,
    language_code: ?[]const u8 = null,
    max_response_bytes: ?usize = null,
    max_download_bytes: ?usize = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
        const state = try alloc.create(OpenAiTranscriberState);
        errdefer alloc.destroy(state);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .base_url = try alloc.dupe(u8, cfg.base_url orelse "https://api.openai.com/v1"),
            .model = try alloc.dupe(u8, cfg.model orelse "whisper-1"),
            .language_code = try dupOpt(alloc, cfg.language_code),
            .max_response_bytes = cfg.max_response_bytes,
            .max_download_bytes = cfg.max_download_bytes,
        };
        if (cfg.bearer_token orelse cfg.api_key) |token| try state.setBearer(token);

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

    fn setBearer(self: *OpenAiTranscriberState, token: []const u8) !void {
        if (self.auth_header) |header| self.alloc.free(header[1]);
        self.auth_header = .{
            "Authorization",
            try std.fmt.allocPrint(self.alloc, "Bearer {s}", .{token}),
        };
    }

    fn transcribe(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *OpenAiTranscriberState = @ptrCast(@alignCast(ptr));
        const audio_bytes = try resolveAudioInputAlloc(alloc, req.url, self.max_download_bytes);
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
            .max_response_size = self.max_response_bytes,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.TranscribeRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        return try parseOpenAiVerboseResponseAlloc(alloc, payload);
    }
};

/// An OpenAI transcription response. `verbose_json` carries language,
/// duration and segments (seconds become milliseconds here); `json` carries
/// only `text`, and every other field stays absent.
fn parseOpenAiVerboseResponseAlloc(alloc: Allocator, payload: []const u8) !Response {
    const Body = struct {
        text: ?[]const u8 = null,
        language: ?[]const u8 = null,
        duration: ?f64 = null,
        segments: []const struct {
            start: f64 = 0,
            end: f64 = 0,
            text: ?[]const u8 = null,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(Body, alloc, payload, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    var response = Response{
        .text = try dupOpt(alloc, parsed.value.text),
        .duration_ms = if (parsed.value.duration) |seconds| secondsToMs(seconds) else null,
    };
    errdefer deinitResponse(alloc, &response);
    response.language = try dupOpt(alloc, parsed.value.language);
    if (parsed.value.segments.len > 0) {
        const segments = try alloc.alloc(Segment, parsed.value.segments.len);
        var filled: usize = 0;
        errdefer {
            for (segments[0..filled]) |segment| {
                var owned = segment;
                deinitSegment(alloc, &owned);
            }
            alloc.free(segments);
        }
        for (parsed.value.segments, 0..) |segment, i| {
            segments[i] = .{
                .text = try dupOpt(alloc, if (segment.text) |text| std.mem.trim(u8, text, " ") else null),
                .start_ms = secondsToMs(segment.start),
                .end_ms = secondsToMs(segment.end),
            };
            filled += 1;
        }
        response.segments = segments;
    }
    return response;
}

fn secondsToMs(seconds: f64) i64 {
    if (!std.math.isFinite(seconds) or seconds <= 0) return 0;
    return @intFromFloat(@round(seconds * 1000.0));
}

const VertexTranscriberState = struct {
    alloc: Allocator,
    http: *httpx.Client,
    base_url: []const u8,
    auth_header: ?[2][]const u8 = null,
    token_source: ?*google_auth.CachedTokenSource = null,
    project_id: []const u8,
    location: []const u8,
    model: []const u8,
    language_code: []const u8,
    enable_automatic_punctuation: ?bool = null,
    max_response_bytes: ?usize = null,
    max_download_bytes: ?usize = null,
    /// Provider-level defaults for the request options. A request may
    /// override either; a registered provider that asks for speaker labels
    /// must get them even when the caller says nothing about them.
    timestamps: ?bool = null,
    diarization: ?bool = null,

    fn init(alloc: Allocator, http: *httpx.Client, cfg: Config) !Transcriber {
        const state = try alloc.create(VertexTranscriberState);
        errdefer alloc.destroy(state);

        state.* = .{
            .alloc = alloc,
            .http = http,
            .base_url = try alloc.dupe(u8, cfg.base_url orelse "https://speech.googleapis.com/v2"),
            .project_id = if (cfg.project_id) |value| try alloc.dupe(u8, value) else (try vertexProjectIdFromConfigAlloc(alloc, cfg.credentials_path) orelse return error.InvalidTranscribingConfig),
            .location = try alloc.dupe(u8, cfg.location orelse "global"),
            .model = try alloc.dupe(u8, cfg.model orelse "latest_long"),
            .language_code = try alloc.dupe(u8, cfg.language_code orelse "en-US"),
            .enable_automatic_punctuation = cfg.enable_automatic_punctuation,
            .max_response_bytes = cfg.max_response_bytes,
            .max_download_bytes = cfg.max_download_bytes,
            .timestamps = cfg.timestamps,
            .diarization = cfg.diarization,
        };
        errdefer state.deinitState();

        if (cfg.bearer_token orelse cfg.api_key) |token| {
            try state.setBearer(token);
        } else {
            state.token_source = try initVertexTokenSource(alloc, cfg.credentials_path);
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
        if (self.token_source) |source| {
            source.deinit();
            self.alloc.destroy(source);
        }
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

    fn appendAuthHeaders(
        self: *VertexTranscriberState,
        alloc: Allocator,
        headers: *std.ArrayList([2][]const u8),
        minted_auth: *?[]u8,
    ) !void {
        if (self.auth_header) |header| {
            try headers.append(alloc, header);
            return;
        }
        if (self.token_source) |source| {
            minted_auth.* = try source.authorizationValueAlloc(alloc);
            try headers.append(alloc, .{ "Authorization", minted_auth.*.? });
        }
    }

    fn transcribe(ptr: *anyopaque, alloc: Allocator, req: Request) anyerror!Response {
        const self: *VertexTranscriberState = @ptrCast(@alignCast(ptr));
        const audio_bytes = try resolveAudioInputAlloc(alloc, req.url, self.max_download_bytes);
        defer alloc.free(audio_bytes);

        const encoded_len = std.base64.standard.Encoder.calcSize(audio_bytes.len);
        const encoded = try alloc.alloc(u8, encoded_len);
        defer alloc.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, audio_bytes);

        const language = req.language orelse self.language_code;
        const DiarizationConfig = struct {
            minSpeakerCount: u32 = 1,
            maxSpeakerCount: u32 = 6,
        };
        const RecognitionFeatures = struct {
            enableWordTimeOffsets: bool,
            enableAutomaticPunctuation: ?bool = null,
            diarizationConfig: ?DiarizationConfig = null,
        };
        const RecognitionConfig = struct {
            explicitDecodingConfig: struct {} = .{},
            model: []const u8,
            languageCodes: []const []const u8,
            features: RecognitionFeatures,
        };
        const RequestBody = struct {
            config: RecognitionConfig,
            content: []const u8,
        };
        const languages = [_][]const u8{language};
        // Word offsets are what segment timing is rebuilt from, so they are
        // requested whenever timestamps are wanted; speaker labels ride on
        // the same words when diarization is on.
        // Request first, then what the provider was registered with, then
        // the default.
        const want_timestamps = req.timestamps orelse self.timestamps orelse true;
        const want_diarization = req.diarization orelse self.diarization orelse false;
        const body = try httpx.json.Json.stringifyRequest(alloc, RequestBody{
            .config = .{
                .model = self.model,
                .languageCodes = &languages,
                .features = .{
                    .enableWordTimeOffsets = want_timestamps or want_diarization,
                    .enableAutomaticPunctuation = self.enable_automatic_punctuation,
                    .diarizationConfig = if (want_diarization) DiarizationConfig{} else null,
                },
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
        var minted_auth: ?[]u8 = null;
        defer if (minted_auth) |value| alloc.free(value);
        try self.appendAuthHeaders(alloc, &headers, &minted_auth);

        var resp = try self.http.post(url, .{
            .json = body,
            .headers = headers.items,
            .max_response_size = self.max_response_bytes,
        });
        defer resp.deinit();
        if (!resp.ok()) return error.TranscribeRequestFailed;

        const payload = resp.body orelse return error.EmptyResponse;
        return try parseVertexRecognizeResponseAlloc(alloc, payload);
    }
};

/// A word as Speech-to-Text v2 reports it: offsets are protobuf durations
/// (`"1.500s"`), and `speakerLabel` is present only with diarization.
const VertexWord = struct {
    startOffset: ?[]const u8 = null,
    endOffset: ?[]const u8 = null,
    word: ?[]const u8 = null,
    speakerLabel: ?[]const u8 = null,
};

/// Speech-to-Text v2 `recognize` answers with one result per utterance. The
/// transcript is the results joined in order; when word offsets are present
/// each result becomes a timed segment, split further wherever the speaker
/// label changes, so diarized audio yields one segment per speaker turn.
fn parseVertexRecognizeResponseAlloc(alloc: Allocator, payload: []const u8) !Response {
    const ResponseBody = struct {
        results: []const struct {
            alternatives: []const struct {
                transcript: ?[]const u8 = null,
                words: []const VertexWord = &.{},
            } = &.{},
            languageCode: ?[]const u8 = null,
            resultEndOffset: ?[]const u8 = null,
        } = &.{},
    };
    var parsed = try std.json.parseFromSlice(ResponseBody, alloc, payload, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const results = parsed.value.results;
    if (results.len == 0 or results[0].alternatives.len == 0) return error.EmptyResponse;

    var text = std.ArrayListUnmanaged(u8).empty;
    defer text.deinit(alloc);
    var segments = std.ArrayListUnmanaged(Segment).empty;
    errdefer {
        for (segments.items) |*segment| deinitSegment(alloc, segment);
        segments.deinit(alloc);
    }
    var last_end_ms: i64 = 0;
    var speakers_seen = std.StringArrayHashMapUnmanaged(void).empty;
    defer speakers_seen.deinit(alloc);

    for (results) |result| {
        if (result.alternatives.len == 0) continue;
        const alternative = result.alternatives[0];
        const transcript = std.mem.trim(u8, alternative.transcript orelse "", " ");
        if (transcript.len > 0) {
            if (text.items.len > 0) try text.append(alloc, ' ');
            try text.appendSlice(alloc, transcript);
        }
        if (alternative.words.len == 0) {
            if (transcript.len == 0) continue;
            const end_ms = if (result.resultEndOffset) |offset| vertexDurationToMs(offset) orelse last_end_ms else last_end_ms;
            try segments.append(alloc, .{
                .text = try alloc.dupe(u8, transcript),
                .start_ms = last_end_ms,
                .end_ms = @max(end_ms, last_end_ms),
            });
            last_end_ms = @max(end_ms, last_end_ms);
            continue;
        }
        // One segment per run of words with the same speaker label. Each
        // run's text is sliced out of the provider's own transcript rather
        // than rebuilt from its words: rebuilding would join with spaces,
        // which is wrong for a language that does not write them (a Chinese
        // transcript would come back with spaces inserted) and loses the
        // transcript's punctuation. The text has to be a verbatim substring
        // of the transcript, because that is how the enrichment locates a
        // phrase to give it timing and a speaker.
        var transcript_cursor: usize = 0;
        var run_start: usize = 0;
        while (run_start < alternative.words.len) {
            const speaker = alternative.words[run_start].speakerLabel;
            var run_end = run_start + 1;
            while (run_end < alternative.words.len and sameSpeaker(alternative.words[run_end].speakerLabel, speaker)) : (run_end += 1) {}
            const run = alternative.words[run_start..run_end];

            var phrase = std.ArrayListUnmanaged(u8).empty;
            defer phrase.deinit(alloc);
            var words = try alloc.alloc(WordTimestamp, run.len);
            var filled: usize = 0;
            errdefer {
                for (words[0..filled]) |*word| deinitWordTimestamp(alloc, word);
                alloc.free(words);
            }
            var slice_start: ?usize = null;
            var slice_end: usize = transcript_cursor;
            var scan = transcript_cursor;
            for (run) |word| {
                const spelled = std.mem.trim(u8, word.word orelse "", " ");
                if (phrase.items.len > 0 and spelled.len > 0) try phrase.append(alloc, ' ');
                try phrase.appendSlice(alloc, spelled);
                if (spelled.len > 0) {
                    if (std.mem.indexOfPos(u8, transcript, scan, spelled)) |found| {
                        if (slice_start == null) slice_start = found;
                        scan = found + spelled.len;
                        slice_end = scan;
                    }
                }
                const start_ms = if (word.startOffset) |offset| vertexDurationToMs(offset) orelse last_end_ms else last_end_ms;
                const end_ms = if (word.endOffset) |offset| vertexDurationToMs(offset) orelse start_ms else start_ms;
                words[filled] = .{
                    .word = try alloc.dupe(u8, spelled),
                    .start_ms = start_ms,
                    .end_ms = @max(end_ms, start_ms),
                };
                filled += 1;
                last_end_ms = @max(last_end_ms, end_ms);
            }
            const start_ms = words[0].start_ms orelse last_end_ms;
            const end_ms = words[filled - 1].end_ms orelse start_ms;
            // Fall back to the joined words only when the run cannot be
            // found in the transcript at all, which means the provider's
            // words and transcript disagree.
            const segment_text = if (slice_start) |begin| blk: {
                transcript_cursor = slice_end;
                break :blk try alloc.dupe(u8, transcript[begin..slice_end]);
            } else try phrase.toOwnedSlice(alloc);
            errdefer alloc.free(segment_text);
            const speaker_id = try dupOpt(alloc, speaker);
            errdefer freeOpt(alloc, speaker_id);
            if (speaker) |label| _ = try speakers_seen.getOrPut(alloc, label);
            try segments.append(alloc, .{
                .text = segment_text,
                .start_ms = start_ms,
                .end_ms = end_ms,
                .speaker = speaker_id,
                .words = words,
            });
            run_start = run_end;
        }
    }

    var response = Response{
        .text = try alloc.dupe(u8, text.items),
        .language = try dupOpt(alloc, results[0].languageCode),
        .duration_ms = if (last_end_ms > 0) last_end_ms else null,
    };
    errdefer deinitResponse(alloc, &response);
    if (segments.items.len > 0) response.segments = try segments.toOwnedSlice(alloc);
    if (speakers_seen.count() > 0) {
        const speakers = try alloc.alloc(Speaker, speakers_seen.count());
        var filled: usize = 0;
        errdefer {
            for (speakers[0..filled]) |*speaker| deinitSpeaker(alloc, speaker);
            alloc.free(speakers);
        }
        for (speakers_seen.keys()) |label| {
            speakers[filled] = .{ .id = try alloc.dupe(u8, label), .label = try alloc.dupe(u8, label) };
            filled += 1;
        }
        response.speakers = speakers;
    }
    return response;
}

fn sameSpeaker(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

/// Parses a protobuf JSON duration such as `"1.500s"` or `"12s"` into
/// milliseconds; anything else is treated as absent.
fn vertexDurationToMs(text: []const u8) ?i64 {
    const trimmed = std.mem.trimEnd(u8, text, "s");
    if (trimmed.len == 0 or trimmed.len == text.len) return null;
    const seconds = std.fmt.parseFloat(f64, trimmed) catch return null;
    return secondsToMs(seconds);
}

test "vertex recognize response yields speaker turns with word timing" {
    const alloc = std.testing.allocator;
    var response = try parseVertexRecognizeResponseAlloc(alloc,
        \\{"results":[{"alternatives":[{"transcript":"hello there how are you",
        \\"words":[{"startOffset":"0s","endOffset":"0.400s","word":"hello","speakerLabel":"1"},
        \\{"startOffset":"0.400s","endOffset":"0.900s","word":"there","speakerLabel":"1"},
        \\{"startOffset":"1.200s","endOffset":"1.500s","word":"how","speakerLabel":"2"},
        \\{"startOffset":"1.500s","endOffset":"1.700s","word":"are","speakerLabel":"2"},
        \\{"startOffset":"1.700s","endOffset":"2s","word":"you","speakerLabel":"2"}]}],
        \\"languageCode":"en-US","resultEndOffset":"2s"},
        \\{"alternatives":[{"transcript":"fine thanks"}],"resultEndOffset":"3.250s"}]}
    );
    defer deinitResponse(alloc, &response);
    try std.testing.expectEqualStrings("hello there how are you fine thanks", response.text.?);
    try std.testing.expectEqualStrings("en-US", response.language.?);
    try std.testing.expectEqual(@as(?i64, 3250), response.duration_ms);
    const segments = response.segments.?;
    try std.testing.expectEqual(@as(usize, 3), segments.len);
    try std.testing.expectEqualStrings("hello there", segments[0].text.?);
    try std.testing.expectEqual(@as(?i64, 0), segments[0].start_ms);
    try std.testing.expectEqual(@as(?i64, 900), segments[0].end_ms);
    try std.testing.expectEqualStrings("1", segments[0].speaker.?);
    try std.testing.expectEqual(@as(usize, 2), segments[0].words.?.len);
    try std.testing.expectEqualStrings("how are you", segments[1].text.?);
    try std.testing.expectEqual(@as(?i64, 1200), segments[1].start_ms);
    try std.testing.expectEqual(@as(?i64, 2000), segments[1].end_ms);
    try std.testing.expectEqualStrings("2", segments[1].speaker.?);
    // A result without word offsets still becomes a segment spanning from the
    // previous end to its own end offset.
    try std.testing.expectEqualStrings("fine thanks", segments[2].text.?);
    try std.testing.expectEqual(@as(?i64, 2000), segments[2].start_ms);
    try std.testing.expectEqual(@as(?i64, 3250), segments[2].end_ms);
    try std.testing.expectEqual(@as(?[]const u8, null), segments[2].speaker);
    try std.testing.expectEqual(@as(usize, 2), response.speakers.?.len);
    try std.testing.expectEqualStrings("2", response.speakers.?[1].id.?);
}

test "vertex segments are sliced from the transcript, not rebuilt from words" {
    const alloc = std.testing.allocator;
    // Chinese is written without spaces between words. Rebuilding a phrase
    // by joining the provider's words would insert them, and the enrichment
    // locates a phrase by searching the transcript for it verbatim, so the
    // whole transcript would lose its timing and speaker attribution.
    var response = try parseVertexRecognizeResponseAlloc(alloc,
        \\{"results":[{"alternatives":[{"transcript":"你好世界再见",
        \\"words":[{"startOffset":"0s","endOffset":"0.400s","word":"你好","speakerLabel":"1"},
        \\{"startOffset":"0.400s","endOffset":"0.900s","word":"世界","speakerLabel":"1"},
        \\{"startOffset":"1s","endOffset":"1.400s","word":"再见","speakerLabel":"2"}]}],
        \\"languageCode":"cmn-Hans-CN","resultEndOffset":"1.400s"}]}
    );
    defer deinitResponse(alloc, &response);

    const text = response.text.?;
    try std.testing.expectEqualStrings("你好世界再见", text);
    const segments = response.segments.?;
    try std.testing.expectEqual(@as(usize, 2), segments.len);
    try std.testing.expectEqualStrings("你好世界", segments[0].text.?);
    try std.testing.expectEqualStrings("再见", segments[1].text.?);
    // Which is what makes both phrases findable in the transcript, the
    // property the enrichment's span lookup depends on.
    var cursor: usize = 0;
    for (segments) |segment| {
        const found = std.mem.indexOfPos(u8, text, cursor, segment.text.?) orelse
            return error.PhraseNotFoundInTranscript;
        cursor = found + segment.text.?.len;
    }
    // The words themselves keep their own timing and speaker runs.
    try std.testing.expectEqual(@as(?i64, 0), segments[0].start_ms);
    try std.testing.expectEqual(@as(?i64, 900), segments[0].end_ms);
    try std.testing.expectEqualStrings("1", segments[0].speaker.?);
    try std.testing.expectEqualStrings("2", segments[1].speaker.?);
}

test "vertex segments keep the transcript's punctuation" {
    const alloc = std.testing.allocator;
    // Vertex reports words without punctuation but punctuates the
    // transcript, so a rebuilt phrase would not appear in it verbatim.
    var response = try parseVertexRecognizeResponseAlloc(alloc,
        \\{"results":[{"alternatives":[{"transcript":"Hello, there! How are you?",
        \\"words":[{"startOffset":"0s","endOffset":"0.400s","word":"Hello","speakerLabel":"1"},
        \\{"startOffset":"0.400s","endOffset":"0.900s","word":"there","speakerLabel":"1"},
        \\{"startOffset":"1s","endOffset":"1.200s","word":"How","speakerLabel":"2"},
        \\{"startOffset":"1.200s","endOffset":"1.400s","word":"are","speakerLabel":"2"},
        \\{"startOffset":"1.400s","endOffset":"1.600s","word":"you","speakerLabel":"2"}]}],
        \\"languageCode":"en-US","resultEndOffset":"1.600s"}]}
    );
    defer deinitResponse(alloc, &response);
    const segments = response.segments.?;
    try std.testing.expectEqual(@as(usize, 2), segments.len);
    try std.testing.expectEqualStrings("Hello, there", segments[0].text.?);
    try std.testing.expectEqualStrings("How are you", segments[1].text.?);
    const text = response.text.?;
    try std.testing.expect(std.mem.indexOf(u8, text, segments[0].text.?) != null);
    try std.testing.expect(std.mem.indexOf(u8, text, segments[1].text.?) != null);
}

test "vertex applies request, then provider config, then default" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    // A provider registered with diarization on must send it even when the
    // request says nothing, and a request may still override it.
    const Case = struct {
        config_diarization: ?bool,
        config_timestamps: ?bool,
        request_diarization: ?bool,
        request_timestamps: ?bool,
        expect_diarization: bool,
        expect_word_offsets: bool,
    };
    const cases = [_]Case{
        .{ .config_diarization = true, .config_timestamps = null, .request_diarization = null, .request_timestamps = null, .expect_diarization = true, .expect_word_offsets = true },
        .{ .config_diarization = null, .config_timestamps = null, .request_diarization = null, .request_timestamps = null, .expect_diarization = false, .expect_word_offsets = true },
        .{ .config_diarization = null, .config_timestamps = false, .request_diarization = null, .request_timestamps = null, .expect_diarization = false, .expect_word_offsets = false },
        .{ .config_diarization = true, .config_timestamps = null, .request_diarization = false, .request_timestamps = null, .expect_diarization = false, .expect_word_offsets = true },
        .{ .config_diarization = null, .config_timestamps = false, .request_diarization = null, .request_timestamps = true, .expect_diarization = false, .expect_word_offsets = true },
    };

    for (cases) |case| {
        var server = try httpx.TestServer.start(alloc, io, &.{.{
            .method = .POST,
            .path = "/projects/p/locations/global/recognizers/_:recognize",
            .assert_request = captureVertexRequestBody,
            .respond = .{ .body = "{\"results\":[{\"alternatives\":[{\"transcript\":\"hi\"}],\"languageCode\":\"en-US\"}]}" },
        }});
        defer server.deinit();
        const endpoint = try std.fmt.allocPrint(alloc, "{s}", .{server.baseUrl()});
        defer alloc.free(endpoint);
        var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        defer client.deinit();

        var response: ?Response = null;
        defer if (response) |*value| deinitResponse(alloc, value);
        var run_err: ?anyerror = null;
        var group = std.Io.Group.init;
        const Fiber = struct {
            fn run(a: Allocator, http: *httpx.Client, url: []const u8, c: Case, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
                out.* = transcribeWithConfig(a, http, .{
                    .provider = .vertex,
                    .base_url = url,
                    .project_id = "p",
                    .api_key = "token",
                    .timestamps = c.config_timestamps,
                    .diarization = c.config_diarization,
                }, .{
                    .url = "data:audio/wav;base64,ZmFrZQ==",
                    .timestamps = c.request_timestamps,
                    .diarization = c.request_diarization,
                }, .{}) catch |err| {
                    err_out.* = err;
                    return;
                };
            }
        };
        vertex_request_body_len = 0;
        group.concurrent(io, Fiber.run, .{ alloc, &client, endpoint, case, &response, &run_err }) catch return;
        try server.handleOne();
        group.await(io) catch {};
        if (run_err) |err| return err;

        const sent = vertex_request_body[0..vertex_request_body_len];
        const sent_diarization = std.mem.indexOf(u8, sent, "\"diarizationConfig\"") != null;
        try std.testing.expectEqual(case.expect_diarization, sent_diarization);
        const sent_word_offsets = std.mem.indexOf(u8, sent, "\"enableWordTimeOffsets\":true") != null;
        try std.testing.expectEqual(case.expect_word_offsets, sent_word_offsets);
    }
}

/// The body of the last request the Vertex fixture server received.
var vertex_request_body: [8192]u8 = undefined;
var vertex_request_body_len: usize = 0;

fn captureVertexRequestBody(req: httpx.testing_mod.RequestInfo) !void {
    vertex_request_body_len = @min(req.body.len, vertex_request_body.len);
    @memcpy(vertex_request_body[0..vertex_request_body_len], req.body[0..vertex_request_body_len]);
}

test "a zero download ceiling is treated as unset" {
    // A generated client that serializes every field sends
    // max_download_bytes: 0, which as a literal ceiling rejects every
    // recording. Zero is below the schema's minimum, so it means "unset".
    const zero = remoteFetchSecurity(0);
    try std.testing.expectEqual(default_max_download_bytes, zero.max_download_size_bytes);
    const unset = remoteFetchSecurity(null);
    try std.testing.expectEqual(default_max_download_bytes, unset.max_download_size_bytes);
    const configured = remoteFetchSecurity(4096);
    try std.testing.expectEqual(@as(usize, 4096), configured.max_download_size_bytes);
}

test "vertex durations parse protobuf seconds" {
    try std.testing.expectEqual(@as(?i64, 1500), vertexDurationToMs("1.500s"));
    try std.testing.expectEqual(@as(?i64, 12000), vertexDurationToMs("12s"));
    try std.testing.expectEqual(@as(?i64, null), vertexDurationToMs("12"));
    try std.testing.expectEqual(@as(?i64, null), vertexDurationToMs("s"));
}

fn initVertexTokenSource(alloc: Allocator, credentials_path: ?[]const u8) !*google_auth.CachedTokenSource {
    var cfg = if (credentials_path) |path| blk: {
        break :blk google_auth.configFromFileAlloc(alloc, path, vertex_auth_scope) catch return error.MissingVertexCredentials;
    } else google_auth.configFromEnvAlloc(alloc, vertex_auth_scope) catch return error.MissingVertexCredentials;
    errdefer cfg.deinit(alloc);

    const source = try alloc.create(google_auth.CachedTokenSource);
    errdefer alloc.destroy(source);
    source.* = try google_auth.CachedTokenSource.init(alloc, cfg);
    return source;
}

fn vertexProjectIdFromConfigAlloc(alloc: Allocator, credentials_path: ?[]const u8) !?[]u8 {
    if (credentials_path) |path| {
        return google_auth.projectIdFromFileAlloc(alloc, path) catch null;
    }
    return try google_auth.projectIdFromDefaultCredentialsAlloc(alloc);
}

const MultipartBody = struct {
    content_type: []u8,
    body: []u8,
};

/// OpenAI's transcription models do not share one response format.
/// `whisper-1` returns `verbose_json`, which carries the segment timing this
/// library maps to `Response.segments`; the GPT-4o transcription models
/// reject that format and accept `json`, which carries the transcript alone.
/// Asking the wrong one of them fails the request outright, so the format
/// follows the configured model and timing is best-effort.
fn openAiResponseFormat(model: []const u8) []const u8 {
    const trimmed = std.mem.trim(u8, model, " \t\r\n");
    if (std.mem.startsWith(u8, trimmed, "gpt-4o")) return "json";
    return "verbose_json";
}

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
    try appendMultipartField(&buf, alloc, boundary, "response_format", openAiResponseFormat(model));
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

fn resolveAudioInputAlloc(alloc: Allocator, url: []const u8, max_download_bytes: ?usize) ![]u8 {
    var content = try resolveAudioContentAlloc(alloc, url, max_download_bytes);
    alloc.free(content.content_type);
    const data = content.data;
    content = undefined;
    return data;
}

fn resolveAudioContentAlloc(alloc: Allocator, url: []const u8, max_download_bytes: ?usize) !scraping.DownloadedContent {
    const security = remoteFetchSecurity(max_download_bytes);
    if (scraping.data_uri.hasScheme(url)) {
        const parsed = try scraping.data_uri.parseRequired(url);
        if (!parsed.has_explicit_media_type or
            !(std.ascii.startsWithIgnoreCase(parsed.media_type_essence, "audio/") or
                std.ascii.startsWithIgnoreCase(parsed.media_type_essence, "video/")))
            return error.InvalidDataUri;
        return try scraping.downloadContentAlloc(alloc, url, &security, null);
    }

    const fetched = try scraping.downloadContentOutcomeAlloc(alloc, url, &security, null);
    switch (fetched) {
        .http_error => |err_resp| {
            _ = err_resp;
            return error.RemoteAudioFetchFailed;
        },
        .ok => |response| return response,
    }
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
        \\  "whisper-local": { "provider": "antfly", "api_url": "http://127.0.0.1:8080", "model": "openai/whisper-base" },
        \\  "whisper-remote": { "provider": "openai", "model": "whisper-1" }
        \\}
    ;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();

    var registry = try Registry.parseFromValue(alloc, parsed.value);
    defer registry.deinit();

    try std.testing.expectEqualStrings("whisper-local", registry.defaultProviderName().?);
    const default_cfg = try registry.getConfig(null);
    try std.testing.expectEqual(Provider.antfly, default_cfg.provider);
    try std.testing.expectEqualStrings("openai/whisper-base", default_cfg.model.?);

    const explicit_cfg = try registry.getConfig("whisper-remote");
    try std.testing.expectEqual(Provider.openai, explicit_cfg.provider);
    try std.testing.expectEqualStrings("whisper-1", explicit_cfg.model.?);
}

test "transcribing config clone preserves the route response ceiling" {
    const alloc = std.testing.allocator;
    var cloned = try cloneConfig(alloc, .{
        .provider = .antfly,
        .max_response_bytes = 1234,
    });
    defer deinitConfig(alloc, &cloned);
    try std.testing.expectEqual(@as(?usize, 1234), cloned.max_response_bytes);
}

test "transcribing registry duplicate provider error does not double free config" {
    const alloc = std.testing.allocator;
    var registry = Registry.init(alloc);
    defer registry.deinit();

    try registry.registerConfig("speech", .{ .provider = .antfly, .model = "transcriber-model" });
    try std.testing.expectError(error.DuplicateTranscribingProviderName, registry.registerConfig("speech", .{
        .provider = .vertex,
        .model = "latest_long",
        .project_id = "proj",
        .credentials_path = "/tmp/does-not-matter.json",
    }));
}

test "transcribing runtime rejects an Antfly provider without a routing model" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
    defer client.deinit();
    var registry = Registry.init(alloc);
    defer registry.deinit();
    try registry.registerConfig("speech", .{ .provider = .antfly });
    var runtime = Runtime.init(alloc);
    defer runtime.deinit();

    try std.testing.expectError(
        error.InvalidTranscribingConfig,
        runtime.loadFromRegistry(&client, &registry),
    );
}

test "transcribing runtime loads antfly provider and transcribes data uri input" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/transcribe", .assert_request = expectAntflyTranscriberBearer, .respond = .{
            .body = "{\"object\":\"list\",\"data\":[{\"object\":\"transcription\",\"index\":0,\"text\":\"hello from antfly\",\"language\":\"en\"}],\"model\":\"openai/whisper-base\",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":3,\"total_tokens\":3}}",
        } },
    });
    defer server.deinit();

    const api_url = try std.fmt.allocPrint(alloc, "{s}", .{server.baseUrl()});
    defer alloc.free(api_url);
    const raw =
        \\{
        \\  "whisper-local": { "provider": "antfly", "api_url": "
    ;
    const suffix =
        \\", "model": "openai/whisper-base", "api_key": "antfly-secret" }
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

    try std.testing.expectEqualStrings("hello from antfly", response.?.text.?);
    try std.testing.expectEqualStrings("en", response.?.language.?);
}

test "Antfly transcriber sends negotiated framed audio attachments" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .POST,
        .path = "/transcribe",
        .assert_request = expectAntflyTranscriberFramed,
        .respond = .{
            .body = "{\"object\":\"list\",\"data\":[{\"object\":\"transcription\",\"index\":0,\"text\":\"framed\"}],\"model\":\"whisper\",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":1,\"total_tokens\":1}}",
        },
    }});
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(allocator, "{s}", .{server.baseUrl()});
    defer allocator.free(endpoint);
    var client = httpx.Client.initWithConfig(allocator, io, .{ .keep_alive = false });
    defer client.deinit();
    var response: ?Response = null;
    defer if (response) |*value| deinitResponse(allocator, value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(a: Allocator, http: *httpx.Client, url: []const u8, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
            out.* = transcribeWithConfig(a, http, .{
                .provider = .antfly,
                .model = "whisper",
                .url = url,
                .framed_attachments = true,
            }, .{ .url = "data:audio/wav;base64,ZmFrZQ==" }, .{}) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    group.concurrent(io, Fiber.run, .{ allocator, &client, endpoint, &response, &run_err }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;
    try std.testing.expectEqualStrings("framed", response.?.text.?);
}

test "transcribing runtime loads openai provider and transcribes data uri input" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/audio/transcriptions", .assert_request = expectOpenAiTranscriberBearer, .respond = .{
            .body = "{\"text\":\"hello from openai\",\"language\":\"en\"}",
        } },
    });
    defer server.deinit();

    const base_url = try std.fmt.allocPrint(alloc, "{s}", .{server.baseUrl()});
    defer alloc.free(base_url);
    const cfg_json = try std.fmt.allocPrint(
        alloc,
        \\{{"whisper-remote":{{"provider":"openai","base_url":"{s}","bearer_token":"openai-bearer","model":"whisper-1"}}}}
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

test "vertex transcriber exchanges service account credentials and sends bearer auth" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/token", .respond = .{
            .body = "{\"access_token\":\"vertex-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}",
        } },
        .{ .method = .POST, .path = "/projects/proj-from-json/locations/global/recognizers/_:recognize", .assert_request = expectVertexBearer, .respond = .{
            .body = "{\"results\":[{\"alternatives\":[{\"transcript\":\"transcribed by vertex\"}],\"languageCode\":\"en-US\"}]}",
        } },
    });
    defer server.deinit();

    const token_uri = try std.fmt.allocPrint(alloc, "{s}/token", .{server.baseUrl()});
    defer alloc.free(token_uri);
    const credentials_json = try fakeVertexCredentialsJsonAlloc(alloc, token_uri);
    defer alloc.free(credentials_json);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "credentials.json", .data = credentials_json });
    const credentials_path = try std.fs.path.join(alloc, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "credentials.json" });
    defer alloc.free(credentials_path);

    var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
    defer client.deinit();

    const transcriber = try initTranscriber(alloc, &client, .{
        .provider = .vertex,
        .base_url = server.baseUrl(),
        .credentials_path = credentials_path,
        .model = "latest_long",
    });
    defer transcriber.deinit();

    var response: ?Response = null;
    defer if (response) |*value| deinitResponse(alloc, value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;

    const Fiber = struct {
        fn run(a: Allocator, t: Transcriber, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
            out.* = t.transcribe(a, .{
                .url = "data:audio/wav;base64,ZmFrZQ==",
            }) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };

    group.concurrent(io, Fiber.run, .{ alloc, transcriber, &response, &run_err }) catch return;
    try server.handleOne();
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    try std.testing.expectEqualStrings("transcribed by vertex", response.?.text.?);
    try std.testing.expectEqualStrings("en-US", response.?.language.?);
}

const fake_vertex_private_key_json =
    "-----BEGIN PRIVATE KEY-----\\n" ++
    "MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBAOXaLd9jk03zcJ95\\n" ++
    "CfwKjyqHiZAaf0KC4rwRWd+TSvrqdiZUHneOXchF4FtwAJ6m+qi5KsTyazOWv4S0\\n" ++
    "FRLd49XFNv8op9e8x+gnItgt4QoQ2UT+QU7qG+wyavU25+m61G2CFB8+I9wXzH3x\\n" ++
    "HMfUuOWgqfy+szxUFNRf3sEfGW8DAgMBAAECgYEAmR1LG5mQggfeCU2vGgfKsRES\\n" ++
    "0Tzlc2APPCruzKGo/Bb917CHjyr2TDhIKYEl2InxRj37QLEgOoB8WiFAPI41e2mZ\\n" ++
    "r/sshHAB74N7OOCG6G4Jin1qsnQKgSwloBctDxtvUydD1ApmjfKQB1vENL6h4jKU\\n" ++
    "VMBm/65DU/4iWJkWgBECQQD4oRPl63IemtUsRTnz+j8tEC5MsH7CNvwNj5os2ptm\\n" ++
    "X3/rAge3BKYMWlN237K6yapZMHfiLj3K3fv8Kkbn7VwpAkEA7KqY97XZaLr4sI3a\\n" ++
    "9EHgbB2GjzJAsnzXSfn7OXLuc812rDpK/+6mcXFSbe1OmQTbzPIOJIARcIz3fqXI\\n" ++
    "uAHXSwJAOlA1RYjKVElGVELMS9/Wr3ALG+uNX2ncBiY3J+wB5Knja7AnNRK/C0io\\n" ++
    "KMpgthSUgqSuiXsE/S7BaixUQxNVuQJBAJC8hHB5tkxmjFDtcEqRPz7fj7tjcE24\\n" ++
    "K7ICP7ISp+IKddk+jT+YJBKcy1yPFNJgNkxQfHW2HPRIQdQib26ZMaECQQCcW21U\\n" ++
    "jsnUTXZp0WrOnzoqkJtQmmey1Bb9ZxBym/IoaQdDefgbdlyeFQTz2tWKDwqAlEsl\\n" ++
    "8peeQ6Fmi8Vuw9qK\\n" ++
    "-----END PRIVATE KEY-----\\n";

fn fakeVertexCredentialsJsonAlloc(alloc: Allocator, token_uri: []const u8) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        \\{{
        \\  "project_id": "proj-from-json",
        \\  "private_key_id": "kid-1",
        \\  "private_key": "{s}",
        \\  "client_email": "svc@example.iam.gserviceaccount.com",
        \\  "token_uri": "{s}"
        \\}}
    ,
        .{ fake_vertex_private_key_json, token_uri },
    );
}

fn expectVertexBearer(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(httpx.Method.POST, req.method);
    try std.testing.expectEqualStrings("Bearer vertex-token", req.header("Authorization") orelse return error.MissingHeader);
}

fn expectAntflyTranscriberBearer(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(httpx.Method.POST, req.method);
    try std.testing.expectEqualStrings("Bearer antfly-secret", req.header("Authorization") orelse return error.MissingHeader);
}

test "registered provider config keeps every option, not only its strings" {
    const alloc = std.testing.allocator;
    var registry = Registry.init(alloc);
    defer registry.deinit();

    var model_buf = "openai/whisper-base".*;
    try registry.registerConfig("speech", .{
        .provider = .antfly,
        .model = &model_buf,
        .max_download_bytes = 4096,
        .max_response_bytes = 8192,
        .timestamps = false,
        .diarization = true,
    });

    const stored = try registry.getConfig("speech");
    try std.testing.expectEqual(@as(?usize, 4096), stored.max_download_bytes);
    try std.testing.expectEqual(@as(?usize, 8192), stored.max_response_bytes);
    try std.testing.expectEqual(@as(?bool, false), stored.timestamps);
    try std.testing.expectEqual(@as(?bool, true), stored.diarization);
    // Owned text is copied, so the caller's buffer can go away or change.
    @memset(&model_buf, 'x');
    try std.testing.expectEqualStrings("openai/whisper-base", stored.model.?);
}

test "openai response format follows the model's capability" {
    // whisper-1 is the only model that returns segment timing.
    try std.testing.expectEqualStrings("verbose_json", openAiResponseFormat("whisper-1"));
    try std.testing.expectEqualStrings("verbose_json", openAiResponseFormat("  whisper-1\n"));
    // The GPT-4o transcription models reject verbose_json.
    try std.testing.expectEqualStrings("json", openAiResponseFormat("gpt-4o-transcribe"));
    try std.testing.expectEqualStrings("json", openAiResponseFormat("gpt-4o-mini-transcribe"));

    const multipart = try buildOpenAiMultipartAlloc(std.testing.allocator, "gpt-4o-transcribe", null, "fake", "audio/wav");
    defer std.testing.allocator.free(multipart.content_type);
    defer std.testing.allocator.free(multipart.body);
    try std.testing.expect(std.mem.indexOf(u8, multipart.body, "name=\"response_format\"\r\n\r\njson\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, multipart.body, "verbose_json") == null);
}

test "a json-only openai response still yields its transcript" {
    const alloc = std.testing.allocator;
    var response = try parseOpenAiVerboseResponseAlloc(alloc, "{\"text\":\"hello there\"}");
    defer deinitResponse(alloc, &response);
    try std.testing.expectEqualStrings("hello there", response.text.?);
    try std.testing.expectEqual(@as(?[]const Segment, null), response.segments);
    try std.testing.expectEqual(@as(?i64, null), response.duration_ms);
}

test "remote antfly transcription asks for diarization and keeps the labels" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .POST,
        .path = "/transcribe",
        .assert_request = expectAntflyTranscriberDiarization,
        .respond = .{
            .body = "{\"object\":\"list\",\"data\":[{\"object\":\"transcription\",\"index\":0,\"text\":\"hello there\",\"segments\":[{\"text\":\"hello\",\"start_ms\":0,\"end_ms\":500,\"words\":[],\"speaker\":\"SPEAKER_00\"},{\"text\":\"there\",\"start_ms\":500,\"end_ms\":900,\"words\":[],\"speaker\":\"SPEAKER_01\"}],\"speakers\":[\"SPEAKER_00\",\"SPEAKER_01\"]}],\"model\":\"whisper\",\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":2,\"total_tokens\":2}}",
        },
    }});
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(allocator, "{s}", .{server.baseUrl()});
    defer allocator.free(endpoint);
    var client = httpx.Client.initWithConfig(allocator, io, .{ .keep_alive = false });
    defer client.deinit();
    var response: ?Response = null;
    defer if (response) |*value| deinitResponse(allocator, value);
    var run_err: ?anyerror = null;
    var group = std.Io.Group.init;
    const Fiber = struct {
        fn run(a: Allocator, http: *httpx.Client, url: []const u8, out: *?Response, err_out: *?anyerror) std.Io.Cancelable!void {
            out.* = transcribeWithConfig(a, http, .{
                .provider = .antfly,
                .model = "whisper",
                .url = url,
            }, .{ .url = "data:audio/wav;base64,ZmFrZQ==", .diarization = true }, .{}) catch |err| {
                err_out.* = err;
                return;
            };
        }
    };
    group.concurrent(io, Fiber.run, .{ allocator, &client, endpoint, &response, &run_err }) catch return;
    try server.handleOne();
    group.await(io) catch {};
    if (run_err) |err| return err;

    const value = response.?;
    try std.testing.expectEqualStrings("hello there", value.text.?);
    const speakers = value.speakers orelse return error.MissingSpeakers;
    try std.testing.expectEqual(@as(usize, 2), speakers.len);
    try std.testing.expectEqualStrings("SPEAKER_00", speakers[0].label.?);
    try std.testing.expectEqualStrings("SPEAKER_01", speakers[1].id.?);
    const segments = value.segments orelse return error.MissingSegments;
    try std.testing.expectEqual(@as(usize, 2), segments.len);
    try std.testing.expectEqualStrings("SPEAKER_00", segments[0].speaker.?);
    try std.testing.expectEqualStrings("SPEAKER_01", segments[1].speaker.?);

    // The same transcript, as text, keeps the turns.
    const attributed = try speakerAttributedTextAlloc(allocator, &value);
    defer allocator.free(attributed);
    try std.testing.expectEqualStrings("SPEAKER_00: hello\nSPEAKER_01: there", attributed);
}

fn expectAntflyTranscriberDiarization(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(httpx.Method.POST, req.method);
    try std.testing.expect(std.mem.indexOf(u8, req.body, "\"diarization\":true") != null);
}

fn expectAntflyTranscriberFramed(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqualStrings(
        httpx.attachment_envelope.content_type,
        req.header("Content-Type") orelse return error.MissingHeader,
    );
    var envelope = try httpx.attachment_envelope.parseAlloc(std.testing.allocator, req.body, .{});
    defer envelope.deinit();
    try std.testing.expectEqual(@as(usize, 1), envelope.attachments.len);
    try std.testing.expectEqualStrings("audio/wav", envelope.attachments[0].mime_type);
    try std.testing.expectEqualStrings("fake", envelope.attachments[0].data);
    try std.testing.expect(std.mem.indexOf(u8, envelope.metadata, "\"audio\":\"attachment:0\"") != null);
}

fn expectOpenAiTranscriberBearer(req: httpx.testing_mod.RequestInfo) !void {
    try std.testing.expectEqual(httpx.Method.POST, req.method);
    try std.testing.expectEqualStrings("Bearer openai-bearer", req.header("Authorization") orelse return error.MissingHeader);
}
