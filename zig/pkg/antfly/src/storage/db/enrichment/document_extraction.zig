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
const builtin = @import("builtin");
const build_options = @import("build_options");
const pdf = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    struct {
        pub const reader = struct {
            pub const Reader = struct {
                pub fn init(_: Allocator, _: []const u8) !Reader {
                    return error.PdfExtractionUnavailable;
                }

                pub fn deinit(_: *Reader) void {}

                pub fn pageCount(_: *Reader) !usize {
                    return 0;
                }

                pub fn extractPageTextAlloc(_: *Reader, _: usize) ![]u8 {
                    return error.PdfExtractionUnavailable;
                }

                pub fn extractPageTextRunsAlloc(_: *const Reader, _: usize) ![]TextRun {
                    return error.PdfExtractionUnavailable;
                }

                pub fn extractPageBox(_: *Reader, _: usize) !struct { min_x: f64, min_y: f64, max_x: f64, max_y: f64 } {
                    return error.PdfExtractionUnavailable;
                }

                pub fn extractPageRotation(_: *const Reader, _: usize) !?i32 {
                    return error.PdfExtractionUnavailable;
                }
            };

            pub const TextRun = struct {
                text: []const u8,
                x: f64 = 0,
                y: f64 = 0,
                font_size: f64 = 0,
                a: f64 = 1,
                b: f64 = 0,
                c: f64 = 0,
                d: f64 = 1,
                advance_width: f64 = 0,
                ascent: f64 = 0,
                descent: f64 = 0,

                pub fn deinit(_: *TextRun, _: Allocator) void {}
            };
        };

        pub const render = struct {
            pub fn textRunBounds(_: reader.TextRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
                return .{ .min_x = 0, .max_x = 0, .min_y = 0, .max_y = 0 };
            }
        };

        pub fn renderPagePngAlloc(_: Allocator, _: []const u8, _: usize, _: u16, _: u64) ![]u8 {
            return error.PdfRenderingUnavailable;
        }
    }
else
    @import("antfly_pdf");
const scraping = if (builtin.os.tag == .freestanding or build_options.bench_minimal_deps)
    @import("../scraping_stub.zig")
else
    @import("antfly_scraping");
const template_remote = if (builtin.os.tag == .freestanding or builtin.is_test or build_options.bench_minimal_deps)
    @import("../template_remote_stub.zig")
else
    @import("../../../template_remote.zig");

const Allocator = std.mem.Allocator;

pub fn effectiveRemoteContentMaxDownloadSize(remote_content: ?*const scraping.RemoteContentConfig) u64 {
    if (comptime builtin.os.tag != .freestanding and !build_options.bench_minimal_deps) {
        if (remote_content) |remote| {
            if (remote.security) |security| {
                if (security.max_download_size_bytes) |value| return value;
            }
        }
    }
    return template_remote.default_remote_fetch_max_download_size_bytes;
}

pub fn inlineDataUriSourceTooLarge(remote_content: ?*const scraping.RemoteContentConfig, source_text: []const u8) !bool {
    if (!std.mem.startsWith(u8, source_text, "data:")) return false;
    const decoded_len = scraping.dataUriDecodedSize(source_text) catch return false;
    return @as(u64, @intCast(decoded_len)) > effectiveRemoteContentMaxDownloadSize(remote_content);
}

pub fn validateInlineSourceSize(remote_content: ?*const scraping.RemoteContentConfig, source_text: []const u8) !void {
    if (try inlineDataUriSourceTooLarge(remote_content, source_text)) return error.StreamTooLong;
}

pub fn renderPdfPagePngAlloc(alloc: Allocator, pdf_bytes: []const u8, page_number: usize, dpi: u16, max_pixels: u64) ![]u8 {
    return try pdf.renderPagePngAlloc(alloc, pdf_bytes, page_number, dpi, max_pixels);
}

pub fn ocrPagePartsJsonAlloc(alloc: Allocator, config: Config, route_type: []const u8, source_content_type: []const u8, unit: Unit, png: []const u8) ![]u8 {
    const encoded_len = std.base64.standard.Encoder.calcSize(png.len);
    const encoded = try alloc.alloc(u8, encoded_len);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, png);
    const prompt = if (config.ocr_prompt.len > 0) config.ocr_prompt else default_ocr_prompt;
    return try std.json.Stringify.valueAlloc(alloc, .{
        .{ .type = "text", .text = prompt },
        .{ .type = "media", .mime_type = "image/png", .data = encoded },
        .{ .type = "metadata", .schema = "antfly.document_generated_text_request.v1", .route_type = route_type, .source_content_type = source_content_type, .unit_id = unit.unit_id, .page_number = unit.page_number, .page_label = unit.page_label, .page_bbox = unit.page_bbox, .page_rotation = unit.page_rotation },
    }, .{});
}

pub fn rebaseUnitCharOffsets(units: []Unit) void {
    var cursor: usize = 0;
    for (units) |*unit| {
        unit.char_start = std.math.cast(u32, cursor);
        cursor += unit.text.len;
        unit.char_end = std.math.cast(u32, cursor);
    }
}

pub const TextRegion = struct {
    span: [2]u32,
    bbox: [4]f64,
};

pub const Unit = struct {
    unit_id: []u8,
    unit_type: []u8,
    text: []u8,
    method: []u8,
    source_path: ?[]u8 = null,
    extraction_status: ?[]u8 = null,
    source_sha256: ?[]u8 = null,
    byte_length: ?u64 = null,
    ocr_used: bool = false,
    ocr_attempted: bool = false,
    ocr_render_dpi: ?u16 = null,
    ocr_trigger_reasons: ?[]u8 = null,
    ocr_embedded_quality: ?[]u8 = null,
    ocr_output_quality: ?[]u8 = null,
    ocr_confidence: ?f64 = null,
    ocr_bbox: ?[4]f64 = null,
    transcript_used: bool = false,
    transcript_confidence: ?f64 = null,
    extraction_warning: ?[]u8 = null,
    page_number: ?u32 = null,
    page_label: ?[]u8 = null,
    page_bbox: ?[4]f64 = null,
    page_rotation: ?i32 = null,
    text_regions: []TextRegion = &.{},
    char_start: ?u32 = null,
    char_end: ?u32 = null,

    pub fn deinit(self: *Unit, alloc: Allocator) void {
        alloc.free(self.unit_id);
        alloc.free(self.unit_type);
        alloc.free(self.text);
        alloc.free(self.method);
        if (self.source_path) |value| alloc.free(value);
        if (self.extraction_status) |value| alloc.free(value);
        if (self.source_sha256) |value| alloc.free(value);
        if (self.extraction_warning) |value| alloc.free(value);
        if (self.ocr_trigger_reasons) |value| alloc.free(value);
        if (self.ocr_embedded_quality) |value| alloc.free(value);
        if (self.ocr_output_quality) |value| alloc.free(value);
        if (self.page_label) |value| alloc.free(value);
        if (self.text_regions.len > 0) alloc.free(self.text_regions);
        self.* = undefined;
    }
};

pub const Result = struct {
    content_type: []u8,
    route_type: []u8,
    unsupported_reason: []u8 = "",
    units: []Unit,
    ocr_attempted_count: usize = 0,
    ocr_selected_count: usize = 0,
    ocr_retained_embedded_count: usize = 0,
    ocr_failed_count: usize = 0,
    ocr_failed_page_numbers: []const u32 = &.{},

    pub fn deinit(self: *Result, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.route_type);
        if (self.unsupported_reason.len > 0) alloc.free(self.unsupported_reason);
        for (self.units) |*unit| unit.deinit(alloc);
        if (self.units.len > 0) alloc.free(self.units);
        self.* = undefined;
    }
};

pub const StreamInfo = struct {
    content_type: []const u8,
    route_type: []const u8,
    unsupported_reason: []const u8 = "",
};

pub const UnitSink = struct {
    ptr: *anyopaque,
    on_begin: *const fn (ptr: *anyopaque, info: StreamInfo) anyerror!void,
    on_unit: *const fn (ptr: *anyopaque, unit: *Unit) anyerror!void,
    on_end: *const fn (ptr: *anyopaque) anyerror!void,
};

pub const Config = struct {
    filename: []const u8 = "",
    content_type: []const u8 = "",
    etag: []const u8 = "",
    checksum: []const u8 = "",
    version: []const u8 = "",
    last_modified: []const u8 = "",
    credentials: []const u8 = "",
    filename_field: []const u8 = "",
    content_type_field: []const u8 = "",
    etag_field: []const u8 = "",
    checksum_field: []const u8 = "",
    version_field: []const u8 = "",
    last_modified_field: []const u8 = "",
    html_strip_tags: bool = true,
    ocr_enabled: bool = false,
    ocr_mode: OcrMode = .auto,
    ocr_config_json: []const u8 = "",
    ocr_render_dpi: u16 = 150,
    ocr_max_rendered_pixels: u64 = 40_000_000,
    ocr_prompt: []const u8 = "",
    ocr_quality: OcrQualityConfig = .{},
    transcription_enabled: bool = false,
    transcription_config_json: []const u8 = "",
    route_preset: RoutePreset = .mixed_files,
    routes: []Route = &.{},

    pub fn deinit(self: *Config, alloc: Allocator) void {
        if (self.filename.len > 0) alloc.free(@constCast(self.filename));
        if (self.content_type.len > 0) alloc.free(@constCast(self.content_type));
        if (self.etag.len > 0) alloc.free(@constCast(self.etag));
        if (self.checksum.len > 0) alloc.free(@constCast(self.checksum));
        if (self.version.len > 0) alloc.free(@constCast(self.version));
        if (self.last_modified.len > 0) alloc.free(@constCast(self.last_modified));
        if (self.credentials.len > 0) alloc.free(@constCast(self.credentials));
        if (self.filename_field.len > 0) alloc.free(@constCast(self.filename_field));
        if (self.content_type_field.len > 0) alloc.free(@constCast(self.content_type_field));
        if (self.etag_field.len > 0) alloc.free(@constCast(self.etag_field));
        if (self.checksum_field.len > 0) alloc.free(@constCast(self.checksum_field));
        if (self.version_field.len > 0) alloc.free(@constCast(self.version_field));
        if (self.last_modified_field.len > 0) alloc.free(@constCast(self.last_modified_field));
        if (self.ocr_config_json.len > 0) alloc.free(@constCast(self.ocr_config_json));
        if (self.ocr_prompt.len > 0) alloc.free(@constCast(self.ocr_prompt));
        if (self.transcription_config_json.len > 0) alloc.free(@constCast(self.transcription_config_json));
        for (self.routes) |*route| route.deinit(alloc);
        if (self.routes.len > 0) alloc.free(self.routes);
        self.* = undefined;
    }
};

pub const OcrMode = enum {
    auto,
    always,
};

pub const default_ocr_prompt = "Transcribe this page faithfully. Preserve reading order, headings, lists, and table structure. Render tables as Markdown with one row per visual row. Do not summarize, infer, or omit text. Return only the transcription.";

pub const OcrQualityConfig = struct {
    min_content_chars: usize = 50,
    garbled_min_words: usize = 20,
    garbled_sample_words: usize = 50,
    max_single_char_word_ratio: f64 = 0.40,
    substantial_line_min_chars: usize = 15,
    max_corrupted_line_ratio: f64 = 0.20,
    font_corruption_score_threshold: f64 = 0.50,
    max_replacement_char_ratio: f64 = 0.05,
};

pub const OcrQuality = struct {
    too_short: bool = false,
    garbled: bool = false,
    font_corrupted: bool = false,
    replacement_corrupted: bool = false,
    single_char_word_ratio: f64 = 0,
    corrupted_line_ratio: f64 = 0,
    replacement_char_ratio: f64 = 0,
    trimmed_len: usize = 0,

    pub fn needsFallback(self: OcrQuality) bool {
        return self.too_short or self.garbled or self.font_corrupted or self.replacement_corrupted;
    }

    pub fn failureCount(self: OcrQuality) u8 {
        return @as(u8, @intFromBool(self.too_short)) +
            @as(u8, @intFromBool(self.garbled)) +
            @as(u8, @intFromBool(self.font_corrupted)) +
            @as(u8, @intFromBool(self.replacement_corrupted));
    }
};

pub fn assessOcrQuality(text: []const u8, config: OcrQualityConfig) OcrQuality {
    const trimmed = std.mem.trim(u8, text, &std.ascii.whitespace);
    var out = OcrQuality{ .trimmed_len = trimmed.len, .too_short = trimmed.len < config.min_content_chars };

    var word_count: usize = 0;
    var sampled_words: usize = 0;
    var suspicious_words: usize = 0;
    var words = std.mem.tokenizeAny(u8, trimmed, &std.ascii.whitespace);
    while (words.next()) |word| {
        word_count += 1;
        if (sampled_words >= config.garbled_sample_words) continue;
        sampled_words += 1;
        if (word.len == 1 and std.mem.indexOfScalar(u8, ".-Xxv:", word[0]) == null) suspicious_words += 1;
    }
    if (sampled_words > 0) out.single_char_word_ratio = @as(f64, @floatFromInt(suspicious_words)) / @as(f64, @floatFromInt(sampled_words));
    out.garbled = word_count >= config.garbled_min_words and out.single_char_word_ratio > config.max_single_char_word_ratio;

    var substantial_lines: usize = 0;
    var corrupted_lines: usize = 0;
    var lines = std.mem.splitScalar(u8, trimmed, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, &std.ascii.whitespace);
        if (line.len < config.substantial_line_min_chars) continue;
        substantial_lines += 1;
        if (fontCorruptionScore(line) > config.font_corruption_score_threshold) corrupted_lines += 1;
    }
    if (substantial_lines > 0) out.corrupted_line_ratio = @as(f64, @floatFromInt(corrupted_lines)) / @as(f64, @floatFromInt(substantial_lines));
    out.font_corrupted = out.corrupted_line_ratio > config.max_corrupted_line_ratio;

    var codepoints: usize = 0;
    var replacements: usize = 0;
    const view = std.unicode.Utf8View.init(trimmed) catch null;
    if (view) |valid_view| {
        var iter = valid_view.iterator();
        while (iter.nextCodepoint()) |cp| {
            codepoints += 1;
            if (cp == 0xfffd) replacements += 1;
        }
    } else {
        codepoints = trimmed.len;
        var cursor: usize = 0;
        while (std.mem.indexOfPos(u8, trimmed, cursor, "\xef\xbf\xbd")) |idx| {
            replacements += 1;
            cursor = idx + 3;
        }
    }
    if (codepoints > 0) out.replacement_char_ratio = @as(f64, @floatFromInt(replacements)) / @as(f64, @floatFromInt(codepoints));
    out.replacement_corrupted = out.replacement_char_ratio > config.max_replacement_char_ratio;
    return out;
}

fn fontCorruptionScore(text_raw: []const u8) f64 {
    const text = std.mem.trim(u8, text_raw, &std.ascii.whitespace);
    if (text.len < 10) return 0;
    var letters: usize = 0;
    var upper: usize = 0;
    var lower: usize = 0;
    var vowels: usize = 0;
    var consonant_runs: usize = 0;
    var consonant_run: usize = 0;
    for (text) |ch| {
        if (!std.ascii.isAlphabetic(ch)) continue;
        letters += 1;
        if (std.ascii.isUpper(ch)) upper += 1 else lower += 1;
        if (std.mem.indexOfScalar(u8, "aeiouAEIOU", ch) != null) {
            vowels += 1;
            if (consonant_run >= 4) consonant_runs += 1;
            consonant_run = 0;
        } else consonant_run += 1;
    }
    if (consonant_run >= 4) consonant_runs += 1;
    if (letters == 0) return 0;
    var score: f64 = 0;
    if (upper > 0 and lower > 0) {
        const upper_ratio = @as(f64, @floatFromInt(upper)) / @as(f64, @floatFromInt(letters));
        if (upper_ratio > 0.3 and upper_ratio < 0.7) score += 0.3;
    }
    var word_count: usize = 0;
    var no_vowel_long_words: usize = 0;
    var words = std.mem.tokenizeAny(u8, text, &std.ascii.whitespace);
    while (words.next()) |word_raw| {
        word_count += 1;
        const word = std.mem.trim(u8, word_raw, ".,!?;:\"'()[]{}$%");
        if (word.len <= 4) continue;
        var has_vowel = false;
        for (word) |ch| if (std.mem.indexOfScalar(u8, "aeiouAEIOU", ch) != null) {
            has_vowel = true;
            break;
        };
        if (!has_vowel) no_vowel_long_words += 1;
    }
    const consonant_ratio = @as(f64, @floatFromInt(consonant_runs)) / @as(f64, @floatFromInt(word_count + 1));
    if (consonant_runs > 0 and consonant_ratio > 0.3) score += 0.3;
    const vowel_ratio = @as(f64, @floatFromInt(vowels)) / @as(f64, @floatFromInt(letters));
    if (vowel_ratio < 0.15) score += 0.3;
    if (word_count > 0 and @as(f64, @floatFromInt(no_vowel_long_words)) / @as(f64, @floatFromInt(word_count)) > 0.5) score += 0.2;
    return @min(1.0, score);
}

pub fn preferOcrText(embedded: OcrQuality, ocr: OcrQuality) bool {
    if (embedded.needsFallback() != ocr.needsFallback()) return !ocr.needsFallback();
    if (embedded.failureCount() != ocr.failureCount()) return ocr.failureCount() < embedded.failureCount();
    if (embedded.replacement_char_ratio != ocr.replacement_char_ratio) return ocr.replacement_char_ratio < embedded.replacement_char_ratio;
    if (embedded.corrupted_line_ratio != ocr.corrupted_line_ratio) return ocr.corrupted_line_ratio < embedded.corrupted_line_ratio;
    if (embedded.single_char_word_ratio != ocr.single_char_word_ratio) return ocr.single_char_word_ratio < embedded.single_char_word_ratio;
    return ocr.trimmed_len > embedded.trimmed_len;
}

pub fn ocrQualityJsonAlloc(alloc: Allocator, quality: OcrQuality) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, quality, .{});
}

fn ocrTriggerReasonsAlloc(alloc: Allocator, quality: OcrQuality, forced: bool) ![]u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    if (forced) try out.appendSlice(alloc, "always");
    if (quality.too_short) {
        if (out.items.len > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "too_short");
    }
    if (quality.garbled) {
        if (out.items.len > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "garbled");
    }
    if (quality.font_corrupted) {
        if (out.items.len > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "font_corruption");
    }
    if (quality.replacement_corrupted) {
        if (out.items.len > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "replacement_chars");
    }
    return try out.toOwnedSlice(alloc);
}

const RoutePreset = enum {
    mixed_files,
    explicit_only,

    fn parse(value: []const u8) ?RoutePreset {
        if (std.mem.eql(u8, value, "mixed_files") or std.mem.eql(u8, value, "default")) return .mixed_files;
        if (std.mem.eql(u8, value, "explicit_only") or std.mem.eql(u8, value, "none")) return .explicit_only;
        return null;
    }
};

const ExtractorType = enum {
    pdf,
    html,
    text,
    email,
    docx,
    pptx,
    xlsx,
    archive,
    ocr,
    audio,
    unsupported,

    fn parse(value: []const u8) ?ExtractorType {
        if (std.mem.eql(u8, value, "pdf")) return .pdf;
        if (std.mem.eql(u8, value, "html")) return .html;
        if (std.mem.eql(u8, value, "text")) return .text;
        if (std.mem.eql(u8, value, "email")) return .email;
        if (std.mem.eql(u8, value, "docx")) return .docx;
        if (std.mem.eql(u8, value, "pptx")) return .pptx;
        if (std.mem.eql(u8, value, "xlsx")) return .xlsx;
        if (std.mem.eql(u8, value, "archive")) return .archive;
        if (std.mem.eql(u8, value, "ocr") or std.mem.eql(u8, value, "image")) return .ocr;
        if (std.mem.eql(u8, value, "audio") or std.mem.eql(u8, value, "transcript")) return .audio;
        if (std.mem.eql(u8, value, "unsupported")) return .unsupported;
        return null;
    }
};

const RouteMatch = struct {
    content_type: []const u8 = "",
    content_type_prefix: []const u8 = "",
    extensions: []const []const u8 = &.{},
    magic_prefixes: []const []const u8 = &.{},

    fn deinit(self: *const RouteMatch, alloc: Allocator) void {
        if (self.content_type.len > 0) alloc.free(@constCast(self.content_type));
        if (self.content_type_prefix.len > 0) alloc.free(@constCast(self.content_type_prefix));
        for (self.extensions) |extension| alloc.free(@constCast(extension));
        if (self.extensions.len > 0) alloc.free(@constCast(self.extensions));
        for (self.magic_prefixes) |prefix| alloc.free(@constCast(prefix));
        if (self.magic_prefixes.len > 0) alloc.free(@constCast(self.magic_prefixes));
    }
};

const Route = struct {
    match: RouteMatch = .{},
    extractor_type: ExtractorType,
    unit: []const u8 = "",

    fn deinit(self: *const Route, alloc: Allocator) void {
        self.match.deinit(alloc);
        if (self.unit.len > 0) alloc.free(@constCast(self.unit));
    }
};

pub fn parseConfig(alloc: Allocator, raw: []const u8) !Config {
    if (raw.len == 0) return .{};
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentExtractionConfig;

    const object = parsed.value.object;
    var config = Config{};
    errdefer config.deinit(alloc);
    config.filename = try dupeStringField(alloc, object, "filename");
    config.content_type = try dupeStringField(alloc, object, "content_type");
    config.etag = try dupeStringField(alloc, object, "etag");
    config.checksum = try dupeStringField(alloc, object, "checksum");
    config.version = try dupeStringField(alloc, object, "version");
    config.last_modified = try dupeStringField(alloc, object, "last_modified");
    config.credentials = try dupeStringField(alloc, object, "credentials");
    config.filename_field = try dupeSourceStringField(alloc, object, "filename_field");
    config.content_type_field = try dupeSourceStringField(alloc, object, "content_type_field");
    config.etag_field = try dupeSourceStringField(alloc, object, "etag_field");
    config.checksum_field = try dupeSourceStringField(alloc, object, "checksum_field");
    config.version_field = try dupeSourceStringField(alloc, object, "version_field");
    config.last_modified_field = try dupeSourceStringField(alloc, object, "last_modified_field");
    config.html_strip_tags = boolField(object, "html_strip_tags") orelse true;
    config.ocr_enabled = boolField(object, "ocr_fallback") orelse false;
    config.ocr_config_json = try parseOptionalProducerConfigJsonAlloc(alloc, object, "ocr", &config.ocr_enabled);
    try parseOcrOptions(alloc, object, &config);
    config.transcription_enabled = boolField(object, "transcribe_audio") orelse false;
    config.transcription_config_json = try parseOptionalProducerConfigJsonAlloc(alloc, object, "transcription", &config.transcription_enabled);
    config.route_preset = try parseRoutePreset(object);
    config.routes = try parseRoutesAlloc(alloc, object);
    return config;
}

fn parseOcrOptions(alloc: Allocator, object: std.json.ObjectMap, config: *Config) !void {
    const value = object.get("ocr") orelse return;
    if (value != .object) return;
    const ocr = value.object;
    if (ocr.get("mode")) |mode| {
        if (mode != .string) return error.InvalidDocumentExtractionConfig;
        config.ocr_mode = if (std.mem.eql(u8, mode.string, "auto"))
            .auto
        else if (std.mem.eql(u8, mode.string, "always"))
            .always
        else
            return error.InvalidDocumentExtractionConfig;
    }
    if (intField(ocr, "render_dpi")) |dpi| {
        if (dpi < 72 or dpi > 600) return error.InvalidDocumentExtractionConfig;
        config.ocr_render_dpi = @intCast(dpi);
    }
    if (intField(ocr, "max_rendered_pixels")) |pixels| {
        if (pixels < 1) return error.InvalidDocumentExtractionConfig;
        config.ocr_max_rendered_pixels = @intCast(pixels);
    }
    if (ocr.get("prompt")) |prompt| {
        if (prompt != .string) return error.InvalidDocumentExtractionConfig;
        config.ocr_prompt = try alloc.dupe(u8, prompt.string);
    } else if (ocr.get("config")) |producer| {
        if (producer == .object) if (producer.object.get("prompt")) |prompt| {
            if (prompt != .string) return error.InvalidDocumentExtractionConfig;
            config.ocr_prompt = try alloc.dupe(u8, prompt.string);
        };
    }
    const quality_value = ocr.get("quality") orelse return;
    if (quality_value != .object) return error.InvalidDocumentExtractionConfig;
    const quality = quality_value.object;
    if (intField(quality, "min_content_chars")) |v| {
        if (v < 0) return error.InvalidDocumentExtractionConfig;
        config.ocr_quality.min_content_chars = @intCast(v);
    }
    if (intField(quality, "garbled_min_words")) |v| {
        if (v < 0) return error.InvalidDocumentExtractionConfig;
        config.ocr_quality.garbled_min_words = @intCast(v);
    }
    if (intField(quality, "garbled_sample_words")) |v| {
        if (v < 1) return error.InvalidDocumentExtractionConfig;
        config.ocr_quality.garbled_sample_words = @intCast(v);
    }
    if (floatField(quality, "max_single_char_word_ratio")) |v| config.ocr_quality.max_single_char_word_ratio = try ratio(v);
    if (intField(quality, "substantial_line_min_chars")) |v| {
        if (v < 1) return error.InvalidDocumentExtractionConfig;
        config.ocr_quality.substantial_line_min_chars = @intCast(v);
    }
    if (floatField(quality, "max_corrupted_line_ratio")) |v| config.ocr_quality.max_corrupted_line_ratio = try ratio(v);
    if (floatField(quality, "font_corruption_score_threshold")) |v| config.ocr_quality.font_corruption_score_threshold = try ratio(v);
    if (floatField(quality, "max_replacement_char_ratio")) |v| config.ocr_quality.max_replacement_char_ratio = try ratio(v);
}

fn intField(object: std.json.ObjectMap, field: []const u8) ?i64 {
    const value = object.get(field) orelse return null;
    return if (value == .integer) value.integer else null;
}

fn floatField(object: std.json.ObjectMap, field: []const u8) ?f64 {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .float => |v| v,
        .integer => |v| @floatFromInt(v),
        else => null,
    };
}

fn ratio(value: f64) !f64 {
    if (!std.math.isFinite(value) or value < 0 or value > 1) return error.InvalidDocumentExtractionConfig;
    return value;
}

fn parseRoutePreset(object: std.json.ObjectMap) !RoutePreset {
    const value = object.get("route_preset") orelse object.get("routes_preset") orelse return .mixed_files;
    if (value != .string) return error.InvalidDocumentExtractionConfig;
    return RoutePreset.parse(value.string) orelse error.InvalidDocumentExtractionConfig;
}

fn dupeStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) ![]const u8 {
    const value = object.get(field) orelse return "";
    if (value != .string) return error.InvalidDocumentExtractionConfig;
    return try alloc.dupe(u8, value.string);
}

fn dupeSourceStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) ![]const u8 {
    if (object.get("source")) |source| {
        if (source != .object) return error.InvalidDocumentExtractionConfig;
        if (source.object.get(field)) |value| {
            if (value != .string) return error.InvalidDocumentExtractionConfig;
            return try alloc.dupe(u8, value.string);
        }
    }
    return try dupeStringField(alloc, object, field);
}

fn boolField(object: std.json.ObjectMap, field: []const u8) ?bool {
    const value = object.get(field) orelse return null;
    return switch (value) {
        .bool => |v| v,
        else => null,
    };
}

fn parseOptionalProducerConfigJsonAlloc(
    alloc: Allocator,
    object: std.json.ObjectMap,
    field: []const u8,
    enabled: *bool,
) ![]const u8 {
    const value = object.get(field) orelse return "";
    switch (value) {
        .bool => |flag| {
            enabled.* = flag;
            return "";
        },
        .object => |producer_object| {
            enabled.* = boolField(producer_object, "enabled") orelse true;
            const config_value = producer_object.get("config") orelse .null;
            if (config_value == .null) return "";
            return try std.json.Stringify.valueAlloc(alloc, config_value, .{});
        },
        else => return error.InvalidDocumentExtractionConfig,
    }
}

fn parseRoutesAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]Route {
    const value = object.get("routes") orelse return &.{};
    if (value != .array) return error.InvalidDocumentExtractionConfig;
    if (value.array.items.len == 0) return &.{};

    var routes = try alloc.alloc(Route, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (routes[0..initialized]) |*route| route.deinit(alloc);
        alloc.free(routes);
    }

    for (value.array.items) |item| {
        if (item != .object) return error.InvalidDocumentExtractionConfig;
        const route_object = item.object;
        const extractor_value = route_object.get("extractor") orelse return error.InvalidDocumentExtractionConfig;
        if (extractor_value != .object) return error.InvalidDocumentExtractionConfig;
        const extractor_object = extractor_value.object;
        const type_value = extractor_object.get("type") orelse return error.InvalidDocumentExtractionConfig;
        if (type_value != .string) return error.InvalidDocumentExtractionConfig;
        const extractor_type = ExtractorType.parse(type_value.string) orelse return error.InvalidDocumentExtractionConfig;

        routes[initialized] = blk: {
            var route = Route{ .extractor_type = extractor_type };
            errdefer route.deinit(alloc);
            route.match = if (route_object.get("match")) |match_value| route_match: {
                if (match_value != .object) return error.InvalidDocumentExtractionConfig;
                break :route_match try parseRouteMatchAlloc(alloc, match_value.object);
            } else RouteMatch{};
            route.unit = try dupeStringField(alloc, extractor_object, "unit");
            break :blk route;
        };
        initialized += 1;
    }

    return routes;
}

fn parseRouteMatchAlloc(alloc: Allocator, object: std.json.ObjectMap) !RouteMatch {
    return .{
        .content_type = try dupeStringField(alloc, object, "content_type"),
        .content_type_prefix = try dupeStringField(alloc, object, "content_type_prefix"),
        .extensions = try dupeStringArrayOrStringField(alloc, object, "extension"),
        .magic_prefixes = try dupeStringArrayOrStringField(alloc, object, "magic_prefix"),
    };
}

fn dupeStringArrayOrStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) ![]const []const u8 {
    const value = object.get(field) orelse return &.{};
    switch (value) {
        .string => |string| {
            const out = try alloc.alloc([]const u8, 1);
            errdefer alloc.free(out);
            out[0] = try alloc.dupe(u8, string);
            return out;
        },
        .array => |array| {
            if (array.items.len == 0) return &.{};
            var out = try alloc.alloc([]const u8, array.items.len);
            var initialized: usize = 0;
            errdefer {
                for (out[0..initialized]) |item| alloc.free(@constCast(item));
                alloc.free(out);
            }
            for (array.items) |item| {
                if (item != .string) return error.InvalidDocumentExtractionConfig;
                out[initialized] = try alloc.dupe(u8, item.string);
                initialized += 1;
            }
            return out;
        },
        else => return error.InvalidDocumentExtractionConfig,
    }
}

pub fn applySourceMetadataFromJson(alloc: Allocator, config: *Config, doc_value: []const u8) !void {
    if (config.filename_field.len == 0 and
        config.content_type_field.len == 0 and
        config.etag_field.len == 0 and
        config.checksum_field.len == 0 and
        config.version_field.len == 0 and
        config.last_modified_field.len == 0) return;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, doc_value, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;

    if (config.filename_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.filename_field)) |value| {
            if (config.filename.len > 0) alloc.free(@constCast(config.filename));
            config.filename = value;
        }
    }
    if (config.content_type_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.content_type_field)) |value| {
            if (config.content_type.len > 0) alloc.free(@constCast(config.content_type));
            config.content_type = value;
        }
    }
    if (config.etag_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.etag_field)) |value| {
            if (config.etag.len > 0) alloc.free(@constCast(config.etag));
            config.etag = value;
        }
    }
    if (config.checksum_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.checksum_field)) |value| {
            if (config.checksum.len > 0) alloc.free(@constCast(config.checksum));
            config.checksum = value;
        }
    }
    if (config.version_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.version_field)) |value| {
            if (config.version.len > 0) alloc.free(@constCast(config.version));
            config.version = value;
        }
    }
    if (config.last_modified_field.len > 0) {
        if (try dupeOptionalStringField(alloc, parsed.value.object, config.last_modified_field)) |value| {
            if (config.last_modified.len > 0) alloc.free(@constCast(config.last_modified));
            config.last_modified = value;
        }
    }
}

fn dupeOptionalStringField(alloc: Allocator, object: std.json.ObjectMap, field: []const u8) !?[]const u8 {
    const value = object.get(field) orelse return null;
    if (value != .string) return null;
    return try alloc.dupe(u8, value.string);
}

pub fn metadataFingerprintAlloc(
    alloc: Allocator,
    source_url: []const u8,
    config_json: []const u8,
    config: Config,
) !?[]u8 {
    if (config.etag.len == 0 and
        config.checksum.len == 0 and
        config.version.len == 0 and
        config.last_modified.len == 0) return null;

    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hashField(&hasher, "kind", "document_extraction_metadata_fingerprint_v1");
    hashField(&hasher, "source_url", source_url);
    hashField(&hasher, "config_json", config_json);
    hashField(&hasher, "filename", config.filename);
    hashField(&hasher, "content_type", config.content_type);
    hashField(&hasher, "etag", config.etag);
    hashField(&hasher, "checksum", config.checksum);
    hashField(&hasher, "version", config.version);
    hashField(&hasher, "last_modified", config.last_modified);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try hexBytesAlloc(alloc, &digest);
}

fn hashField(hasher: *std.crypto.hash.sha2.Sha256, name: []const u8, value: []const u8) void {
    var len_buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, @intCast(name.len), .big);
    hasher.update(&len_buf);
    hasher.update(name);
    std.mem.writeInt(u64, &len_buf, @intCast(value.len), .big);
    hasher.update(&len_buf);
    hasher.update(value);
}

fn hexBytesAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

pub fn extractDownloadedAlloc(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config: Config,
) !Result {
    const content_type = if (config.content_type.len > 0) config.content_type else downloaded.content_type;
    for (config.routes) |route| {
        if (!routeMatches(route.match, content_type, config.filename, source_url, downloaded.data)) continue;
        return try extractWithRouteAlloc(alloc, downloaded.data, content_type, route, config.html_strip_tags, config.ocr_mode, config.ocr_quality);
    }
    if (config.route_preset == .explicit_only) {
        return try unsupportedResultAlloc(alloc, content_type, "no_configured_route_matched");
    }
    if (isPdfContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractPdfAlloc(alloc, downloaded.data, content_type, config.ocr_mode, config.ocr_quality);
    }
    if (isHtmlContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractSingleTextUnitAlloc(alloc, downloaded.data, content_type, "article:000001", "article", "html_text", config.html_strip_tags);
    }
    if (isEmailContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractEmailAlloc(alloc, downloaded.data, content_type, "email");
    }
    if (isDocxContent(content_type, config.filename, source_url)) {
        return try extractDocxAlloc(alloc, downloaded.data, content_type);
    }
    if (isPptxContent(content_type, config.filename, source_url)) {
        return try extractPptxAlloc(alloc, downloaded.data, content_type);
    }
    if (isXlsxContent(content_type, config.filename, source_url)) {
        return try extractXlsxAlloc(alloc, downloaded.data, content_type);
    }
    if (isZipArchiveContent(content_type, config.filename, source_url)) {
        return try extractZipArchiveAlloc(alloc, downloaded.data, content_type);
    }
    if (isImageContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractMediaPlaceholderAlloc(alloc, downloaded.data, content_type, "image", "image:000001", "image", "ocr_pending", "pending_ocr", false, false);
    }
    if (isAudioContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractMediaPlaceholderAlloc(alloc, downloaded.data, content_type, "audio", "audio:000001", "audio", "transcript_pending", "pending_transcription", false, false);
    }
    if (isTextContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractSingleTextUnitAlloc(alloc, downloaded.data, content_type, "document:000001", "document", "text", false);
    }
    return try unsupportedResultAlloc(alloc, content_type, "unsupported_content_type");
}

pub fn extractDownloadedStreaming(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config: Config,
    sink: UnitSink,
) !void {
    const content_type = if (config.content_type.len > 0) config.content_type else downloaded.content_type;
    for (config.routes) |route| {
        if (!routeMatches(route.match, content_type, config.filename, source_url, downloaded.data)) continue;
        return try extractWithRouteStreaming(alloc, downloaded.data, content_type, route, config.html_strip_tags, config.ocr_mode, config.ocr_quality, sink);
    }
    if (config.route_preset == .explicit_only) {
        try streamUnsupportedResult(sink, content_type, "no_configured_route_matched");
        return;
    }
    if (isPdfContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractPdfStreaming(alloc, downloaded.data, content_type, config.ocr_mode, config.ocr_quality, sink);
    }
    if (isHtmlContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractSingleTextUnitStreaming(alloc, downloaded.data, content_type, "article:000001", "article", "html_text", config.html_strip_tags, sink);
    }
    if (isEmailContent(content_type, config.filename, source_url, downloaded.data)) {
        return try streamBufferedExtraction(alloc, try extractEmailAlloc(alloc, downloaded.data, content_type, "email"), sink);
    }
    if (isDocxContent(content_type, config.filename, source_url)) {
        return try streamBufferedExtraction(alloc, try extractDocxAlloc(alloc, downloaded.data, content_type), sink);
    }
    if (isPptxContent(content_type, config.filename, source_url)) {
        return try streamBufferedExtraction(alloc, try extractPptxAlloc(alloc, downloaded.data, content_type), sink);
    }
    if (isXlsxContent(content_type, config.filename, source_url)) {
        return try streamBufferedExtraction(alloc, try extractXlsxAlloc(alloc, downloaded.data, content_type), sink);
    }
    if (isZipArchiveContent(content_type, config.filename, source_url)) {
        return try streamBufferedExtraction(alloc, try extractZipArchiveAlloc(alloc, downloaded.data, content_type), sink);
    }
    if (isImageContent(content_type, config.filename, source_url, downloaded.data)) {
        return try streamBufferedExtraction(alloc, try extractMediaPlaceholderAlloc(alloc, downloaded.data, content_type, "image", "image:000001", "image", "ocr_pending", "pending_ocr", false, false), sink);
    }
    if (isAudioContent(content_type, config.filename, source_url, downloaded.data)) {
        return try streamBufferedExtraction(alloc, try extractMediaPlaceholderAlloc(alloc, downloaded.data, content_type, "audio", "audio:000001", "audio", "transcript_pending", "pending_transcription", false, false), sink);
    }
    if (isTextContent(content_type, config.filename, source_url, downloaded.data)) {
        return try extractSingleTextUnitStreaming(alloc, downloaded.data, content_type, "document:000001", "document", "text", false, sink);
    }
    try streamUnsupportedResult(sink, content_type, "unsupported_content_type");
}

fn extractWithRouteStreaming(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    route: Route,
    html_strip_tags: bool,
    ocr_mode: OcrMode,
    ocr_quality: OcrQualityConfig,
    sink: UnitSink,
) !void {
    switch (route.extractor_type) {
        .pdf => return try extractPdfStreaming(alloc, bytes, content_type, ocr_mode, ocr_quality, sink),
        .html => return try extractSingleConfiguredUnitStreaming(alloc, bytes, content_type, route.unit, "article", "html_text", html_strip_tags, sink),
        .text => return try extractSingleConfiguredUnitStreaming(alloc, bytes, content_type, route.unit, "document", "text", false, sink),
        .email => return try streamBufferedExtraction(alloc, try extractEmailAlloc(alloc, bytes, content_type, if (route.unit.len > 0) route.unit else "email"), sink),
        .docx => return try streamBufferedExtraction(alloc, try extractDocxAlloc(alloc, bytes, content_type), sink),
        .pptx => return try streamBufferedExtraction(alloc, try extractPptxAlloc(alloc, bytes, content_type), sink),
        .xlsx => return try streamBufferedExtraction(alloc, try extractXlsxAlloc(alloc, bytes, content_type), sink),
        .archive => return try streamBufferedExtraction(alloc, try extractZipArchiveAlloc(alloc, bytes, content_type), sink),
        .ocr => return try streamBufferedExtraction(alloc, try extractConfiguredMediaPlaceholderAlloc(alloc, bytes, content_type, "image", route.unit, "image", "ocr_pending", "pending_ocr"), sink),
        .audio => return try streamBufferedExtraction(alloc, try extractConfiguredMediaPlaceholderAlloc(alloc, bytes, content_type, "audio", route.unit, "audio", "transcript_pending", "pending_transcription"), sink),
        .unsupported => return try streamUnsupportedResult(sink, content_type, "matched_unsupported_route"),
    }
}

fn extractSingleConfiguredUnitStreaming(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    configured_unit: []const u8,
    default_unit: []const u8,
    method: []const u8,
    strip_html: bool,
    sink: UnitSink,
) !void {
    const unit_type = if (configured_unit.len > 0) configured_unit else default_unit;
    const unit_id = try std.fmt.allocPrint(alloc, "{s}:000001", .{unit_type});
    defer alloc.free(unit_id);
    try extractSingleTextUnitStreaming(alloc, bytes, content_type, unit_id, unit_type, method, strip_html, sink);
}

fn extractSingleTextUnitStreaming(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    unit_id: []const u8,
    unit_type: []const u8,
    method: []const u8,
    strip_html: bool,
    sink: UnitSink,
) !void {
    try sink.on_begin(sink.ptr, .{ .content_type = content_type, .route_type = if (strip_html) "html" else "text" });
    var text = if (strip_html)
        try htmlToTextAlloc(alloc, bytes)
    else
        try alloc.dupe(u8, bytes);
    errdefer alloc.free(text);
    var unit = Unit{
        .unit_id = try alloc.dupe(u8, unit_id),
        .unit_type = try alloc.dupe(u8, unit_type),
        .text = text,
        .method = try alloc.dupe(u8, method),
        .char_start = 0,
        .char_end = std.math.cast(u32, text.len),
    };
    text = &.{};
    defer unit.deinit(alloc);
    try sink.on_unit(sink.ptr, &unit);
    try sink.on_end(sink.ptr);
}

fn streamUnsupportedResult(sink: UnitSink, content_type: []const u8, reason: []const u8) !void {
    try sink.on_begin(sink.ptr, .{ .content_type = content_type, .route_type = "unsupported", .unsupported_reason = reason });
    try sink.on_end(sink.ptr);
}

fn streamBufferedExtraction(alloc: Allocator, result: Result, sink: UnitSink) !void {
    var buffered = result;
    defer buffered.deinit(alloc);
    try sink.on_begin(sink.ptr, .{
        .content_type = buffered.content_type,
        .route_type = buffered.route_type,
        .unsupported_reason = buffered.unsupported_reason,
    });
    for (buffered.units) |*unit| {
        try sink.on_unit(sink.ptr, unit);
    }
    try sink.on_end(sink.ptr);
}

fn routeMatches(match: RouteMatch, content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (match.content_type.len == 0 and match.content_type_prefix.len == 0 and match.extensions.len == 0 and match.magic_prefixes.len == 0) return true;
    if (match.content_type.len > 0 and contentTypeEquals(content_type, match.content_type)) return true;
    if (match.content_type_prefix.len > 0 and contentTypeStartsWith(content_type, match.content_type_prefix)) return true;
    for (match.extensions) |extension| {
        if (hasConfiguredExtension(filename, extension) or hasConfiguredExtension(source_url, extension)) return true;
    }
    for (match.magic_prefixes) |prefix| {
        if (std.mem.startsWith(u8, bytes, prefix)) return true;
    }
    return false;
}

fn extractWithRouteAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    route: Route,
    html_strip_tags: bool,
    ocr_mode: OcrMode,
    ocr_quality: OcrQualityConfig,
) !Result {
    return switch (route.extractor_type) {
        .pdf => try extractPdfAlloc(alloc, bytes, content_type, ocr_mode, ocr_quality),
        .html => try extractSingleConfiguredUnitAlloc(alloc, bytes, content_type, route.unit, "article", "html_text", html_strip_tags),
        .text => try extractSingleConfiguredUnitAlloc(alloc, bytes, content_type, route.unit, "document", "text", false),
        .email => try extractEmailAlloc(alloc, bytes, content_type, if (route.unit.len > 0) route.unit else "email"),
        .docx => try extractDocxAlloc(alloc, bytes, content_type),
        .pptx => try extractPptxAlloc(alloc, bytes, content_type),
        .xlsx => try extractXlsxAlloc(alloc, bytes, content_type),
        .archive => try extractZipArchiveAlloc(alloc, bytes, content_type),
        .ocr => try extractConfiguredMediaPlaceholderAlloc(alloc, bytes, content_type, "image", route.unit, "image", "ocr_pending", "pending_ocr"),
        .audio => try extractConfiguredMediaPlaceholderAlloc(alloc, bytes, content_type, "audio", route.unit, "audio", "transcript_pending", "pending_transcription"),
        .unsupported => try unsupportedResultAlloc(alloc, content_type, "matched_unsupported_route"),
    };
}

fn extractSingleConfiguredUnitAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    configured_unit: []const u8,
    default_unit: []const u8,
    method: []const u8,
    strip_html: bool,
) !Result {
    const unit_type = if (configured_unit.len > 0) configured_unit else default_unit;
    const unit_id = try std.fmt.allocPrint(alloc, "{s}:000001", .{unit_type});
    defer alloc.free(unit_id);
    return try extractSingleTextUnitAlloc(alloc, bytes, content_type, unit_id, unit_type, method, strip_html);
}

fn unsupportedResultAlloc(alloc: Allocator, content_type: []const u8, reason: []const u8) !Result {
    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "unsupported"),
        .unsupported_reason = try alloc.dupe(u8, reason),
        .units = try alloc.alloc(Unit, 0),
    };
}

fn sha256HexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const out = try alloc.alloc(u8, digest.len * 2);
    for (digest, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn extractPdfAlloc(alloc: Allocator, bytes: []const u8, content_type: []const u8, ocr_mode: OcrMode, quality_config: OcrQualityConfig) !Result {
    var parsed = try pdf.reader.Reader.init(alloc, bytes);
    defer parsed.deinit();

    const page_count = try parsed.pageCount();
    var units = try alloc.alloc(Unit, page_count);
    var initialized: usize = 0;
    errdefer {
        for (units[0..initialized]) |*unit| unit.deinit(alloc);
        alloc.free(units);
    }

    var page_num: usize = 1;
    var cursor: usize = 0;
    while (page_num <= page_count) : (page_num += 1) {
        const force_ocr = ocr_mode == .always;
        const text = if (force_ocr) try alloc.alloc(u8, 0) else try parsed.extractPageTextAlloc(page_num);
        errdefer alloc.free(text);
        const text_regions: []TextRegion = if (force_ocr)
            @constCast(&.{})
        else
            try extractPdfTextRegionsAlloc(alloc, &parsed, page_num, text);
        errdefer if (text_regions.len > 0) alloc.free(text_regions);
        const page_box = parsed.extractPageBox(page_num) catch null;
        const page_rotation = parsed.extractPageRotation(page_num) catch null;
        const char_start = std.math.cast(u32, cursor);
        const char_end = std.math.cast(u32, cursor + text.len);
        var unit_id: ?[]u8 = try std.fmt.allocPrint(alloc, "page:{d:0>6}", .{page_num});
        errdefer if (unit_id) |value| alloc.free(value);
        var unit_type: ?[]u8 = try alloc.dupe(u8, "page");
        errdefer if (unit_type) |value| alloc.free(value);
        const quality = assessOcrQuality(text, quality_config);
        const scanned_page = force_ocr or quality.needsFallback();
        var method: ?[]u8 = try alloc.dupe(u8, if (scanned_page) "pdf_ocr_pending" else "pdf_text");
        errdefer if (method) |value| alloc.free(value);
        var extraction_status: ?[]u8 = if (scanned_page) try alloc.dupe(u8, "pending_ocr") else null;
        errdefer if (extraction_status) |value| alloc.free(value);
        var page_label: ?[]u8 = try std.fmt.allocPrint(alloc, "{d}", .{page_num});
        errdefer if (page_label) |value| alloc.free(value);
        units[initialized] = .{
            .unit_id = unit_id.?,
            .unit_type = unit_type.?,
            .text = text,
            .method = method.?,
            .extraction_status = extraction_status,
            .ocr_used = false,
            .ocr_trigger_reasons = if (scanned_page) try ocrTriggerReasonsAlloc(alloc, quality, force_ocr) else null,
            .ocr_embedded_quality = if (scanned_page) try ocrQualityJsonAlloc(alloc, quality) else null,
            .page_number = @intCast(page_num),
            .page_label = page_label.?,
            .page_bbox = if (page_box) |box| .{ box.min_x, box.min_y, box.max_x, box.max_y } else null,
            .page_rotation = page_rotation,
            .text_regions = text_regions,
            .char_start = char_start,
            .char_end = char_end,
        };
        unit_id = null;
        unit_type = null;
        method = null;
        extraction_status = null;
        page_label = null;
        cursor += text.len;
        initialized += 1;
    }

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "pdf"),
        .units = units,
    };
}

fn extractPdfStreaming(alloc: Allocator, bytes: []const u8, content_type: []const u8, ocr_mode: OcrMode, quality_config: OcrQualityConfig, sink: UnitSink) !void {
    var parsed = try pdf.reader.Reader.init(alloc, bytes);
    defer parsed.deinit();

    const page_count = try parsed.pageCount();
    try sink.on_begin(sink.ptr, .{ .content_type = content_type, .route_type = "pdf" });

    var page_num: usize = 1;
    var cursor: usize = 0;
    while (page_num <= page_count) : (page_num += 1) {
        const force_ocr = ocr_mode == .always;
        var text = if (force_ocr) try alloc.alloc(u8, 0) else try parsed.extractPageTextAlloc(page_num);
        errdefer alloc.free(text);
        var text_regions: []TextRegion = if (force_ocr)
            @constCast(&.{})
        else
            try extractPdfTextRegionsAlloc(alloc, &parsed, page_num, text);
        errdefer if (text_regions.len > 0) alloc.free(text_regions);
        const page_text_len = text.len;
        const page_box = parsed.extractPageBox(page_num) catch null;
        const page_rotation = parsed.extractPageRotation(page_num) catch null;
        const char_start = std.math.cast(u32, cursor);
        const char_end = std.math.cast(u32, cursor + page_text_len);
        const quality = assessOcrQuality(text, quality_config);
        const scanned_page = force_ocr or quality.needsFallback();
        var unit_id: ?[]u8 = try std.fmt.allocPrint(alloc, "page:{d:0>6}", .{page_num});
        errdefer if (unit_id) |value| alloc.free(value);
        var unit_type: ?[]u8 = try alloc.dupe(u8, "page");
        errdefer if (unit_type) |value| alloc.free(value);
        var method: ?[]u8 = try alloc.dupe(u8, if (scanned_page) "pdf_ocr_pending" else "pdf_text");
        errdefer if (method) |value| alloc.free(value);
        var extraction_status: ?[]u8 = if (scanned_page) try alloc.dupe(u8, "pending_ocr") else null;
        errdefer if (extraction_status) |value| alloc.free(value);
        var page_label: ?[]u8 = try std.fmt.allocPrint(alloc, "{d}", .{page_num});
        errdefer if (page_label) |value| alloc.free(value);
        var unit = Unit{
            .unit_id = unit_id.?,
            .unit_type = unit_type.?,
            .text = text,
            .method = method.?,
            .extraction_status = extraction_status,
            .ocr_used = false,
            .ocr_trigger_reasons = if (scanned_page) try ocrTriggerReasonsAlloc(alloc, quality, force_ocr) else null,
            .ocr_embedded_quality = if (scanned_page) try ocrQualityJsonAlloc(alloc, quality) else null,
            .page_number = @intCast(page_num),
            .page_label = page_label.?,
            .page_bbox = if (page_box) |box| .{ box.min_x, box.min_y, box.max_x, box.max_y } else null,
            .page_rotation = page_rotation,
            .text_regions = text_regions,
            .char_start = char_start,
            .char_end = char_end,
        };
        unit_id = null;
        unit_type = null;
        method = null;
        extraction_status = null;
        page_label = null;
        text = &.{};
        text_regions = &.{};
        errdefer unit.deinit(alloc);
        try sink.on_unit(sink.ptr, &unit);
        unit.deinit(alloc);
        cursor += page_text_len;
    }

    try sink.on_end(sink.ptr);
}

fn extractPdfTextRegionsAlloc(
    alloc: Allocator,
    parsed: *const pdf.reader.Reader,
    page_num: usize,
    page_text: []const u8,
) ![]TextRegion {
    if (page_text.len == 0) return &.{};
    const runs = try parsed.extractPageTextRunsAlloc(page_num);
    defer {
        for (runs) |*run| run.deinit(alloc);
        if (runs.len > 0) alloc.free(runs);
    }

    var regions = std.ArrayListUnmanaged(TextRegion).empty;
    defer regions.deinit(alloc);
    var search_from: usize = 0;
    for (runs) |run| {
        if (run.text.len == 0) continue;
        const start = std.mem.indexOfPos(u8, page_text, search_from, run.text) orelse continue;
        const end = start + run.text.len;
        const span_start = std.math.cast(u32, start) orelse continue;
        const span_end = std.math.cast(u32, end) orelse continue;
        const bounds = pdf.render.textRunBounds(run);
        try regions.append(alloc, .{
            .span = .{ span_start, span_end },
            .bbox = .{ bounds.min_x, bounds.min_y, bounds.max_x, bounds.max_y },
        });
        search_from = end;
    }
    return try regions.toOwnedSlice(alloc);
}

fn extractSingleTextUnitAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    unit_id: []const u8,
    unit_type: []const u8,
    method: []const u8,
    strip_html: bool,
) !Result {
    var units = try alloc.alloc(Unit, 1);
    errdefer alloc.free(units);
    const text = if (strip_html)
        try htmlToTextAlloc(alloc, bytes)
    else
        try alloc.dupe(u8, bytes);
    errdefer alloc.free(text);
    units[0] = .{
        .unit_id = try alloc.dupe(u8, unit_id),
        .unit_type = try alloc.dupe(u8, unit_type),
        .text = text,
        .method = try alloc.dupe(u8, method),
        .char_start = 0,
        .char_end = std.math.cast(u32, text.len),
    };
    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, if (strip_html) "html" else "text"),
        .units = units,
    };
}

fn extractEmailAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    unit_prefix: []const u8,
) !Result {
    const split = emailHeaderBodySplit(bytes) orelse return try unsupportedResultAlloc(alloc, content_type, "invalid_rfc822_message");
    const headers = bytes[0..split.header_end];
    const body = bytes[split.body_start..];

    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }

    if (headers.len > 0) {
        const header_text = try normalizeEmailHeaderTextAlloc(alloc, headers);
        errdefer alloc.free(header_text);
        if (header_text.len > 0) {
            const unit_id = try std.fmt.allocPrint(alloc, "{s}:headers", .{unit_prefix});
            errdefer alloc.free(unit_id);
            try units.append(alloc, .{
                .unit_id = unit_id,
                .unit_type = try alloc.dupe(u8, "email_headers"),
                .text = header_text,
                .method = try alloc.dupe(u8, "email_rfc822"),
                .char_start = 0,
                .char_end = std.math.cast(u32, split.header_end),
            });
        } else {
            alloc.free(header_text);
        }
    }

    try appendEmailBodyUnits(alloc, &units, headers, body, unit_prefix, split.body_start, bytes.len);

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "email"),
        .units = try units.toOwnedSlice(alloc),
    };
}

fn extractDocxAlloc(alloc: Allocator, bytes: []const u8, content_type: []const u8) !Result {
    const document_xml = (try zipEntryDataAlloc(alloc, bytes, "word/document.xml")) orelse return error.MissingDocxDocumentXml;
    defer alloc.free(document_xml);

    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }

    var section_index: usize = 1;
    var segment_start: usize = 0;
    var search_start: usize = 0;
    while (findXmlTagStart(document_xml, search_start, "sectPr")) |tag_start| {
        const section_text = try ooxmlXmlTextAlloc(alloc, document_xml[segment_start..tag_start], .word);
        errdefer alloc.free(section_text);
        if (section_text.len > 0) {
            try appendOwnedUnit(alloc, &units, "section:{d:0>6}", .{section_index}, "section", "docx_text", section_text, null);
            section_index += 1;
        } else {
            alloc.free(section_text);
        }
        const tag_end = xmlTagEnd(document_xml, tag_start) orelse tag_start + 1;
        if (findXmlEndTagAfter(document_xml, tag_end, "sectPr")) |end_tag| {
            segment_start = end_tag;
            search_start = end_tag;
        } else {
            segment_start = tag_end;
            search_start = tag_end;
        }
    }

    const section_text = try ooxmlXmlTextAlloc(alloc, document_xml[segment_start..], .word);
    errdefer alloc.free(section_text);
    if (section_text.len > 0) {
        try appendOwnedUnit(alloc, &units, "section:{d:0>6}", .{section_index}, "section", "docx_text", section_text, null);
    } else {
        alloc.free(section_text);
    }

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "docx"),
        .units = try units.toOwnedSlice(alloc),
    };
}

fn extractPptxAlloc(alloc: Allocator, bytes: []const u8, content_type: []const u8) !Result {
    const entries = try zipEntriesAlloc(alloc, bytes);
    defer alloc.free(entries);

    var parts = std.ArrayListUnmanaged(OoxmlPart).empty;
    defer {
        for (parts.items) |*part| part.deinit(alloc);
        parts.deinit(alloc);
    }
    for (entries) |entry| {
        const slide_index = pptxSlideIndex(entry.name) orelse continue;
        const xml = try zipEntryDataFromEntryAlloc(alloc, bytes, entry);
        defer alloc.free(xml);
        const text = try ooxmlXmlTextAlloc(alloc, xml, .presentation);
        errdefer alloc.free(text);
        if (text.len == 0) {
            alloc.free(text);
            continue;
        }
        try parts.append(alloc, .{ .index = slide_index, .text = text });
    }
    std.mem.sort(OoxmlPart, parts.items, {}, OoxmlPart.lessThan);

    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }
    for (parts.items, 1..) |*part, ordinal| {
        const text = part.text;
        part.text = &.{};
        try appendOwnedUnit(alloc, &units, "slide:{d:0>6}", .{ordinal}, "slide", "pptx_text", text, null);
    }

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "pptx"),
        .units = try units.toOwnedSlice(alloc),
    };
}

fn extractXlsxAlloc(alloc: Allocator, bytes: []const u8, content_type: []const u8) !Result {
    var shared_strings: [][]u8 = &.{};
    defer freeStringList(alloc, shared_strings);
    if (try zipEntryDataAlloc(alloc, bytes, "xl/sharedStrings.xml")) |shared_xml| {
        defer alloc.free(shared_xml);
        shared_strings = try xlsxSharedStringsAlloc(alloc, shared_xml);
    }

    const entries = try zipEntriesAlloc(alloc, bytes);
    defer alloc.free(entries);

    var parts = std.ArrayListUnmanaged(OoxmlPart).empty;
    defer {
        for (parts.items) |*part| part.deinit(alloc);
        parts.deinit(alloc);
    }
    for (entries) |entry| {
        const sheet_index = xlsxSheetIndex(entry.name) orelse continue;
        const xml = try zipEntryDataFromEntryAlloc(alloc, bytes, entry);
        defer alloc.free(xml);
        const text = try xlsxSheetTextAlloc(alloc, xml, shared_strings);
        errdefer alloc.free(text);
        if (text.len == 0) {
            alloc.free(text);
            continue;
        }
        try parts.append(alloc, .{ .index = sheet_index, .text = text });
    }
    std.mem.sort(OoxmlPart, parts.items, {}, OoxmlPart.lessThan);

    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }
    for (parts.items, 1..) |*part, ordinal| {
        const text = part.text;
        part.text = &.{};
        try appendOwnedUnit(alloc, &units, "sheet:{d:0>6}", .{ordinal}, "sheet", "xlsx_text", text, null);
    }

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "xlsx"),
        .units = try units.toOwnedSlice(alloc),
    };
}

fn extractZipArchiveAlloc(alloc: Allocator, bytes: []const u8, content_type: []const u8) !Result {
    const entries = try zipEntriesAlloc(alloc, bytes);
    defer alloc.free(entries);
    std.mem.sort(ZipEntry, entries, {}, zipEntryLessThanName);

    var units = std.ArrayListUnmanaged(Unit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }
    var unit_index: usize = 1;
    for (entries) |entry| {
        if (entry.name.len == 0 or std.mem.endsWith(u8, entry.name, "/")) continue;
        if (!zipEntryNameIsSafe(entry.name)) continue;
        const extracted = zipEntryDataFromEntryAlloc(alloc, bytes, entry) catch |err| switch (err) {
            error.UnsupportedCompressionMethod, error.ZipDecompressSizeMismatch => continue,
            else => return err,
        };
        defer alloc.free(extracted);

        const entry_text = try archiveEntryTextAlloc(alloc, entry.name, extracted);
        errdefer alloc.free(entry_text);
        if (entry_text.len == 0) {
            alloc.free(entry_text);
            continue;
        }
        const method: []const u8 = if (archiveEntryLooksHtml(entry.name, extracted)) "zip_html" else "zip_text";
        try appendOwnedUnit(alloc, &units, "archive:entry:{d:0>6}", .{unit_index}, "archive_entry", method, entry_text, entry.name);
        unit_index += 1;
    }

    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, "archive"),
        .units = try units.toOwnedSlice(alloc),
    };
}

fn extractConfiguredMediaPlaceholderAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    route_type: []const u8,
    configured_unit: []const u8,
    default_unit: []const u8,
    method: []const u8,
    extraction_status: []const u8,
) !Result {
    const unit_type = if (configured_unit.len > 0) configured_unit else default_unit;
    const unit_id = try std.fmt.allocPrint(alloc, "{s}:000001", .{unit_type});
    defer alloc.free(unit_id);
    return try extractMediaPlaceholderAlloc(alloc, bytes, content_type, route_type, unit_id, unit_type, method, extraction_status, false, false);
}

fn extractMediaPlaceholderAlloc(
    alloc: Allocator,
    bytes: []const u8,
    content_type: []const u8,
    route_type: []const u8,
    unit_id: []const u8,
    unit_type: []const u8,
    method: []const u8,
    extraction_status: []const u8,
    ocr_used: bool,
    transcript_used: bool,
) !Result {
    var units = try alloc.alloc(Unit, 1);
    errdefer alloc.free(units);
    const sha256 = try sha256HexAlloc(alloc, bytes);
    errdefer alloc.free(sha256);
    units[0] = .{
        .unit_id = try alloc.dupe(u8, unit_id),
        .unit_type = try alloc.dupe(u8, unit_type),
        .text = try alloc.alloc(u8, 0),
        .method = try alloc.dupe(u8, method),
        .extraction_status = try alloc.dupe(u8, extraction_status),
        .source_sha256 = sha256,
        .byte_length = bytes.len,
        .ocr_used = ocr_used,
        .transcript_used = transcript_used,
        .char_start = 0,
        .char_end = 0,
    };
    return .{
        .content_type = try alloc.dupe(u8, content_type),
        .route_type = try alloc.dupe(u8, route_type),
        .units = units,
    };
}

fn appendOwnedUnit(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    comptime unit_id_fmt: []const u8,
    unit_id_args: anytype,
    unit_type: []const u8,
    method: []const u8,
    text: []u8,
    source_path: ?[]const u8,
) !void {
    errdefer alloc.free(text);
    const unit_id = try std.fmt.allocPrint(alloc, unit_id_fmt, unit_id_args);
    errdefer alloc.free(unit_id);
    const owned_type = try alloc.dupe(u8, unit_type);
    errdefer alloc.free(owned_type);
    const owned_method = try alloc.dupe(u8, method);
    errdefer alloc.free(owned_method);
    const owned_source_path = if (source_path) |path| try alloc.dupe(u8, path) else null;
    errdefer if (owned_source_path) |path| alloc.free(path);
    try units.append(alloc, .{
        .unit_id = unit_id,
        .unit_type = owned_type,
        .text = text,
        .method = owned_method,
        .source_path = owned_source_path,
    });
}

const ZipEntry = struct {
    name: []const u8,
    compression_method: std.zip.CompressionMethod,
    compressed_size: usize,
    uncompressed_size: usize,
    local_file_header_offset: usize,
};

fn zipEntryLessThanName(_: void, lhs: ZipEntry, rhs: ZipEntry) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn zipEntryNameIsSafe(name: []const u8) bool {
    if (name.len == 0 or name[0] == '/' or name[0] == '\\') return false;
    var parts = std.mem.splitAny(u8, name, "/\\");
    while (parts.next()) |part| {
        if (part.len == 0) continue;
        if (std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return false;
    }
    return true;
}

fn zipEntriesAlloc(alloc: Allocator, bytes: []const u8) ![]ZipEntry {
    const end_pos = std.mem.lastIndexOf(u8, bytes, &std.zip.end_record_sig) orelse return error.ZipNoEndRecord;
    if (end_pos + 22 > bytes.len) return error.ZipTruncated;
    const record_count = std.mem.readInt(u16, bytes[end_pos + 10 ..][0..2], .little);
    const central_directory_size = std.mem.readInt(u32, bytes[end_pos + 12 ..][0..4], .little);
    const central_directory_offset = std.mem.readInt(u32, bytes[end_pos + 16 ..][0..4], .little);
    const comment_len = std.mem.readInt(u16, bytes[end_pos + 20 ..][0..2], .little);
    if (end_pos + 22 + @as(usize, comment_len) > bytes.len) return error.ZipTruncated;
    if (central_directory_offset == std.math.maxInt(u32) or central_directory_size == std.math.maxInt(u32) or record_count == std.math.maxInt(u16)) {
        return error.Zip64Unsupported;
    }
    const cd_start: usize = @intCast(central_directory_offset);
    const cd_size: usize = @intCast(central_directory_size);
    if (cd_start > bytes.len or cd_size > bytes.len - cd_start) return error.ZipTruncated;

    var entries = try alloc.alloc(ZipEntry, record_count);
    var initialized: usize = 0;
    errdefer alloc.free(entries);

    var cursor = cd_start;
    while (initialized < record_count) : (initialized += 1) {
        if (cursor + 46 > cd_start + cd_size or cursor + 46 > bytes.len) return error.ZipTruncated;
        if (!std.mem.eql(u8, bytes[cursor .. cursor + 4], &std.zip.central_file_header_sig)) return error.ZipBadCdOffset;
        const flags = std.mem.readInt(u16, bytes[cursor + 8 ..][0..2], .little);
        if ((flags & 0x0001) != 0) return error.ZipEncryptionUnsupported;
        const compression_method: std.zip.CompressionMethod = @enumFromInt(std.mem.readInt(u16, bytes[cursor + 10 ..][0..2], .little));
        const compressed_size_u32 = std.mem.readInt(u32, bytes[cursor + 20 ..][0..4], .little);
        const uncompressed_size_u32 = std.mem.readInt(u32, bytes[cursor + 24 ..][0..4], .little);
        const name_len = std.mem.readInt(u16, bytes[cursor + 28 ..][0..2], .little);
        const extra_len = std.mem.readInt(u16, bytes[cursor + 30 ..][0..2], .little);
        const comment_len_entry = std.mem.readInt(u16, bytes[cursor + 32 ..][0..2], .little);
        const local_offset_u32 = std.mem.readInt(u32, bytes[cursor + 42 ..][0..4], .little);
        if (compressed_size_u32 == std.math.maxInt(u32) or uncompressed_size_u32 == std.math.maxInt(u32) or local_offset_u32 == std.math.maxInt(u32)) {
            return error.Zip64Unsupported;
        }
        const name_start = cursor + 46;
        const name_end = name_start + @as(usize, name_len);
        if (name_end > bytes.len) return error.ZipTruncated;
        entries[initialized] = .{
            .name = bytes[name_start..name_end],
            .compression_method = compression_method,
            .compressed_size = @intCast(compressed_size_u32),
            .uncompressed_size = @intCast(uncompressed_size_u32),
            .local_file_header_offset = @intCast(local_offset_u32),
        };
        cursor = name_end + @as(usize, extra_len) + @as(usize, comment_len_entry);
        if (cursor > cd_start + cd_size) return error.ZipTruncated;
    }
    if (cursor != cd_start + cd_size) return error.ZipCdSizeMismatch;
    return entries;
}

fn zipEntryDataAlloc(alloc: Allocator, bytes: []const u8, wanted_name: []const u8) !?[]u8 {
    const entries = try zipEntriesAlloc(alloc, bytes);
    defer alloc.free(entries);
    for (entries) |entry| {
        if (std.mem.eql(u8, entry.name, wanted_name)) {
            return try zipEntryDataFromEntryAlloc(alloc, bytes, entry);
        }
    }
    return null;
}

fn zipEntryDataFromEntryAlloc(alloc: Allocator, bytes: []const u8, entry: ZipEntry) ![]u8 {
    const offset = entry.local_file_header_offset;
    if (offset + 30 > bytes.len) return error.ZipTruncated;
    if (!std.mem.eql(u8, bytes[offset .. offset + 4], &std.zip.local_file_header_sig)) return error.ZipBadFileOffset;
    const name_len = std.mem.readInt(u16, bytes[offset + 26 ..][0..2], .little);
    const extra_len = std.mem.readInt(u16, bytes[offset + 28 ..][0..2], .little);
    const data_start = offset + 30 + @as(usize, name_len) + @as(usize, extra_len);
    if (data_start > bytes.len or entry.compressed_size > bytes.len - data_start) return error.ZipTruncated;
    const compressed = bytes[data_start .. data_start + entry.compressed_size];
    return switch (entry.compression_method) {
        .store => try alloc.dupe(u8, compressed),
        .deflate => try inflateRawAlloc(alloc, compressed, entry.uncompressed_size),
        else => error.UnsupportedCompressionMethod,
    };
}

fn inflateRawAlloc(alloc: Allocator, compressed: []const u8, expected_size: usize) ![]u8 {
    var in: std.Io.Reader = .fixed(compressed);
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    var flate_buffer: [std.compress.flate.max_window_len]u8 = undefined;
    var decompress: std.compress.flate.Decompress = .init(&in, .raw, &flate_buffer);
    _ = try decompress.reader.streamRemaining(&out.writer);
    const inflated = try out.toOwnedSlice();
    errdefer alloc.free(inflated);
    if (inflated.len != expected_size) return error.ZipDecompressSizeMismatch;
    return inflated;
}

fn archiveEntryTextAlloc(alloc: Allocator, name: []const u8, bytes: []const u8) ![]u8 {
    if (archiveEntryLooksHtml(name, bytes)) return try htmlToTextAlloc(alloc, bytes);
    if (!archiveEntryLooksText(name, bytes)) return try alloc.alloc(u8, 0);
    return try alloc.dupe(u8, bytes);
}

fn archiveEntryLooksHtml(name: []const u8, bytes: []const u8) bool {
    return hasExtension(name, ".html") or hasExtension(name, ".htm") or looksLikeHtml(bytes);
}

fn archiveEntryLooksText(name: []const u8, bytes: []const u8) bool {
    if (hasExtension(name, ".txt") or
        hasExtension(name, ".md") or
        hasExtension(name, ".markdown") or
        hasExtension(name, ".csv") or
        hasExtension(name, ".json") or
        hasExtension(name, ".jsonl") or
        hasExtension(name, ".xml") or
        hasExtension(name, ".log"))
    {
        return looksLikePlainText(bytes);
    }
    return looksLikePlainText(bytes);
}

const OoxmlTextMode = enum {
    word,
    presentation,
};

fn ooxmlXmlTextAlloc(alloc: Allocator, xml: []const u8, mode: OoxmlTextMode) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var in_text = false;
    var cursor: usize = 0;
    while (cursor < xml.len) {
        const tag_start = std.mem.indexOfScalarPos(u8, xml, cursor, '<') orelse {
            if (in_text) try appendXmlDecodedText(alloc, &out, xml[cursor..]);
            break;
        };
        if (in_text and tag_start > cursor) try appendXmlDecodedText(alloc, &out, xml[cursor..tag_start]);
        const tag_end_pos = xmlTagEnd(xml, tag_start) orelse break;
        const tag = xml[tag_start + 1 .. tag_end_pos - 1];
        const local = xmlTagLocalName(tag) orelse {
            cursor = tag_end_pos;
            continue;
        };
        const is_end = xmlTagIsEnd(tag);
        const is_self_closing = xmlTagIsSelfClosing(tag);
        if (std.mem.eql(u8, local, "t")) {
            if (is_end) {
                in_text = false;
            } else if (!is_self_closing) {
                in_text = true;
            }
        } else if (mode == .word and !is_end and (std.mem.eql(u8, local, "tab") or std.mem.eql(u8, local, "br"))) {
            try appendOoxmlSeparator(alloc, &out, if (std.mem.eql(u8, local, "tab")) '\t' else '\n');
        } else if ((mode == .word or mode == .presentation) and is_end and std.mem.eql(u8, local, "p")) {
            try appendOoxmlSeparator(alloc, &out, '\n');
        }
        cursor = tag_end_pos;
    }
    trimTrailingAsciiWhitespace(&out);
    return try out.toOwnedSlice(alloc);
}

fn appendXmlDecodedText(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), text: []const u8) !void {
    var i: usize = 0;
    while (i < text.len) {
        if (text[i] != '&') {
            try out.append(alloc, text[i]);
            i += 1;
            continue;
        }
        const semi_offset = std.mem.indexOfScalar(u8, text[i..], ';') orelse {
            try out.append(alloc, text[i]);
            i += 1;
            continue;
        };
        const entity = text[i + 1 .. i + semi_offset];
        if (std.mem.eql(u8, entity, "amp")) {
            try out.append(alloc, '&');
        } else if (std.mem.eql(u8, entity, "lt")) {
            try out.append(alloc, '<');
        } else if (std.mem.eql(u8, entity, "gt")) {
            try out.append(alloc, '>');
        } else if (std.mem.eql(u8, entity, "quot")) {
            try out.append(alloc, '"');
        } else if (std.mem.eql(u8, entity, "apos")) {
            try out.append(alloc, '\'');
        } else if (entity.len > 1 and entity[0] == '#') {
            const codepoint = if (entity.len > 2 and (entity[1] == 'x' or entity[1] == 'X'))
                std.fmt.parseInt(u21, entity[2..], 16) catch null
            else
                std.fmt.parseInt(u21, entity[1..], 10) catch null;
            if (codepoint) |cp| {
                var buf: [4]u8 = undefined;
                const len = try std.unicode.utf8Encode(cp, &buf);
                try out.appendSlice(alloc, buf[0..len]);
            } else {
                try out.appendSlice(alloc, text[i .. i + semi_offset + 1]);
            }
        } else {
            try out.appendSlice(alloc, text[i .. i + semi_offset + 1]);
        }
        i += semi_offset + 1;
    }
}

fn appendOoxmlSeparator(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), sep: u8) !void {
    if (out.items.len == 0) return;
    const last = out.items[out.items.len - 1];
    if (last == sep) return;
    if (sep == '\n' and (last == ' ' or last == '\t')) trimTrailingHorizontalWhitespace(out);
    try out.append(alloc, sep);
}

fn trimTrailingAsciiWhitespace(out: *std.ArrayListUnmanaged(u8)) void {
    while (out.items.len > 0 and std.ascii.isWhitespace(out.items[out.items.len - 1])) {
        _ = out.pop();
    }
}

fn trimTrailingHorizontalWhitespace(out: *std.ArrayListUnmanaged(u8)) void {
    while (out.items.len > 0 and (out.items[out.items.len - 1] == ' ' or out.items[out.items.len - 1] == '\t')) {
        _ = out.pop();
    }
}

fn findXmlTagStart(xml: []const u8, start: usize, local_name: []const u8) ?usize {
    var cursor = start;
    while (std.mem.indexOfScalarPos(u8, xml, cursor, '<')) |pos| {
        const tag_end_pos = xmlTagEnd(xml, pos) orelse return null;
        const tag = xml[pos + 1 .. tag_end_pos - 1];
        if (!xmlTagIsEnd(tag)) {
            if (xmlTagLocalName(tag)) |local| {
                if (std.mem.eql(u8, local, local_name)) return pos;
            }
        }
        cursor = tag_end_pos;
    }
    return null;
}

fn findXmlEndTagAfter(xml: []const u8, start: usize, local_name: []const u8) ?usize {
    var cursor = start;
    while (std.mem.indexOfScalarPos(u8, xml, cursor, '<')) |pos| {
        const tag_end_pos = xmlTagEnd(xml, pos) orelse return null;
        const tag = xml[pos + 1 .. tag_end_pos - 1];
        if (xmlTagIsEnd(tag)) {
            if (xmlTagLocalName(tag)) |local| {
                if (std.mem.eql(u8, local, local_name)) return tag_end_pos;
            }
        }
        cursor = tag_end_pos;
    }
    return null;
}

fn xmlTagEnd(xml: []const u8, tag_start: usize) ?usize {
    const offset = std.mem.indexOfScalarPos(u8, xml, tag_start, '>') orelse return null;
    return offset + 1;
}

fn xmlTagIsEnd(tag: []const u8) bool {
    const trimmed = std.mem.trim(u8, tag, " \t\r\n");
    return trimmed.len > 0 and trimmed[0] == '/';
}

fn xmlTagIsSelfClosing(tag: []const u8) bool {
    const trimmed = std.mem.trim(u8, tag, " \t\r\n");
    return trimmed.len > 0 and trimmed[trimmed.len - 1] == '/';
}

fn xmlTagLocalName(tag: []const u8) ?[]const u8 {
    var trimmed = std.mem.trim(u8, tag, " \t\r\n");
    if (trimmed.len == 0) return null;
    if (trimmed[0] == '/' or trimmed[0] == '?' or trimmed[0] == '!') {
        trimmed = std.mem.trim(u8, trimmed[1..], " \t\r\n");
    }
    if (trimmed.len == 0) return null;
    const name_end = std.mem.indexOfAny(u8, trimmed, " \t\r\n/") orelse trimmed.len;
    const qname = trimmed[0..name_end];
    const colon = std.mem.lastIndexOfScalar(u8, qname, ':') orelse return qname;
    return qname[colon + 1 ..];
}

const OoxmlPart = struct {
    index: usize,
    text: []u8,

    fn deinit(self: *OoxmlPart, alloc: Allocator) void {
        if (self.text.len > 0) alloc.free(self.text);
        self.* = undefined;
    }

    fn lessThan(_: void, lhs: OoxmlPart, rhs: OoxmlPart) bool {
        return lhs.index < rhs.index;
    }
};

fn pptxSlideIndex(name: []const u8) ?usize {
    if (!std.mem.startsWith(u8, name, "ppt/slides/slide") or !std.mem.endsWith(u8, name, ".xml")) return null;
    const start = "ppt/slides/slide".len;
    return std.fmt.parseInt(usize, name[start .. name.len - ".xml".len], 10) catch null;
}

fn xlsxSheetIndex(name: []const u8) ?usize {
    if (!std.mem.startsWith(u8, name, "xl/worksheets/sheet") or !std.mem.endsWith(u8, name, ".xml")) return null;
    const start = "xl/worksheets/sheet".len;
    return std.fmt.parseInt(usize, name[start .. name.len - ".xml".len], 10) catch null;
}

fn xlsxSharedStringsAlloc(alloc: Allocator, xml: []const u8) ![][]u8 {
    var strings = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (strings.items) |string| alloc.free(string);
        strings.deinit(alloc);
    }
    var cursor: usize = 0;
    while (findXmlTagStart(xml, cursor, "si")) |si_start| {
        const si_start_end = xmlTagEnd(xml, si_start) orelse break;
        const si_end = findXmlEndTagAfter(xml, si_start_end, "si") orelse break;
        const text = try ooxmlXmlTextAlloc(alloc, xml[si_start_end .. si_end - "</si>".len], .word);
        errdefer alloc.free(text);
        try strings.append(alloc, text);
        cursor = si_end;
    }
    return try strings.toOwnedSlice(alloc);
}

fn freeStringList(alloc: Allocator, strings: [][]u8) void {
    for (strings) |string| alloc.free(string);
    if (strings.len > 0) alloc.free(strings);
}

fn xlsxSheetTextAlloc(alloc: Allocator, xml: []const u8, shared_strings: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var cursor: usize = 0;
    var first_row = true;
    while (findXmlTagStart(xml, cursor, "row")) |row_start| {
        const row_start_end = xmlTagEnd(xml, row_start) orelse break;
        const row_end = findXmlEndTagAfter(xml, row_start_end, "row") orelse break;
        const row = xml[row_start_end .. row_end - "</row>".len];
        const row_text = try xlsxRowTextAlloc(alloc, row, shared_strings);
        defer alloc.free(row_text);
        if (row_text.len > 0) {
            if (!first_row) try out.append(alloc, '\n');
            first_row = false;
            try out.appendSlice(alloc, row_text);
        }
        cursor = row_end;
    }
    trimTrailingAsciiWhitespace(&out);
    return try out.toOwnedSlice(alloc);
}

fn xlsxRowTextAlloc(alloc: Allocator, row_xml: []const u8, shared_strings: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var cursor: usize = 0;
    var first_cell = true;
    while (findXmlTagStart(row_xml, cursor, "c")) |cell_start| {
        const cell_start_end = xmlTagEnd(row_xml, cell_start) orelse break;
        const cell_end = findXmlEndTagAfter(row_xml, cell_start_end, "c") orelse break;
        const start_tag = row_xml[cell_start + 1 .. cell_start_end - 1];
        const cell = row_xml[cell_start_end .. cell_end - "</c>".len];
        const cell_text = try xlsxCellTextAlloc(alloc, start_tag, cell, shared_strings);
        defer alloc.free(cell_text);
        if (cell_text.len > 0) {
            if (!first_cell) try out.append(alloc, '\t');
            first_cell = false;
            try out.appendSlice(alloc, cell_text);
        }
        cursor = cell_end;
    }
    return try out.toOwnedSlice(alloc);
}

fn xlsxCellTextAlloc(alloc: Allocator, start_tag: []const u8, cell_xml: []const u8, shared_strings: []const []const u8) ![]u8 {
    if (xmlTagHasAttrValue(start_tag, "t", "s")) {
        const value = try xmlFirstElementTextAlloc(alloc, cell_xml, "v");
        defer alloc.free(value);
        const index = std.fmt.parseInt(usize, std.mem.trim(u8, value, " \t\r\n"), 10) catch return try alloc.alloc(u8, 0);
        if (index >= shared_strings.len) return try alloc.alloc(u8, 0);
        return try alloc.dupe(u8, shared_strings[index]);
    }
    if (xmlTagHasAttrValue(start_tag, "t", "inlineStr")) {
        return try ooxmlXmlTextAlloc(alloc, cell_xml, .word);
    }
    return try xmlFirstElementTextAlloc(alloc, cell_xml, "v");
}

fn xmlFirstElementTextAlloc(alloc: Allocator, xml: []const u8, local_name: []const u8) ![]u8 {
    const start = findXmlTagStart(xml, 0, local_name) orelse return try alloc.alloc(u8, 0);
    const start_end = xmlTagEnd(xml, start) orelse return try alloc.alloc(u8, 0);
    const end = findXmlEndTagAfter(xml, start_end, local_name) orelse return try alloc.alloc(u8, 0);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendXmlDecodedText(alloc, &out, xml[start_end .. end - (local_name.len + 3)]);
    trimTrailingAsciiWhitespace(&out);
    return try out.toOwnedSlice(alloc);
}

fn xmlTagHasAttrValue(tag: []const u8, attr_name: []const u8, expected: []const u8) bool {
    var cursor: usize = 0;
    while (std.mem.indexOfScalarPos(u8, tag, cursor, '=')) |eq| {
        const name_start = blk: {
            var pos = eq;
            while (pos > 0 and !std.ascii.isWhitespace(tag[pos - 1])) pos -= 1;
            break :blk pos;
        };
        const name = xmlLocalName(std.mem.trim(u8, tag[name_start..eq], " \t\r\n"));
        var value_start = eq + 1;
        while (value_start < tag.len and std.ascii.isWhitespace(tag[value_start])) value_start += 1;
        if (value_start >= tag.len or (tag[value_start] != '"' and tag[value_start] != '\'')) {
            cursor = eq + 1;
            continue;
        }
        const quote = tag[value_start];
        const value_body_start = value_start + 1;
        const value_end_offset = std.mem.indexOfScalar(u8, tag[value_body_start..], quote) orelse return false;
        const value = tag[value_body_start .. value_body_start + value_end_offset];
        if (std.mem.eql(u8, name, attr_name) and std.mem.eql(u8, value, expected)) return true;
        cursor = value_body_start + value_end_offset + 1;
    }
    return false;
}

fn xmlLocalName(qname: []const u8) []const u8 {
    const colon = std.mem.lastIndexOfScalar(u8, qname, ':') orelse return qname;
    return qname[colon + 1 ..];
}

const EmailHeaderBodySplit = struct {
    header_end: usize,
    body_start: usize,
};

fn emailHeaderBodySplit(bytes: []const u8) ?EmailHeaderBodySplit {
    if (std.mem.indexOf(u8, bytes, "\r\n\r\n")) |pos| {
        if (!looksLikeEmailHeaders(bytes[0..pos])) return null;
        return .{ .header_end = pos, .body_start = pos + 4 };
    }
    if (std.mem.indexOf(u8, bytes, "\n\n")) |pos| {
        if (!looksLikeEmailHeaders(bytes[0..pos])) return null;
        return .{ .header_end = pos, .body_start = pos + 2 };
    }
    return null;
}

fn looksLikeEmailHeaders(headers: []const u8) bool {
    var lines = std.mem.splitScalar(u8, headers, '\n');
    while (lines.next()) |raw_line| {
        const line = trimTrailingCarriageReturn(raw_line);
        if (line.len == 0) continue;
        if (line[0] == ' ' or line[0] == '\t') continue;
        if (std.mem.indexOfScalar(u8, line, ':') != null) return true;
    }
    return false;
}

fn normalizeEmailHeaderTextAlloc(alloc: Allocator, headers: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var lines = std.mem.splitScalar(u8, headers, '\n');
    var first = true;
    while (lines.next()) |raw_line| {
        const line = trimTrailingCarriageReturn(raw_line);
        if (line.len == 0) continue;
        if (line[0] == ' ' or line[0] == '\t') {
            try out.append(alloc, ' ');
            try out.appendSlice(alloc, std.mem.trim(u8, line, " \t"));
            continue;
        }
        if (!first) try out.append(alloc, '\n');
        first = false;
        try out.appendSlice(alloc, line);
    }
    return try out.toOwnedSlice(alloc);
}

fn appendEmailBodyUnits(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    headers: []const u8,
    body: []const u8,
    unit_prefix: []const u8,
    body_start: usize,
    message_len: usize,
) !void {
    if (try emailMultipartBoundaryAlloc(alloc, headers)) |boundary| {
        defer alloc.free(boundary);
        const appended = try appendEmailMultipartTextUnits(alloc, units, body, unit_prefix, body_start, boundary);
        if (appended > 0) return;
    }

    const body_text = try emailBodyTextAlloc(alloc, headers, body, false);
    errdefer alloc.free(body_text);
    if (body_text.len > 0) {
        const unit_id = try std.fmt.allocPrint(alloc, "{s}:body", .{unit_prefix});
        errdefer alloc.free(unit_id);
        try units.append(alloc, .{
            .unit_id = unit_id,
            .unit_type = try alloc.dupe(u8, "email_body"),
            .text = body_text,
            .method = try alloc.dupe(u8, "email_rfc822"),
            .char_start = std.math.cast(u32, body_start),
            .char_end = std.math.cast(u32, message_len),
        });
    } else {
        alloc.free(body_text);
    }
}

fn appendEmailMultipartTextUnits(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    body: []const u8,
    unit_prefix: []const u8,
    body_start: usize,
    boundary: []const u8,
) !usize {
    if (boundary.len == 0) return 0;
    var appended: usize = 0;
    var in_part = false;
    var part_start: usize = 0;
    var cursor: usize = 0;
    while (nextEmailLine(body, cursor)) |line| {
        if (emailBoundaryLineKind(line.text, boundary)) |kind| {
            if (in_part and line.start >= part_start) {
                if (try appendEmailMultipartTextPartUnit(alloc, units, body[part_start..line.start], unit_prefix, body_start + part_start, appended + 1)) {
                    appended += 1;
                }
            }
            if (kind == .closing) break;
            in_part = true;
            part_start = line.next;
        }
        cursor = line.next;
    }
    return appended;
}

const EmailLine = struct {
    start: usize,
    next: usize,
    text: []const u8,
};

fn nextEmailLine(bytes: []const u8, start: usize) ?EmailLine {
    if (start >= bytes.len) return null;
    const newline_offset = std.mem.indexOfScalar(u8, bytes[start..], '\n');
    const raw_end = if (newline_offset) |offset| start + offset else bytes.len;
    const next = if (newline_offset != null) raw_end + 1 else bytes.len;
    return .{
        .start = start,
        .next = next,
        .text = trimTrailingCarriageReturn(bytes[start..raw_end]),
    };
}

const EmailBoundaryLineKind = enum {
    part,
    closing,
};

fn emailBoundaryLineKind(line: []const u8, boundary: []const u8) ?EmailBoundaryLineKind {
    if (line.len < boundary.len + 2) return null;
    if (line[0] != '-' or line[1] != '-') return null;
    if (!std.mem.eql(u8, line[2 .. 2 + boundary.len], boundary)) return null;
    const suffix = line[2 + boundary.len ..];
    if (suffix.len == 0) return .part;
    if (std.mem.eql(u8, suffix, "--")) return .closing;
    return null;
}

fn appendEmailMultipartTextPartUnit(
    alloc: Allocator,
    units: *std.ArrayListUnmanaged(Unit),
    part: []const u8,
    unit_prefix: []const u8,
    part_message_start: usize,
    part_index: usize,
) !bool {
    const split = emailHeaderBodySplit(part) orelse return false;
    const part_headers = part[0..split.header_end];
    const part_body = part[split.body_start..];
    const part_content_type = emailHeaderValue(part_headers, "content-type") orelse "text/plain";
    const part_content_base = contentTypeBase(part_content_type);
    const strip_html = std.ascii.eqlIgnoreCase(part_content_base, "text/html");
    if (!strip_html and !std.ascii.eqlIgnoreCase(part_content_base, "text/plain")) return false;

    const part_text = try emailBodyTextAlloc(alloc, part_headers, part_body, strip_html);
    errdefer alloc.free(part_text);
    if (part_text.len == 0) {
        alloc.free(part_text);
        return false;
    }

    const unit_id = try std.fmt.allocPrint(alloc, "{s}:part:{d:0>6}", .{ unit_prefix, part_index });
    errdefer alloc.free(unit_id);
    try units.append(alloc, .{
        .unit_id = unit_id,
        .unit_type = try alloc.dupe(u8, "email_part"),
        .text = part_text,
        .method = try alloc.dupe(u8, if (strip_html) "email_html" else "email_text"),
        .char_start = std.math.cast(u32, part_message_start + split.body_start),
        .char_end = std.math.cast(u32, part_message_start + part.len),
    });
    return true;
}

fn emailBodyTextAlloc(alloc: Allocator, headers: []const u8, body: []const u8, strip_html: bool) ![]u8 {
    const encoding = emailHeaderValue(headers, "content-transfer-encoding") orelse "";
    const decoded = if (std.ascii.eqlIgnoreCase(encoding, "base64"))
        decodeBase64BodyAlloc(alloc, body) catch try alloc.dupe(u8, body)
    else if (std.ascii.eqlIgnoreCase(encoding, "quoted-printable"))
        try decodeQuotedPrintableAlloc(alloc, body)
    else
        try alloc.dupe(u8, body);
    errdefer alloc.free(decoded);
    if (!strip_html) return decoded;
    const text = try htmlToTextAlloc(alloc, decoded);
    alloc.free(decoded);
    return text;
}

fn emailHeaderValue(headers: []const u8, name_lower: []const u8) ?[]const u8 {
    var lines = std.mem.splitScalar(u8, headers, '\n');
    while (lines.next()) |raw_line| {
        const line = trimTrailingCarriageReturn(raw_line);
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        if (!std.ascii.eqlIgnoreCase(name, name_lower)) continue;
        return std.mem.trim(u8, line[colon + 1 ..], " \t");
    }
    return null;
}

fn emailMultipartBoundaryAlloc(alloc: Allocator, headers: []const u8) !?[]u8 {
    const content_type = emailHeaderValue(headers, "content-type") orelse return null;
    if (!contentTypeStartsWith(content_type, "multipart/")) return null;
    const boundary = contentTypeParameter(content_type, "boundary") orelse return null;
    return try alloc.dupe(u8, boundary);
}

fn contentTypeParameter(content_type: []const u8, name_lower: []const u8) ?[]const u8 {
    var parts = std.mem.splitScalar(u8, content_type, ';');
    _ = parts.next();
    while (parts.next()) |part| {
        const trimmed = std.mem.trim(u8, part, " \t\r\n");
        const eq = std.mem.indexOfScalar(u8, trimmed, '=') orelse continue;
        const name = std.mem.trim(u8, trimmed[0..eq], " \t");
        if (!std.ascii.eqlIgnoreCase(name, name_lower)) continue;
        var value = std.mem.trim(u8, trimmed[eq + 1 ..], " \t");
        if (value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"') {
            value = value[1 .. value.len - 1];
        }
        return value;
    }
    return null;
}

fn trimTrailingCarriageReturn(line: []const u8) []const u8 {
    if (line.len > 0 and line[line.len - 1] == '\r') return line[0 .. line.len - 1];
    return line;
}

fn decodeBase64BodyAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var compact = std.ArrayListUnmanaged(u8).empty;
    defer compact.deinit(alloc);
    for (body) |byte| {
        if (std.ascii.isWhitespace(byte)) continue;
        try compact.append(alloc, byte);
    }
    const decoded_len = try std.base64.standard.Decoder.calcSizeForSlice(compact.items);
    const decoded = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, compact.items);
    return decoded;
}

fn decodeQuotedPrintableAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var i: usize = 0;
    while (i < body.len) {
        if (body[i] != '=') {
            try out.append(alloc, body[i]);
            i += 1;
            continue;
        }
        if (i + 1 < body.len and body[i + 1] == '\n') {
            i += 2;
            continue;
        }
        if (i + 2 < body.len and body[i + 1] == '\r' and body[i + 2] == '\n') {
            i += 3;
            continue;
        }
        if (i + 2 < body.len) {
            const hi = std.fmt.charToDigit(body[i + 1], 16) catch null;
            const lo = std.fmt.charToDigit(body[i + 2], 16) catch null;
            if (hi != null and lo != null) {
                try out.append(alloc, @as(u8, @intCast(hi.? * 16 + lo.?)));
                i += 3;
                continue;
            }
        }
        try out.append(alloc, body[i]);
        i += 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn htmlToTextAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var in_tag = false;
    var last_was_space = true;
    for (bytes) |c| {
        if (c == '<') {
            in_tag = true;
            if (!last_was_space) {
                try out.append(alloc, ' ');
                last_was_space = true;
            }
            continue;
        }
        if (c == '>') {
            in_tag = false;
            continue;
        }
        if (in_tag) continue;
        if (std.ascii.isWhitespace(c)) {
            if (!last_was_space) {
                try out.append(alloc, ' ');
                last_was_space = true;
            }
            continue;
        }
        try out.append(alloc, c);
        last_was_space = false;
    }
    while (out.items.len > 0 and std.ascii.isWhitespace(out.items[out.items.len - 1])) {
        _ = out.pop();
    }
    return try out.toOwnedSlice(alloc);
}

fn isPdfContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeEquals(content_type, "application/pdf")) return true;
    if (hasExtension(filename, ".pdf") or hasExtension(source_url, ".pdf")) return true;
    return std.mem.startsWith(u8, bytes, "%PDF-");
}

fn isHtmlContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeEquals(content_type, "text/html")) return true;
    if (hasExtension(filename, ".html") or hasExtension(filename, ".htm") or
        hasExtension(source_url, ".html") or hasExtension(source_url, ".htm"))
    {
        return true;
    }
    return looksLikeHtml(bytes);
}

fn isEmailContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeEquals(content_type, "message/rfc822")) return true;
    if (hasExtension(filename, ".eml") or hasExtension(source_url, ".eml")) return true;
    return contentTypeAllowsTextSniff(content_type) and emailHeaderBodySplit(bytes) != null;
}

fn isImageContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeStartsWith(content_type, "image/")) return true;
    if (hasExtension(filename, ".png") or hasExtension(source_url, ".png") or
        hasExtension(filename, ".jpg") or hasExtension(source_url, ".jpg") or
        hasExtension(filename, ".jpeg") or hasExtension(source_url, ".jpeg") or
        hasExtension(filename, ".gif") or hasExtension(source_url, ".gif") or
        hasExtension(filename, ".webp") or hasExtension(source_url, ".webp") or
        hasExtension(filename, ".tif") or hasExtension(source_url, ".tif") or
        hasExtension(filename, ".tiff") or hasExtension(source_url, ".tiff") or
        hasExtension(filename, ".bmp") or hasExtension(source_url, ".bmp"))
    {
        return true;
    }
    return std.mem.startsWith(u8, bytes, "\x89PNG\r\n\x1a\n") or
        std.mem.startsWith(u8, bytes, "\xff\xd8\xff") or
        std.mem.startsWith(u8, bytes, "GIF87a") or
        std.mem.startsWith(u8, bytes, "GIF89a") or
        (std.mem.startsWith(u8, bytes, "RIFF") and bytes.len >= 12 and std.mem.eql(u8, bytes[8..12], "WEBP"));
}

fn isAudioContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeStartsWith(content_type, "audio/")) return true;
    if (hasExtension(filename, ".mp3") or hasExtension(source_url, ".mp3") or
        hasExtension(filename, ".wav") or hasExtension(source_url, ".wav") or
        hasExtension(filename, ".m4a") or hasExtension(source_url, ".m4a") or
        hasExtension(filename, ".aac") or hasExtension(source_url, ".aac") or
        hasExtension(filename, ".ogg") or hasExtension(source_url, ".ogg") or
        hasExtension(filename, ".opus") or hasExtension(source_url, ".opus") or
        hasExtension(filename, ".flac") or hasExtension(source_url, ".flac"))
    {
        return true;
    }
    return std.mem.startsWith(u8, bytes, "ID3") or
        std.mem.startsWith(u8, bytes, "OggS") or
        std.mem.startsWith(u8, bytes, "fLaC") or
        (std.mem.startsWith(u8, bytes, "RIFF") and bytes.len >= 12 and std.mem.eql(u8, bytes[8..12], "WAVE"));
}

fn isDocxContent(content_type: []const u8, filename: []const u8, source_url: []const u8) bool {
    if (contentTypeEquals(content_type, "application/vnd.openxmlformats-officedocument.wordprocessingml.document")) return true;
    return hasExtension(filename, ".docx") or hasExtension(source_url, ".docx");
}

fn isPptxContent(content_type: []const u8, filename: []const u8, source_url: []const u8) bool {
    if (contentTypeEquals(content_type, "application/vnd.openxmlformats-officedocument.presentationml.presentation")) return true;
    return hasExtension(filename, ".pptx") or hasExtension(source_url, ".pptx");
}

fn isXlsxContent(content_type: []const u8, filename: []const u8, source_url: []const u8) bool {
    if (contentTypeEquals(content_type, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")) return true;
    return hasExtension(filename, ".xlsx") or hasExtension(source_url, ".xlsx");
}

fn isZipArchiveContent(content_type: []const u8, filename: []const u8, source_url: []const u8) bool {
    if (contentTypeEquals(content_type, "application/zip")) return true;
    if (contentTypeEquals(content_type, "application/x-zip-compressed")) return true;
    if (contentTypeEquals(content_type, "multipart/x-zip")) return true;
    return hasExtension(filename, ".zip") or hasExtension(source_url, ".zip");
}

fn isTextContent(content_type: []const u8, filename: []const u8, source_url: []const u8, bytes: []const u8) bool {
    if (contentTypeStartsWith(content_type, "text/")) return true;
    if (contentTypeEquals(content_type, "application/json")) return true;
    if (hasExtension(filename, ".txt") or hasExtension(source_url, ".txt") or
        hasExtension(filename, ".json") or hasExtension(source_url, ".json") or
        hasExtension(filename, ".csv") or hasExtension(source_url, ".csv"))
    {
        return true;
    }
    return contentTypeAllowsTextSniff(content_type) and looksLikePlainText(bytes);
}

fn looksLikeHtml(bytes: []const u8) bool {
    const trimmed = std.mem.trim(u8, bytes, " \t\r\n");
    if (trimmed.len == 0 or trimmed[0] != '<') return false;
    return startsWithIgnoreCase(trimmed, "<!doctype html") or
        startsWithIgnoreCase(trimmed, "<html") or
        startsWithIgnoreCase(trimmed, "<head") or
        startsWithIgnoreCase(trimmed, "<body") or
        startsWithIgnoreCase(trimmed, "<title") or
        startsWithIgnoreCase(trimmed, "<meta");
}

fn contentTypeAllowsTextSniff(content_type: []const u8) bool {
    const base = contentTypeBase(content_type);
    return base.len == 0 or
        std.ascii.eqlIgnoreCase(base, "application/octet-stream") or
        std.ascii.eqlIgnoreCase(base, "binary/octet-stream") or
        std.ascii.eqlIgnoreCase(base, "application/x-unknown-content-type");
}

fn looksLikePlainText(bytes: []const u8) bool {
    if (bytes.len == 0) return false;
    _ = std.unicode.utf8CountCodepoints(bytes) catch return false;
    var printable: usize = 0;
    for (bytes) |byte| {
        if (byte == 0) return false;
        if (byte == '\n' or byte == '\r' or byte == '\t') continue;
        if (byte < 0x20) return false;
        printable += 1;
    }
    return printable > 0;
}

fn startsWithIgnoreCase(value: []const u8, prefix: []const u8) bool {
    if (value.len < prefix.len) return false;
    return std.ascii.eqlIgnoreCase(value[0..prefix.len], prefix);
}

fn hasExtension(value: []const u8, extension: []const u8) bool {
    const cleaned = trimUrlSuffix(value);
    if (cleaned.len < extension.len) return false;
    return std.ascii.eqlIgnoreCase(cleaned[cleaned.len - extension.len ..], extension);
}

fn hasConfiguredExtension(value: []const u8, extension: []const u8) bool {
    if (extension.len == 0) return false;
    if (extension[0] == '.') return hasExtension(value, extension);
    const cleaned = trimUrlSuffix(value);
    if (cleaned.len < extension.len) return false;
    if (!std.ascii.eqlIgnoreCase(cleaned[cleaned.len - extension.len ..], extension)) return false;
    if (cleaned.len == extension.len) return true;
    return cleaned[cleaned.len - extension.len - 1] == '.';
}

fn trimUrlSuffix(value: []const u8) []const u8 {
    var end = value.len;
    if (std.mem.indexOfScalar(u8, value, '?')) |pos| end = @min(end, pos);
    if (std.mem.indexOfScalar(u8, value, '#')) |pos| end = @min(end, pos);
    return value[0..end];
}

fn contentTypeBase(content_type: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, content_type, ';') orelse content_type.len;
    return std.mem.trim(u8, content_type[0..end], " \t\r\n");
}

fn contentTypeEquals(content_type: []const u8, expected: []const u8) bool {
    return std.ascii.eqlIgnoreCase(contentTypeBase(content_type), expected);
}

fn contentTypeStartsWith(content_type: []const u8, prefix: []const u8) bool {
    const base = contentTypeBase(content_type);
    if (base.len < prefix.len) return false;
    return std.ascii.eqlIgnoreCase(base[0..prefix.len], prefix);
}

const TestDownloadedContent = struct {
    content_type: []u8,
    data: []u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.data);
    }
};

const TestZipEntry = struct {
    name: []const u8,
    data: []const u8,
};

fn buildStoredZipAlloc(alloc: Allocator, entries: []const TestZipEntry) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var central = std.ArrayListUnmanaged(u8).empty;
    defer central.deinit(alloc);

    for (entries) |entry| {
        const offset = out.items.len;
        const crc = std.hash.crc.Crc32.hash(entry.data);
        try appendZipLe32(alloc, &out, 0x04034b50);
        try appendZipLe16(alloc, &out, 20);
        try appendZipLe16(alloc, &out, 0);
        try appendZipLe16(alloc, &out, 0);
        try appendZipLe16(alloc, &out, 0);
        try appendZipLe16(alloc, &out, 0);
        try appendZipLe32(alloc, &out, crc);
        try appendZipLe32(alloc, &out, @intCast(entry.data.len));
        try appendZipLe32(alloc, &out, @intCast(entry.data.len));
        try appendZipLe16(alloc, &out, @intCast(entry.name.len));
        try appendZipLe16(alloc, &out, 0);
        try out.appendSlice(alloc, entry.name);
        try out.appendSlice(alloc, entry.data);

        try appendZipLe32(alloc, &central, 0x02014b50);
        try appendZipLe16(alloc, &central, 20);
        try appendZipLe16(alloc, &central, 20);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe32(alloc, &central, crc);
        try appendZipLe32(alloc, &central, @intCast(entry.data.len));
        try appendZipLe32(alloc, &central, @intCast(entry.data.len));
        try appendZipLe16(alloc, &central, @intCast(entry.name.len));
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe16(alloc, &central, 0);
        try appendZipLe32(alloc, &central, 0);
        try appendZipLe32(alloc, &central, @intCast(offset));
        try central.appendSlice(alloc, entry.name);
    }

    const central_offset = out.items.len;
    try out.appendSlice(alloc, central.items);
    try appendZipLe32(alloc, &out, 0x06054b50);
    try appendZipLe16(alloc, &out, 0);
    try appendZipLe16(alloc, &out, 0);
    try appendZipLe16(alloc, &out, @intCast(entries.len));
    try appendZipLe16(alloc, &out, @intCast(entries.len));
    try appendZipLe32(alloc, &out, @intCast(central.items.len));
    try appendZipLe32(alloc, &out, @intCast(central_offset));
    try appendZipLe16(alloc, &out, 0);
    return try out.toOwnedSlice(alloc);
}

fn appendZipLe16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn appendZipLe32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

test "document extraction extracts text data uri content as single document unit" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/plain"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "data:text/plain,alpha%20beta", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("document:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("alpha beta", result.units[0].text);
    try std.testing.expectEqual(@as(?u32, 0), result.units[0].char_start);
    try std.testing.expectEqual(@as(?u32, 10), result.units[0].char_end);
}

test "document extraction routes configured extensions into text units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"filename":"notes.md","routes":[{"match":{"extension":["md"]},"extractor":{"type":"text","unit":"note"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download?id=1", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("note:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("note", result.units[0].unit_type);
    try std.testing.expectEqualStrings("alpha beta", result.units[0].text);
}

test "document extraction explicit-only route preset disables builtin fallback" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/plain"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"route_preset":"explicit_only","routes":[{"match":{"extension":"md"},"extractor":{"type":"text","unit":"note"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/file.txt", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("unsupported", result.route_type);
    try std.testing.expectEqualStrings("no_configured_route_matched", result.unsupported_reason);
    try std.testing.expectEqual(@as(usize, 0), result.units.len);
}

test "document extraction explicit-only route preset still uses configured route matches" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"route_preset":"explicit_only","routes":[{"match":{"extension":"md"},"extractor":{"type":"text","unit":"note"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/notes.md", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("note:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("alpha beta", result.units[0].text);
}

test "document extraction routes configured magic prefixes into text units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "# title\nbody"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"magic_prefix":"# "},"extractor":{"type":"text","unit":"markdown"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("markdown:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("markdown", result.units[0].unit_type);
    try std.testing.expectEqualStrings("# title\nbody", result.units[0].text);
}

test "document extraction sniffs html bytes without metadata" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "  <!doctype html><html><body><h1>Alpha</h1></body></html>"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("html", result.route_type);
    try std.testing.expectEqualStrings("article:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("article", result.units[0].unit_type);
    try std.testing.expectEqualStrings("Alpha", result.units[0].text);
}

test "document extraction sniffs utf8 text bytes without metadata" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "plain text\nsecond line"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqualStrings("document:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("document", result.units[0].unit_type);
    try std.testing.expectEqualStrings("plain text\nsecond line", result.units[0].text);
}

test "document extraction routes rfc822 email into header and body units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "message/rfc822"),
        .data = try alloc.dupe(u8, "Subject: Alpha\r\nFrom: a@example.test\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nHello=20world=\r\n!"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/message.eml", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("email", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("email:headers", result.units[0].unit_id);
    try std.testing.expectEqualStrings("email_headers", result.units[0].unit_type);
    try std.testing.expect(std.mem.indexOf(u8, result.units[0].text, "Subject: Alpha") != null);
    try std.testing.expectEqualStrings("email:body", result.units[1].unit_id);
    try std.testing.expectEqualStrings("email_body", result.units[1].unit_type);
    try std.testing.expectEqualStrings("Hello world!", result.units[1].text);
    try std.testing.expectEqualStrings("email_rfc822", result.units[1].method);
}

test "document extraction routes configured email extractor with unit prefix" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "Subject: Alpha\n\nBody"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"extension":"mail"},"extractor":{"type":"email","unit":"message"}}],"filename":"thread.mail"}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/download", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("email", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("message:headers", result.units[0].unit_id);
    try std.testing.expectEqualStrings("message:body", result.units[1].unit_id);
    try std.testing.expectEqualStrings("Body", result.units[1].text);
}

test "document extraction extracts multipart rfc822 text parts" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "message/rfc822"),
        .data = try alloc.dupe(
            u8,
            "Subject: Alpha\r\nContent-Type: multipart/alternative; boundary=\"b1\"\r\n\r\n" ++
                "--b1\r\nContent-Type: text/plain\r\n\r\nPlain body\r\n" ++
                "--b1\r\nContent-Type: text/html\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n<p>HTML=20body</p>\r\n" ++
                "--b1\r\nContent-Type: application/octet-stream\r\n\r\nopaque\r\n" ++
                "--b1--\r\n",
        ),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/message.eml", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("email", result.route_type);
    try std.testing.expectEqual(@as(usize, 3), result.units.len);
    try std.testing.expectEqualStrings("email:headers", result.units[0].unit_id);
    try std.testing.expectEqualStrings("email:part:000001", result.units[1].unit_id);
    try std.testing.expectEqualStrings("email_part", result.units[1].unit_type);
    try std.testing.expectEqualStrings("email_text", result.units[1].method);
    try std.testing.expectEqualStrings("Plain body\r\n", result.units[1].text);
    try std.testing.expectEqualStrings("email:part:000002", result.units[2].unit_id);
    try std.testing.expectEqualStrings("email_html", result.units[2].method);
    try std.testing.expectEqualStrings("HTML body", result.units[2].text);
}

test "document extraction extracts docx sections from ooxml package" {
    const alloc = std.testing.allocator;
    const zip = try buildStoredZipAlloc(alloc, &.{
        .{
            .name = "word/document.xml",
            .data =
            \\<?xml version="1.0" encoding="UTF-8"?>
            \\<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
            \\  <w:body>
            \\    <w:p><w:r><w:t>Alpha &amp; Beta</w:t></w:r></w:p>
            \\    <w:p><w:r><w:t>First section</w:t></w:r><w:sectPr/></w:p>
            \\    <w:p><w:r><w:t>Second section</w:t></w:r></w:p>
            \\  </w:body>
            \\</w:document>
            ,
        },
    });
    defer alloc.free(zip);
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        .data = try alloc.dupe(u8, zip),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/report.docx", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("docx", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("section:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("section", result.units[0].unit_type);
    try std.testing.expectEqualStrings("docx_text", result.units[0].method);
    try std.testing.expectEqualStrings("Alpha & Beta\nFirst section", result.units[0].text);
    try std.testing.expectEqualStrings("section:000002", result.units[1].unit_id);
    try std.testing.expectEqualStrings("Second section", result.units[1].text);
}

test "document extraction extracts pptx slides in numeric order" {
    const alloc = std.testing.allocator;
    const zip = try buildStoredZipAlloc(alloc, &.{
        .{
            .name = "ppt/slides/slide2.xml",
            .data = "<p:sld xmlns:a=\"a\" xmlns:p=\"p\"><p:cSld><a:p><a:r><a:t>Second slide</a:t></a:r></a:p></p:cSld></p:sld>",
        },
        .{
            .name = "ppt/slides/slide1.xml",
            .data = "<p:sld xmlns:a=\"a\" xmlns:p=\"p\"><p:cSld><a:p><a:r><a:t>First slide</a:t></a:r></a:p></p:cSld></p:sld>",
        },
    });
    defer alloc.free(zip);
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        .data = try alloc.dupe(u8, zip),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/deck.pptx", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("pptx", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("slide:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("slide", result.units[0].unit_type);
    try std.testing.expectEqualStrings("pptx_text", result.units[0].method);
    try std.testing.expectEqualStrings("First slide", result.units[0].text);
    try std.testing.expectEqualStrings("slide:000002", result.units[1].unit_id);
    try std.testing.expectEqualStrings("Second slide", result.units[1].text);
}

test "document extraction extracts xlsx sheets with shared strings" {
    const alloc = std.testing.allocator;
    const zip = try buildStoredZipAlloc(alloc, &.{
        .{
            .name = "xl/sharedStrings.xml",
            .data = "<sst><si><t>Name</t></si><si><t>Ada &amp; Bob</t></si></sst>",
        },
        .{
            .name = "xl/worksheets/sheet1.xml",
            .data =
            \\<worksheet>
            \\  <sheetData>
            \\    <row><c t="s"><v>0</v></c><c t="inlineStr"><is><t>Score</t></is></c></row>
            \\    <row><c t="s"><v>1</v></c><c><v>42</v></c></row>
            \\  </sheetData>
            \\</worksheet>
            ,
        },
    });
    defer alloc.free(zip);
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        .data = try alloc.dupe(u8, zip),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/book.xlsx", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("xlsx", result.route_type);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("sheet:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("sheet", result.units[0].unit_type);
    try std.testing.expectEqualStrings("xlsx_text", result.units[0].method);
    try std.testing.expectEqualStrings("Name\tScore\nAda & Bob\t42", result.units[0].text);
}

test "document extraction extracts zip archive text entries with source paths" {
    const alloc = std.testing.allocator;
    const zip = try buildStoredZipAlloc(alloc, &.{
        .{
            .name = "z-last.txt",
            .data = "Last text",
        },
        .{
            .name = "a/page.html",
            .data = "<h1>Heading</h1><p>Body &amp; more</p>",
        },
        .{
            .name = "../escape.txt",
            .data = "skip me",
        },
        .{
            .name = "bin/data.bin",
            .data = "abc\x00def",
        },
    });
    defer alloc.free(zip);
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/zip"),
        .data = try alloc.dupe(u8, zip),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/archive.zip", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("archive", result.route_type);
    try std.testing.expectEqual(@as(usize, 2), result.units.len);
    try std.testing.expectEqualStrings("archive:entry:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("archive_entry", result.units[0].unit_type);
    try std.testing.expectEqualStrings("zip_html", result.units[0].method);
    try std.testing.expectEqualStrings("a/page.html", result.units[0].source_path.?);
    try std.testing.expectEqualStrings("Heading Body &amp; more", result.units[0].text);
    try std.testing.expectEqualStrings("archive:entry:000002", result.units[1].unit_id);
    try std.testing.expectEqualStrings("zip_text", result.units[1].method);
    try std.testing.expectEqualStrings("z-last.txt", result.units[1].source_path.?);
    try std.testing.expectEqualStrings("Last text", result.units[1].text);
}

test "document extraction routes image content to pending OCR unit" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "image/png"),
        .data = try alloc.dupe(u8, "\x89PNG\r\n\x1a\nimage bytes"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/image.png", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("image", result.route_type);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("image:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("image", result.units[0].unit_type);
    try std.testing.expectEqualStrings("ocr_pending", result.units[0].method);
    try std.testing.expectEqualStrings("pending_ocr", result.units[0].extraction_status.?);
    try std.testing.expectEqual(@as(usize, 0), result.units[0].text.len);
    try std.testing.expectEqual(@as(u64, 19), result.units[0].byte_length.?);
    try std.testing.expectEqual(@as(usize, 64), result.units[0].source_sha256.?.len);
    try std.testing.expect(!result.units[0].ocr_used);
    try std.testing.expect(!result.units[0].transcript_used);
}

test "document extraction applies configured audio transcript route" {
    const alloc = std.testing.allocator;
    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"content_type_prefix":"audio/"},"extractor":{"type":"transcript","unit":"transcript_segment"}}]}
    );
    defer config.deinit(alloc);
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "audio/mpeg"),
        .data = try alloc.dupe(u8, "ID3audio bytes"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/audio.mp3", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("audio", result.route_type);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("transcript_segment:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("transcript_segment", result.units[0].unit_type);
    try std.testing.expectEqualStrings("transcript_pending", result.units[0].method);
    try std.testing.expectEqualStrings("pending_transcription", result.units[0].extraction_status.?);
    try std.testing.expectEqual(@as(usize, 0), result.units[0].text.len);
    try std.testing.expectEqual(@as(u64, 14), result.units[0].byte_length.?);
    try std.testing.expectEqual(@as(usize, 64), result.units[0].source_sha256.?.len);
    try std.testing.expect(!result.units[0].ocr_used);
    try std.testing.expect(!result.units[0].transcript_used);
}

test "document extraction parses generated OCR and transcription config" {
    const alloc = std.testing.allocator;
    var config = try parseConfig(alloc,
        \\{
        \\  "ocr_fallback": true,
        \\  "ocr": {"config": {"provider": "mock-reader"}},
        \\  "transcription": {"enabled": true, "config": {"provider": "mock-transcriber"}}
        \\}
    );
    defer config.deinit(alloc);

    try std.testing.expect(config.ocr_enabled);
    try std.testing.expect(config.transcription_enabled);
    try std.testing.expect(std.mem.indexOf(u8, config.ocr_config_json, "mock-reader") != null);
    try std.testing.expect(std.mem.indexOf(u8, config.transcription_config_json, "mock-transcriber") != null);
}

test "document extraction applies source metadata fields from row json" {
    const alloc = std.testing.allocator;
    var config = try parseConfig(alloc,
        \\{"source":{"filename_field":"filename","content_type_field":"mime_type","etag_field":"etag","checksum_field":"sha256","version_field":"rev","last_modified_field":"updated_at"}}
    );
    defer config.deinit(alloc);

    try applySourceMetadataFromJson(alloc, &config,
        \\{"filename":"contract.pdf","mime_type":"application/pdf","etag":"abc123","sha256":"def456","rev":"7","updated_at":"2026-06-11T12:00:00Z"}
    );
    try std.testing.expectEqualStrings("contract.pdf", config.filename);
    try std.testing.expectEqualStrings("application/pdf", config.content_type);
    try std.testing.expectEqualStrings("abc123", config.etag);
    try std.testing.expectEqualStrings("def456", config.checksum);
    try std.testing.expectEqualStrings("7", config.version);
    try std.testing.expectEqualStrings("2026-06-11T12:00:00Z", config.last_modified);
}

test "document extraction metadata fingerprint requires version metadata" {
    const alloc = std.testing.allocator;
    var config = try parseConfig(alloc,
        \\{"filename":"contract.pdf","content_type":"application/pdf"}
    );
    defer config.deinit(alloc);

    try std.testing.expect((try metadataFingerprintAlloc(alloc, "data:application/pdf;base64,AA==", "{}", config)) == null);

    if (config.version.len > 0) alloc.free(@constCast(config.version));
    config.version = try alloc.dupe(u8, "1");
    const first = (try metadataFingerprintAlloc(alloc, "data:application/pdf;base64,AA==", "{}", config)).?;
    defer alloc.free(first);

    if (config.version.len > 0) alloc.free(@constCast(config.version));
    config.version = try alloc.dupe(u8, "2");
    const second = (try metadataFingerprintAlloc(alloc, "data:application/pdf;base64,AA==", "{}", config)).?;
    defer alloc.free(second);

    try std.testing.expect(!std.mem.eql(u8, first, second));
}

test "document extraction route matching normalizes content type parameters" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/html; charset=utf-8"),
        .data = try alloc.dupe(u8, "<h1>Alpha</h1><p>Beta</p>"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"content_type":"text/html"},"extractor":{"type":"html","unit":"article"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/doc", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("html", result.route_type);
    try std.testing.expectEqualStrings("article:000001", result.units[0].unit_id);
    try std.testing.expectEqualStrings("Alpha Beta", result.units[0].text);
}

test "document extraction can route configured unsupported files without searchable units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/x-custom"),
        .data = try alloc.dupe(u8, "opaque"),
    };
    defer downloaded.deinit(alloc);

    var config = try parseConfig(alloc,
        \\{"routes":[{"match":{"content_type_prefix":"application/x-"},"extractor":{"type":"unsupported"}}]}
    );
    defer config.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/file.bin", config);
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("unsupported", result.route_type);
    try std.testing.expectEqualStrings("matched_unsupported_route", result.unsupported_reason);
    try std.testing.expectEqual(@as(usize, 0), result.units.len);
}

test "document extraction strips simple html tags" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/html"),
        .data = try alloc.dupe(u8, "<h1>Alpha</h1><p>Beta</p>"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "https://example.test/doc.html", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("article", result.units[0].unit_type);
    try std.testing.expectEqualStrings("Alpha Beta", result.units[0].text);
}

test "document extraction streaming emits text unit without buffering result" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "text/plain"),
        .data = try alloc.dupe(u8, "alpha beta"),
    };
    defer downloaded.deinit(alloc);

    const Ctx = struct {
        begin_count: usize = 0,
        unit_count: usize = 0,
        end_count: usize = 0,
        route_type: []const u8 = "",
        saw_unit_text: bool = false,

        fn onBegin(ptr: *anyopaque, info: StreamInfo) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_count += 1;
            self.route_type = info.route_type;
        }

        fn onUnit(ptr: *anyopaque, unit: *Unit) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.unit_count += 1;
            try std.testing.expectEqualStrings("alpha beta", unit.text);
            self.saw_unit_text = true;
        }

        fn onEnd(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.end_count += 1;
        }
    };

    var ctx = Ctx{};
    try extractDownloadedStreaming(alloc, downloaded, "https://example.test/doc.txt", .{}, .{
        .ptr = &ctx,
        .on_begin = Ctx.onBegin,
        .on_unit = Ctx.onUnit,
        .on_end = Ctx.onEnd,
    });
    try std.testing.expectEqual(@as(usize, 1), ctx.begin_count);
    try std.testing.expectEqual(@as(usize, 1), ctx.unit_count);
    try std.testing.expectEqual(@as(usize, 1), ctx.end_count);
    try std.testing.expectEqualStrings("text", ctx.route_type);
    try std.testing.expect(ctx.saw_unit_text);
}

test "document extraction classifies unsupported content without units" {
    const alloc = std.testing.allocator;
    var downloaded = TestDownloadedContent{
        .content_type = try alloc.dupe(u8, "application/octet-stream"),
        .data = try alloc.dupe(u8, "\x00\x01\x02"),
    };
    defer downloaded.deinit(alloc);

    var result = try extractDownloadedAlloc(alloc, downloaded, "data:application/octet-stream;base64,AAEC", .{});
    defer result.deinit(alloc);
    try std.testing.expectEqualStrings("unsupported", result.route_type);
    try std.testing.expectEqualStrings("unsupported_content_type", result.unsupported_reason);
    try std.testing.expectEqual(@as(usize, 0), result.units.len);
}

test "OCR quality fallback covers born digital scanned garbled and encoding failures" {
    const config = OcrQualityConfig{};
    const born_digital = assessOcrQuality("This is a normal born-digital document page with enough readable English text to be indexed without optical character recognition.", config);
    try std.testing.expect(!born_digital.needsFallback());
    try std.testing.expect(assessOcrQuality("", config).too_short);

    const garbled = assessOcrQuality("a b c d e f g h i j k l m n o p q r s t u v w y z a b c d e f", config);
    try std.testing.expect(garbled.garbled);

    const replacement = assessOcrQuality("Readable source text that is long enough but contains replacement markers \u{fffd} \u{fffd} \u{fffd} \u{fffd} \u{fffd} \u{fffd}.", config);
    try std.testing.expect(replacement.replacement_corrupted);

    try std.testing.expectEqual(@as(u8, 4), (OcrQuality{
        .too_short = true,
        .garbled = true,
        .font_corrupted = true,
        .replacement_corrupted = true,
    }).failureCount());
}

test "OCR text selection retains embedded text on ties and chooses a better transcription" {
    const config = OcrQualityConfig{};
    const embedded = assessOcrQuality("a b c d e f g h i j k l m n o p q r s t u v w y z a b c d e f", config);
    const ocr = assessOcrQuality("A faithfully transcribed table page with readable headings and enough ordinary English content to pass all quality checks.", config);
    try std.testing.expect(preferOcrText(embedded, ocr));
    try std.testing.expect(!preferOcrText(ocr, ocr));
}

test "OCR options parse configurable thresholds resolution and layout prompt" {
    const alloc = std.testing.allocator;
    var config = try parseConfig(alloc,
        \\{"ocr":{"enabled":true,"mode":"always","render_dpi":200,"max_rendered_pixels":123456,"quality":{"min_content_chars":75,"max_replacement_char_ratio":0.1},"config":{"provider":"antfly","prompt":"Preserve tables"}}}
    );
    defer config.deinit(alloc);
    try std.testing.expect(config.ocr_enabled);
    try std.testing.expectEqual(OcrMode.always, config.ocr_mode);
    try std.testing.expectEqual(@as(u16, 200), config.ocr_render_dpi);
    try std.testing.expectEqual(@as(u64, 123456), config.ocr_max_rendered_pixels);
    try std.testing.expectEqual(@as(usize, 75), config.ocr_quality.min_content_chars);
    try std.testing.expectEqualStrings("Preserve tables", config.ocr_prompt);
}

test "OCR page request carries PNG media page metadata and layout prompt" {
    const alloc = std.testing.allocator;
    const unit = Unit{
        .unit_id = @constCast("page:000002"),
        .unit_type = @constCast("page"),
        .text = @constCast(""),
        .method = @constCast("pdf_ocr_pending"),
        .page_number = 2,
    };
    const parts = try ocrPagePartsJsonAlloc(alloc, .{}, "pdf", "application/pdf", unit, "png");
    defer alloc.free(parts);
    try std.testing.expect(std.mem.indexOf(u8, parts, "image/png") != null);
    try std.testing.expect(std.mem.indexOf(u8, parts, "Render tables as Markdown") != null);
    try std.testing.expect(std.mem.indexOf(u8, parts, "page:000002") != null);
    try std.testing.expect(std.mem.indexOf(u8, parts, "cG5n") != null);
}
