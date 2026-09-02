// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Shared contracts for bounded multimodal work. These types describe the
//! scheduler boundary; task-specific request and result types remain in their
//! owning model-family packages.

const std = @import("std");

pub const Task = enum {
    read,
    generate,
    embed,
    rerank,
    chunk,
    extract,
    rewrite,
    classify,
    transcribe,
};

pub const InputGranularity = enum {
    document,
    page,
    chunk,
    /// One task-specific logical request, such as a reranking query plus its
    /// candidates or one audio transcription request.
    item,
};

pub const BatchMode = enum {
    none,
    serial_compatibility,
    native,
};

pub const OutputKind = enum {
    read_result,
    generated_text,
    embedding,
    ranked_items,
    chunks,
    extraction,
    rewritten_text,
    classification,
    transcription,
};

pub const ResultCardinality = enum {
    one_per_item,
    one_per_request,
};

pub const PromptPolicy = enum {
    explicit,
    model_default,
    structured_schema,
};

pub const Modalities = packed struct(u8) {
    text: bool = false,
    image: bool = false,
    audio: bool = false,
    document: bool = false,
    _reserved: u4 = 0,

    pub fn contains(self: Modalities, required: Modalities) bool {
        const available_bits: u8 = @bitCast(self);
        const required_bits: u8 = @bitCast(required);
        return available_bits & required_bits == required_bits;
    }
};

/// MIME families are part of model admission. Modalities alone are too broad:
/// an image model that accepts PNG/JPEG must not be handed an arbitrary
/// `image/*` payload (or a raw PDF) merely because both are media.
pub const MimeTypes = packed struct(u16) {
    text_plain: bool = false,
    application_json: bool = false,
    application_pdf: bool = false,
    image_png: bool = false,
    image_jpeg: bool = false,
    image_webp: bool = false,
    audio_wav: bool = false,
    audio_mpeg: bool = false,
    _reserved: u8 = 0,

    pub fn accepts(self: MimeTypes, content_type: []const u8) bool {
        if (std.ascii.eqlIgnoreCase(content_type, "text/plain")) return self.text_plain;
        if (std.ascii.eqlIgnoreCase(content_type, "application/json")) return self.application_json;
        if (std.ascii.eqlIgnoreCase(content_type, "application/pdf")) return self.application_pdf;
        if (std.ascii.eqlIgnoreCase(content_type, "image/png")) return self.image_png;
        if (std.ascii.eqlIgnoreCase(content_type, "image/jpeg") or std.ascii.eqlIgnoreCase(content_type, "image/jpg")) return self.image_jpeg;
        if (std.ascii.eqlIgnoreCase(content_type, "image/webp")) return self.image_webp;
        if (std.ascii.eqlIgnoreCase(content_type, "audio/wav") or std.ascii.eqlIgnoreCase(content_type, "audio/x-wav")) return self.audio_wav;
        if (std.ascii.eqlIgnoreCase(content_type, "audio/mpeg")) return self.audio_mpeg;
        return false;
    }
};

pub const BatchCapabilities = struct {
    mode: BatchMode = .none,
    preferred_items: usize = 1,
    max_items: usize = 1,
    /// Null means the executor did not publish a hard ceiling. Zero is a known
    /// disabled limit: an invocation containing encoded media is not accepted.
    max_encoded_media_bytes: ?usize = null,
    max_decoded_pixels: ?u64 = null,
    max_media_parts_per_item: usize = 0,
    per_item_failures: bool = false,

    pub fn validate(self: BatchCapabilities) !void {
        if (self.preferred_items == 0 or self.max_items == 0) return error.InvalidInferenceCapabilities;
        if (self.preferred_items > self.max_items) return error.InvalidInferenceCapabilities;
        if (self.mode == .none and (self.preferred_items != 1 or self.max_items != 1))
            return error.InvalidInferenceCapabilities;
    }

    pub fn acceptsItems(self: BatchCapabilities, count: usize) bool {
        return count > 0 and count <= self.max_items;
    }

    pub fn executesNatively(self: BatchCapabilities, count: usize) bool {
        return count > 1 and self.mode == .native and self.acceptsItems(count);
    }

    /// Apply one optional model-manifest limit. These string capabilities are
    /// deliberately transport-neutral: the local resolver and remote model
    /// catalog consume the same model-owned values.
    pub fn applyManifestCapability(self: *BatchCapabilities, capability: []const u8) !void {
        const preferred_prefix = "inference.batch.preferred_items=";
        const max_items_prefix = "inference.batch.max_items=";
        const max_media_bytes_prefix = "inference.batch.max_encoded_media_bytes=";
        const legacy_max_bytes_prefix = "inference.batch.max_encoded_bytes=";
        const max_pixels_prefix = "inference.batch.max_decoded_pixels=";
        const max_parts_prefix = "inference.batch.max_media_parts_per_item=";
        if (std.mem.startsWith(u8, capability, preferred_prefix)) {
            self.preferred_items = clampManifestLimit(self.preferred_items, try parseManifestLimit(usize, capability[preferred_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_items_prefix)) {
            self.max_items = clampManifestLimit(self.max_items, try parseManifestLimit(usize, capability[max_items_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_media_bytes_prefix)) {
            self.max_encoded_media_bytes = clampOptionalManifestLimit(self.max_encoded_media_bytes, try parseOptionalManifestLimit(usize, capability[max_media_bytes_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, legacy_max_bytes_prefix)) {
            self.max_encoded_media_bytes = clampOptionalManifestLimit(self.max_encoded_media_bytes, try parseOptionalManifestLimit(usize, capability[legacy_max_bytes_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_pixels_prefix)) {
            self.max_decoded_pixels = clampOptionalManifestLimit(self.max_decoded_pixels, try parseOptionalManifestLimit(u64, capability[max_pixels_prefix.len..]));
        } else if (std.mem.startsWith(u8, capability, max_parts_prefix)) {
            self.max_media_parts_per_item = clampManifestLimit(self.max_media_parts_per_item, try parseManifestLimit(usize, capability[max_parts_prefix.len..]));
        }
    }
};

fn parseManifestLimit(comptime T: type, raw: []const u8) !T {
    if (raw.len == 0) return error.InvalidInferenceCapabilities;
    const value = std.fmt.parseUnsigned(T, raw, 10) catch return error.InvalidInferenceCapabilities;
    if (value == 0) return error.InvalidInferenceCapabilities;
    return value;
}

fn parseOptionalManifestLimit(comptime T: type, raw: []const u8) !T {
    if (raw.len == 0) return error.InvalidInferenceCapabilities;
    return std.fmt.parseUnsigned(T, raw, 10) catch return error.InvalidInferenceCapabilities;
}

fn clampManifestLimit(current: anytype, requested: @TypeOf(current)) @TypeOf(current) {
    return if (current == 0) requested else @min(current, requested);
}

fn clampOptionalManifestLimit(current: anytype, requested: std.meta.Child(@TypeOf(current))) @TypeOf(current) {
    return if (current) |known| @min(known, requested) else requested;
}

/// Resource and modality facts measured from one concrete executor call.
/// Unknown quantities remain zero and are validated later by the component
/// that materializes them (for example, a remote URL after download).
pub const InvocationShape = struct {
    item_count: usize,
    modalities: Modalities = .{},
    encoded_media_bytes: usize = 0,
    decoded_pixels: u64 = 0,
    max_media_parts_per_item: usize = 0,
};

/// Physical representation selected by the concrete executor boundary.
///
/// This is deliberately separate from `InferenceCapabilities`: a model may be
/// reachable through both a linked, borrowed-byte ABI and a remote HTTP route.
/// Admission must charge the route that will actually execute, not a model or
/// catalog property.
pub const AttachmentTransport = enum {
    borrowed_binary,
    base64_payload,
    data_uri,

    /// Bytes contributed by one attachment to the provider's encoded-media
    /// request ceiling. This is a wire/logical limit, not a process-memory
    /// estimate: a buffered HTTP adapter may temporarily retain both the raw
    /// source and its encoded request body.
    pub fn wireSize(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
    ) !usize {
        if (self == .borrowed_binary) return raw_bytes;
        const rounded = std.math.add(usize, raw_bytes, 2) catch
            return error.InferenceEncodedBytesExceeded;
        const encoded = std.math.mul(usize, rounded / 3, 4) catch
            return error.InferenceEncodedBytesExceeded;
        if (self == .base64_payload) return encoded;
        const prefix = std.math.add(
            usize,
            "data:".len + ";base64,".len,
            mime_type_len,
        ) catch return error.InferenceEncodedBytesExceeded;
        return std.math.add(usize, prefix, encoded) catch
            error.InferenceEncodedBytesExceeded;
    }

    /// Peak media bytes retained while a raw attachment is materialized for
    /// this transport. HTTP adapters are required to build a single request
    /// body directly; request-envelope overhead is separately charged to the
    /// caller's allocator. Borrowed execution adds no transport copy.
    pub fn peakResidentSize(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
    ) !usize {
        if (self == .borrowed_binary) return raw_bytes;
        const wire_bytes = try self.wireSize(raw_bytes, mime_type_len);
        return std.math.add(usize, raw_bytes, wire_bytes) catch
            error.InferenceEncodedBytesExceeded;
    }

    /// Conservative aggregate wire size for `item_count` separately encoded
    /// attachments whose raw bytes sum to `raw_bytes`. Separate base64 padding
    /// can add at most one four-byte quantum per item after the first; data URIs
    /// additionally repeat their prefix for every item.
    pub fn batchWireSizeUpperBound(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
        item_count: usize,
    ) !usize {
        if (item_count == 0) return 0;
        if (self == .borrowed_binary) return raw_bytes;
        const encoded = try AttachmentTransport.base64_payload.wireSize(raw_bytes, 0);
        const padding_slack = std.math.mul(usize, item_count - 1, 4) catch
            return error.InferenceEncodedBytesExceeded;
        const encoded_upper = std.math.add(usize, encoded, padding_slack) catch
            return error.InferenceEncodedBytesExceeded;
        if (self == .base64_payload) return encoded_upper;
        const prefix = std.math.add(
            usize,
            "data:".len + ";base64,".len,
            mime_type_len,
        ) catch return error.InferenceEncodedBytesExceeded;
        const prefixes = std.math.mul(usize, item_count, prefix) catch
            return error.InferenceEncodedBytesExceeded;
        return std.math.add(usize, prefixes, encoded_upper) catch
            error.InferenceEncodedBytesExceeded;
    }

    pub fn batchPeakResidentSize(
        self: AttachmentTransport,
        raw_bytes: usize,
        mime_type_len: usize,
        item_count: usize,
    ) !usize {
        if (self == .borrowed_binary) return raw_bytes;
        const wire_bytes = try self.batchWireSizeUpperBound(raw_bytes, mime_type_len, item_count);
        return std.math.add(usize, raw_bytes, wire_bytes) catch
            error.InferenceEncodedBytesExceeded;
    }

    /// Largest raw attachment aggregate satisfying both the provider's wire
    /// ceiling and the caller's resident-media ceiling. Monotonic binary search
    /// keeps the inverse exact across base64 padding boundaries.
    pub fn maxRawBytesForLimits(
        self: AttachmentTransport,
        mime_type_len: usize,
        item_count: usize,
        wire_limit: usize,
        resident_limit: usize,
    ) !usize {
        var low: usize = 0;
        var high: usize = @min(wire_limit, resident_limit);
        while (low < high) {
            const distance = high - low;
            const candidate = low + distance / 2 + distance % 2;
            const wire_bytes = self.batchWireSizeUpperBound(candidate, mime_type_len, item_count) catch {
                high = candidate - 1;
                continue;
            };
            const resident_bytes = self.batchPeakResidentSize(candidate, mime_type_len, item_count) catch {
                high = candidate - 1;
                continue;
            };
            if (wire_bytes <= wire_limit and resident_bytes <= resident_limit)
                low = candidate
            else
                high = candidate - 1;
        }
        return low;
    }
};

pub const InlineDataUri = struct {
    mime_type: []const u8,
    decoded_size: usize,
    encoding: enum { base64, percent },
};

fn standardBase64Index(byte: u8) ?u8 {
    return switch (byte) {
        'A'...'Z' => byte - 'A',
        'a'...'z' => byte - 'a' + 26,
        '0'...'9' => byte - '0' + 52,
        '+' => 62,
        '/' => 63,
        else => null,
    };
}

/// Validate a complete padded standard-base64 value, including canonical
/// trailing bits, without allocating its decoded representation.
pub fn validateCanonicalStandardBase64(data: []const u8) !usize {
    const decoded_size = std.base64.standard.Decoder.calcSizeForSlice(data) catch
        return error.InvalidDataURI;
    var padding: usize = 0;
    while (padding < data.len and data[data.len - 1 - padding] == '=') : (padding += 1) {}
    if (padding > 2) return error.InvalidDataURI;
    const content_end = data.len - padding;
    for (data[0..content_end]) |byte| _ = standardBase64Index(byte) orelse return error.InvalidDataURI;
    for (data[content_end..]) |byte| if (byte != '=') return error.InvalidDataURI;
    if (padding == 1) {
        if (content_end < 3 or (standardBase64Index(data[content_end - 1]).? & 0x03) != 0)
            return error.InvalidDataURI;
    } else if (padding == 2) {
        if (content_end < 2 or (standardBase64Index(data[content_end - 1]).? & 0x0f) != 0)
            return error.InvalidDataURI;
    }
    return decoded_size;
}

fn percentEncodedDataSize(data: []const u8) !usize {
    var size: usize = 0;
    var index: usize = 0;
    while (index < data.len) {
        if (data[index] == '%') {
            if (index + 2 >= data.len or
                !std.ascii.isHex(data[index + 1]) or
                !std.ascii.isHex(data[index + 2]))
                return error.InvalidDataURI;
            index += 3;
        } else {
            index += 1;
        }
        size = std.math.add(usize, size, 1) catch return error.InvalidDataURI;
    }
    return size;
}

/// Parse an inline data URI without materializing its decoded bytes. Both
/// standard base64 and RFC 2397 percent-encoded payloads are accepted because
/// the inference-node downloader supports both forms. Capability checks use
/// the media-type essence rather than parameters such as `charset`.
pub fn parseInlineDataUri(uri: []const u8) !?InlineDataUri {
    if (!std.ascii.startsWithIgnoreCase(uri, "data:")) return null;
    const comma = std.mem.indexOfScalar(u8, uri, ',') orelse return error.InvalidDataURI;
    const metadata = uri["data:".len..comma];
    const base64 = std.ascii.endsWithIgnoreCase(metadata, ";base64");
    const media_metadata = if (base64) metadata[0 .. metadata.len - ";base64".len] else metadata;
    const parameter = std.mem.indexOfScalar(u8, media_metadata, ';') orelse media_metadata.len;
    const mime_type = std.mem.trim(u8, media_metadata[0..parameter], &std.ascii.whitespace);
    if (mime_type.len == 0) return error.InvalidDataURI;
    const payload = uri[comma + 1 ..];
    return .{
        .mime_type = mime_type,
        .decoded_size = if (base64)
            try validateCanonicalStandardBase64(payload)
        else
            try percentEncodedDataSize(payload),
        .encoding = if (base64) .base64 else .percent,
    };
}

pub const InferenceCapabilities = struct {
    task: Task,
    input_modalities: Modalities,
    accepted_mime_types: MimeTypes = .{},
    input_granularity: InputGranularity,
    batch: BatchCapabilities = .{},
    output: OutputKind,
    result_cardinality: ResultCardinality = .one_per_item,
    prompt_policy: PromptPolicy = .explicit,
    // Compatibility/catalog fact for the local provider ABI. Callers must not
    // use this to select byte accounting: transport is an executor property
    // and is supplied explicitly through AttachmentTransport.
    borrowed_attachments: bool = false,

    pub fn validate(self: InferenceCapabilities) !void {
        try self.batch.validate();
        if (@as(u8, @bitCast(self.input_modalities)) == 0) return error.InvalidInferenceCapabilities;
        const expected_output: OutputKind = switch (self.task) {
            .read => .read_result,
            .generate => .generated_text,
            .embed => .embedding,
            .rerank => .ranked_items,
            .chunk => .chunks,
            .extract => .extraction,
            .rewrite => .rewritten_text,
            .classify => .classification,
            .transcribe => .transcription,
        };
        if (self.output != expected_output) return error.InvalidInferenceCapabilities;
        const expected_cardinality: ResultCardinality = switch (self.task) {
            .rerank, .chunk, .transcribe => .one_per_request,
            else => .one_per_item,
        };
        if (self.result_cardinality != expected_cardinality) return error.InvalidInferenceCapabilities;
        if (self.input_granularity == .document and !self.input_modalities.document)
            return error.InvalidInferenceCapabilities;
    }

    pub fn supports(self: InferenceCapabilities, modalities: Modalities) bool {
        return self.input_modalities.contains(modalities);
    }

    pub fn acceptsMimeType(self: InferenceCapabilities, content_type: []const u8) bool {
        return self.accepted_mime_types.accepts(content_type);
    }

    /// Enforce resolved capabilities at the executor boundary. Planners use
    /// the same limits to form efficient windows, but this check is the hard
    /// contract that protects every caller, including direct ABI users.
    pub fn validateInvocation(self: InferenceCapabilities, task: Task, shape: InvocationShape) !void {
        try self.validate();
        if (self.task != task) return error.InferenceTaskMismatch;
        if (shape.item_count == 0) return;
        if (!self.batch.acceptsItems(shape.item_count)) return error.InferenceBatchTooLarge;
        if (!self.supports(shape.modalities)) return error.UnsupportedInferenceModality;
        if (self.batch.max_encoded_media_bytes) |limit| {
            if (shape.encoded_media_bytes > limit) return error.InferenceEncodedBytesExceeded;
        }
        if (self.batch.max_decoded_pixels) |limit| {
            if (shape.decoded_pixels > limit) return error.InferenceDecodedPixelsExceeded;
        }
        if (shape.max_media_parts_per_item > 0 and
            (self.batch.max_media_parts_per_item == 0 or
                shape.max_media_parts_per_item > self.batch.max_media_parts_per_item))
        {
            return error.InferenceMediaPartLimitExceeded;
        }
    }

    pub fn validateMimeType(self: InferenceCapabilities, content_type: []const u8) !void {
        if (!self.acceptsMimeType(content_type)) return error.UnsupportedInferenceMimeType;
    }
};

/// Stable identity follows an item through rendering, inference, fallback, and
/// persistence. It is never inferred from completion order.
pub const WorkIdentity = struct {
    item_id: []const u8 = "",
    source_fingerprint: ?[]const u8 = null,
    page_number: ?u32 = null,

    pub fn eql(a: WorkIdentity, b: WorkIdentity) bool {
        return std.mem.eql(u8, a.item_id, b.item_id) and
            optionalStringsEqual(a.source_fingerprint, b.source_fingerprint) and
            a.page_number == b.page_number;
    }
};

/// Transport-neutral failure information for one work item. `cause` is the
/// local error identity used by existing control flow; the remaining fields
/// preserve the remote executor's authoritative retry decision without
/// requiring borrowed JSON strings to outlive the invocation.
pub const ItemFailure = struct {
    pub const Code = enum {
        unknown,
        invalid_request,
        content_too_large,
        content_not_allowed,
        model_not_found,
        model_resource_busy,
        service_unavailable,
        generation_failed,
        upstream_failure,

        pub fn fromWire(value: []const u8) Code {
            if (std.mem.eql(u8, value, "INVALID_REQUEST")) return .invalid_request;
            if (std.mem.eql(u8, value, "CONTENT_TOO_LARGE")) return .content_too_large;
            if (std.mem.eql(u8, value, "CONTENT_NOT_ALLOWED")) return .content_not_allowed;
            if (std.mem.eql(u8, value, "MODEL_NOT_FOUND")) return .model_not_found;
            if (std.mem.eql(u8, value, "MODEL_RESOURCE_BUSY")) return .model_resource_busy;
            if (std.mem.eql(u8, value, "SERVICE_UNAVAILABLE")) return .service_unavailable;
            if (std.mem.eql(u8, value, "GENERATION_FAILED") or
                std.mem.eql(u8, value, "STRUCTURED_OUTPUT_INVALID")) return .generation_failed;
            return .upstream_failure;
        }
    };

    cause: anyerror,
    code: Code = .unknown,
    retryable: bool = false,
    retry_after_ms: ?u64 = null,
};

/// Trusted media borrowed for the duration of one synchronous executor call.
pub const Attachment = struct {
    bytes: []const u8,
    content_type: []const u8,
    identity: WorkIdentity = .{},

    pub fn validate(self: Attachment) !void {
        if (self.bytes.len == 0) return error.EmptyInferenceAttachment;
        if (!validContentType(self.content_type)) return error.InvalidInferenceAttachmentContentType;
    }
};

pub const ExecutionReport = struct {
    requested_items: usize = 0,
    native_batches: usize = 0,
    native_items: usize = 0,
    serial_items: usize = 0,
    rejected_items: usize = 0,
    fallback_items: usize = 0,
    fallback_reason: ?[]const u8 = null,

    pub fn validate(self: ExecutionReport) !void {
        const executed = std.math.add(usize, self.native_items, self.serial_items) catch
            return error.InvalidExecutionReport;
        const accounted = std.math.add(usize, executed, self.rejected_items) catch
            return error.InvalidExecutionReport;
        if (accounted != self.requested_items) return error.InvalidExecutionReport;
        if (self.fallback_items > self.serial_items) return error.InvalidExecutionReport;
        if (self.native_batches == 0 and self.native_items != 0) return error.InvalidExecutionReport;
        if (self.native_batches > self.native_items) return error.InvalidExecutionReport;
        if (self.fallback_items == 0 and self.fallback_reason != null) return error.InvalidExecutionReport;
    }

    pub fn native(count: usize) ExecutionReport {
        if (count <= 1) return serial(count);
        return .{
            .requested_items = count,
            .native_batches = 1,
            .native_items = count,
        };
    }

    pub fn serial(count: usize) ExecutionReport {
        return .{ .requested_items = count, .serial_items = count };
    }

    pub fn fallback(count: usize, reason: []const u8) ExecutionReport {
        return .{
            .requested_items = count,
            .serial_items = count,
            .fallback_items = count,
            .fallback_reason = reason,
        };
    }

    pub fn compatibility(count: usize) ExecutionReport {
        return serial(count);
    }
};

/// Task executors keep their result payload typed while sharing one identity
/// envelope. A failed item remains attributable without shifting later items.
pub fn WorkItemResult(comptime T: type) type {
    return struct {
        identity: WorkIdentity,
        result: union(enum) {
            value: T,
            item_error: ItemFailure,
        },
    };
}

fn validContentType(value: []const u8) bool {
    if (value.len < 3 or std.mem.indexOfScalar(u8, value, '/') == null) return false;
    for (value) |byte| {
        if (std.ascii.isWhitespace(byte) or byte < 0x21 or byte == 0x7f) return false;
    }
    return true;
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

test "inference capabilities distinguish native and compatibility batches" {
    const native = InferenceCapabilities{
        .task = .embed,
        .input_modalities = .{ .text = true, .image = true },
        .input_granularity = .page,
        .batch = .{ .mode = .native, .preferred_items = 8, .max_items = 64 },
        .output = .embedding,
        .borrowed_attachments = true,
    };
    try native.validate();
    try std.testing.expect(native.batch.executesNatively(8));

    const compatibility = InferenceCapabilities{
        .task = .generate,
        .input_modalities = .{ .text = true, .image = true },
        .input_granularity = .page,
        .batch = .{ .mode = .serial_compatibility, .preferred_items = 8, .max_items = 64 },
        .output = .generated_text,
    };
    try compatibility.validate();
    try std.testing.expect(!compatibility.batch.executesNatively(8));
}

test "attachment transport separates wire and peak resident representations" {
    try std.testing.expectEqual(
        @as(usize, 3),
        try AttachmentTransport.borrowed_binary.wireSize(3, "image/png".len),
    );
    try std.testing.expectEqual(
        @as(usize, 4),
        try AttachmentTransport.base64_payload.wireSize(3, "image/png".len),
    );
    try std.testing.expectEqual(
        "data:image/png;base64,AQID".len,
        try AttachmentTransport.data_uri.wireSize(3, "image/png".len),
    );
    try std.testing.expectEqual(@as(usize, 7), try AttachmentTransport.base64_payload.peakResidentSize(3, 0));
    try std.testing.expectEqual(@as(usize, 3), try AttachmentTransport.base64_payload.maxRawBytesForLimits(0, 1, 4, 7));
    try std.testing.expectEqual(@as(usize, 0), try AttachmentTransport.base64_payload.maxRawBytesForLimits(0, 1, 3, 7));
    try std.testing.expectEqual(@as(usize, 12), try AttachmentTransport.base64_payload.batchWireSizeUpperBound(6, 0, 2));
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        AttachmentTransport.base64_payload.wireSize(std.math.maxInt(usize), 0),
    );
}

test "inline data URI parser validates canonical metadata" {
    const parsed = (try parseInlineDataUri("data:image/png;base64,AQID")).?;
    try std.testing.expectEqualStrings("image/png", parsed.mime_type);
    try std.testing.expectEqual(@as(usize, 3), parsed.decoded_size);
    try std.testing.expectEqual(.base64, parsed.encoding);
    const parameterized = (try parseInlineDataUri("data:image/png;charset=binary;base64,AQID")).?;
    try std.testing.expectEqualStrings("image/png", parameterized.mime_type);
    const percent = (try parseInlineDataUri("data:image/png,%89PNG")).?;
    try std.testing.expectEqual(@as(usize, 4), percent.decoded_size);
    try std.testing.expectEqual(.percent, percent.encoding);
    try std.testing.expect((try parseInlineDataUri("https://example.invalid/image.png")) == null);
    try std.testing.expectError(error.InvalidDataURI, parseInlineDataUri("data:;base64,AQID"));
    try std.testing.expectError(error.InvalidDataURI, parseInlineDataUri("data:image/png;base64,YR=="));
    try std.testing.expectError(error.InvalidDataURI, parseInlineDataUri("data:image/png,%8"));
}

test "inference capabilities keep every model family output typed" {
    const cases = [_]struct { Task, OutputKind }{
        .{ .read, .read_result },
        .{ .generate, .generated_text },
        .{ .embed, .embedding },
        .{ .rerank, .ranked_items },
        .{ .chunk, .chunks },
        .{ .extract, .extraction },
        .{ .rewrite, .rewritten_text },
        .{ .classify, .classification },
        .{ .transcribe, .transcription },
    };
    for (cases) |case| {
        const capabilities = InferenceCapabilities{
            .task = case[0],
            .input_modalities = .{ .text = true },
            .input_granularity = .item,
            .output = case[1],
            .result_cardinality = switch (case[0]) {
                .rerank, .chunk, .transcribe => .one_per_request,
                else => .one_per_item,
            },
        };
        try capabilities.validate();
    }
}

test "inference capabilities enforce invocation resource limits" {
    const capabilities = InferenceCapabilities{
        .task = .embed,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true },
        .input_granularity = .page,
        .batch = .{
            .mode = .native,
            .preferred_items = 2,
            .max_items = 4,
            .max_encoded_media_bytes = 1024,
            .max_decoded_pixels = 4096,
            .max_media_parts_per_item = 1,
        },
        .output = .embedding,
    };
    try capabilities.validateInvocation(.embed, .{
        .item_count = 2,
        .modalities = .{ .image = true },
        .encoded_media_bytes = 512,
        .decoded_pixels = 2048,
        .max_media_parts_per_item = 1,
    });
    try capabilities.validateMimeType("image/png");
    try std.testing.expectError(
        error.InferenceBatchTooLarge,
        capabilities.validateInvocation(.embed, .{ .item_count = 5, .modalities = .{ .image = true } }),
    );
    try std.testing.expectError(
        error.InferenceEncodedBytesExceeded,
        capabilities.validateInvocation(.embed, .{ .item_count = 1, .modalities = .{ .image = true }, .encoded_media_bytes = 1025 }),
    );
    try std.testing.expectError(error.UnsupportedInferenceMimeType, capabilities.validateMimeType("image/webp"));
}

test "batch capabilities accept model-owned limit overrides" {
    var batch = BatchCapabilities{
        .mode = .native,
        .preferred_items = 8,
        .max_items = 64,
        .max_encoded_media_bytes = 64 * 1024 * 1024,
        .max_decoded_pixels = 50_000_000,
        .max_media_parts_per_item = 1,
    };
    try batch.applyManifestCapability("inference.batch.max_items=12");
    try batch.applyManifestCapability("inference.batch.max_encoded_media_bytes=4096");
    try batch.applyManifestCapability("inference.batch.max_decoded_pixels=12345");
    try batch.applyManifestCapability("inference.batch.max_media_parts_per_item=3");
    try std.testing.expectEqual(@as(usize, 12), batch.max_items);
    try std.testing.expectEqual(@as(?usize, 4096), batch.max_encoded_media_bytes);

    try batch.applyManifestCapability("inference.batch.max_encoded_media_bytes=0");
    try std.testing.expectEqual(@as(?usize, 0), batch.max_encoded_media_bytes);
    try std.testing.expectEqual(@as(?u64, 12345), batch.max_decoded_pixels);
    try std.testing.expectEqual(@as(usize, 1), batch.max_media_parts_per_item);
    try batch.applyManifestCapability("inference.batch.max_items=128");
    try std.testing.expectEqual(@as(usize, 12), batch.max_items);
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        batch.applyManifestCapability("inference.batch.max_items=invalid"),
    );
    try std.testing.expectError(
        error.InvalidInferenceCapabilities,
        batch.applyManifestCapability("inference.batch.max_items=0"),
    );
}

test "work identity and execution reports preserve per-item semantics" {
    const identity = WorkIdentity{ .item_id = "page:7", .source_fingerprint = "doc-a", .page_number = 7 };
    try std.testing.expect(identity.eql(.{ .item_id = "page:7", .source_fingerprint = "doc-a", .page_number = 7 }));
    try std.testing.expect(!identity.eql(.{ .item_id = "page:8", .source_fingerprint = "doc-a", .page_number = 8 }));

    const attachment = Attachment{ .bytes = "png", .content_type = "image/png", .identity = identity };
    try attachment.validate();
    try std.testing.expectError(
        error.InvalidInferenceAttachmentContentType,
        (Attachment{ .bytes = "png", .content_type = "image png" }).validate(),
    );

    const report = ExecutionReport.native(8);
    try report.validate();
    const singleton = ExecutionReport.native(1);
    try singleton.validate();
    try std.testing.expectEqual(@as(usize, 0), singleton.native_batches);
    try std.testing.expectEqual(@as(usize, 1), singleton.serial_items);
    try std.testing.expectError(
        error.InvalidExecutionReport,
        (ExecutionReport{ .requested_items = 2, .serial_items = 1 }).validate(),
    );
    try std.testing.expectError(
        error.InvalidExecutionReport,
        (ExecutionReport{ .requested_items = 1, .native_batches = 2, .native_items = 1 }).validate(),
    );
}
